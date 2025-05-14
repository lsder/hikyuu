/*
 *  Copyright(C) 2021 hikyuu.org
 *
 *  Create on: 2021-02-16
 *     Author: fasiondog
 */

#include <csignal>
#include <unordered_set>
#include "hikyuu/utilities/os.h"
#include "hikyuu/utilities/ini_parser/IniParser.h"
#include "hikyuu/utilities/node/NodeClient.h"
#include "hikyuu/global/GlobalSpotAgent.h"
#include "hikyuu/global/schedule/scheduler.h"
#include "hikyuu/global/sysinfo.h"
#include "hikyuu/hikyuu.h"
#include "Strategy.h"

namespace hku {

std::atomic_bool Strategy::ms_keep_running = true;

void Strategy::sig_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        ms_keep_running = false;
        auto* scheduler = getScheduler();
        scheduler->stop();
        exit(0);
    }
}

Strategy::Strategy() : Strategy("Strategy", "") {}

Strategy::Strategy(const string& name, const string& config_file)
: m_name(name), m_config_file(config_file) {
    _initParam();
    if (m_config_file.empty()) {
        string home = getUserDir();
        HKU_ERROR_IF(home == "", "Failed get user home path!");
#if HKU_OS_WINOWS
        m_config_file = format("{}\\{}", home, ".hikyuu\\hikyuu.ini");
#else
        m_config_file = format("{}/{}", home, ".hikyuu/hikyuu.ini");
#endif
    }
}

Strategy::Strategy(const vector<string>& codeList, const vector<KQuery::KType>& ktypeList,
                   const unordered_map<string, int>& preloadNum, const string& name,
                   const string& config_file)
: Strategy(name, config_file) {
    _initParam();
    m_context.setStockCodeList(codeList);
    m_context.setKTypeList(ktypeList);
    m_context.setPreloadNum(preloadNum);
}

Strategy::Strategy(const StrategyContext& context, const string& name, const string& config_file)
: Strategy(name, config_file) {
    _initParam();
    m_context = context;
}

Strategy::~Strategy() {
    // ms_keep_running 用于全局 ctrl-c 终止，不能在释放时释放，否则新创建的策略对象将运行
    // ms_keep_running = false;
    event([]() {});
}

void Strategy::_initParam() {
    setParam<int>("spot_worker_num", 1);
    setParam<string>("quotation_server", string());
}

void Strategy::baseCheckParam(const string& name) const {
    if (name == "spot_worker_num") {
        HKU_ASSERT(getParam<int>(name) > 0);
    }
}

void Strategy::paramChanged() {}

bool Strategy::running() const {
    return ms_keep_running;
}

void Strategy::_init() {
    StockManager& sm = StockManager::instance();

    // sm 尚未初始化，则初始化
    if (sm.thread_id() == std::thread::id()) {
        // 注册 ctrl-c 终止信号
        if (!runningInPython()) {
            std::signal(SIGINT, sig_handler);
        }

        CLS_INFO("{} is running! You can press Ctrl-C to terminte ...", m_name);

        // 初始化
        hikyuu_init(m_config_file, false, m_context);

    } else {
        m_context = sm.getStrategyContext();
    }

    CLS_CHECK(!m_context.getStockCodeList().empty(), "The context does not contain any stocks!");

    // 先将行情接收代理停止，以便后面加入处理函数
    stopSpotAgent();
}

void Strategy::start(bool autoRecieveSpot) {
    HKU_WARN_IF_RETURN(pythonInInteractive(), void(),
                       "Can not start strategy in python interactive mode!");
    HKU_WARN_IF(!m_on_recieved_spot && !m_on_change && m_run_daily_at_list.empty() &&
                  m_run_daily_at_funcs.empty(),
                "No any process function is set!");

    _init();

    _runDailyAt();

    if (autoRecieveSpot) {
        auto& agent = *getGlobalSpotAgent();
        agent.addProcess([this](const SpotRecord& spot) { _receivedSpot(spot); });
        agent.addPostProcess([this](Datetime revTime) {
            if (m_on_recieved_spot) {
                event([this, revTime]() { m_on_recieved_spot(this, revTime); });
            }
        });
        startSpotAgent(true, getParam<int>("spot_worker_num"),
                       getParam<string>("quotation_server"));
    }

    _runDaily();

    CLS_INFO("{} start even loop ...", name());
    _startEventLoop();
}

void Strategy::onChange(
  const std::function<void(Strategy*, const Stock&, const SpotRecord& spot)>& changeFunc) {
    HKU_CHECK(changeFunc, "Invalid changeFunc!");
    m_on_change = std::move(changeFunc);
}

void Strategy::onReceivedSpot(const std::function<void(Strategy*, const Datetime&)>& recievedFucn) {
    HKU_CHECK(recievedFucn, "Invalid recievedFucn!");
    m_on_recieved_spot = std::move(recievedFucn);
}

void Strategy::_receivedSpot(const SpotRecord& spot) {
    Stock stk = getStock(format("{}{}", spot.market, spot.code));
    if (!stk.isNull()) {
        if (m_on_change) {
            event([this, stk, spot]() { m_on_change(this, stk, spot); });
        }
    }
}

void Strategy::runDaily(const std::function<void(Strategy*)>& func, const TimeDelta& delta,
                        const std::string& market, bool ignoreMarket) {
    HKU_CHECK(func, "Invalid func!");
    HKU_CHECK(!market.empty(), "The market can not be empty!");
    HKU_WARN_IF(delta > Hours(1), "The delta may be large! {}", delta);

    RunDailyAt run_at;
    run_at.delta = delta;
    run_at.market = market;
    run_at.ignoreMarket = ignoreMarket;

    if (ignoreMarket) {
        run_at.func = [this, f = std::move(func)]() { event([this, f]() { f(this); }); };

    } else {
        run_at.func = [this, market = run_at.market, f = std::move(func)]() {
            const auto& sm = StockManager::instance();
            auto today = Datetime::today();
            int day = today.dayOfWeek();
            if (day == 0 || day == 6 || sm.isHoliday(today)) {
                return;
            }

            auto market_info = sm.getMarketInfo(market);
            Datetime open1 = today + market_info.openTime1();
            Datetime close1 = today + market_info.closeTime1();
            Datetime open2 = today + market_info.openTime2();
            Datetime close2 = today + market_info.closeTime2();
            Datetime now = Datetime::now();
            if ((now >= open1 && now <= close1) || (now >= open2 && now <= close2)) {
                event([this, f]() { f(this); });
            }
        };
    }

    m_run_daily_at_list.push_front(run_at);
}

void Strategy::_runDaily() {
    HKU_IF_RETURN(m_run_daily_at_list.empty(), void());

    auto* scheduler = getScheduler();

    for (auto& run_at : m_run_daily_at_list) {
        if (run_at.ignoreMarket) {
            scheduler->addDurationFunc(std::numeric_limits<int>::max(), run_at.delta, run_at.func);

        } else {
            try {
                const auto& sm = StockManager::instance();
                auto market_info = sm.getMarketInfo(run_at.market);
                HKU_ERROR_IF_RETURN(market_info == Null<MarketInfo>(), void(),
                                    "market {} not found! The run daily func is discard!",
                                    run_at.market);

                auto today = Datetime::today();
                auto now = Datetime::now();
                TimeDelta now_time = now - today;
                if (now_time >= market_info.closeTime2()) {
                    scheduler->addFuncAtTime(
                      today.nextDay() + market_info.openTime1(), [&run_at]() {
                          run_at.func();
                          auto* sched = getScheduler();
                          sched->addDurationFunc(std::numeric_limits<int>::max(), run_at.delta,
                                                 run_at.func);
                      });

                } else if (now_time >= market_info.openTime2()) {
                    int64_t ticks = now_time.ticks() - market_info.openTime2().ticks();
                    int64_t delta_ticks = run_at.delta.ticks();
                    if (ticks % delta_ticks == 0) {
                        scheduler->addDurationFunc(std::numeric_limits<int>::max(), run_at.delta,
                                                   run_at.func);
                    } else {
                        auto delay =
                          TimeDelta::fromTicks((ticks / delta_ticks + 1) * delta_ticks - ticks);
                        scheduler->addFuncAtTime(now + delay, [&run_at]() {
                            run_at.func();
                            auto* sched = getScheduler();
                            sched->addDurationFunc(std::numeric_limits<int>::max(), run_at.delta,
                                                   run_at.func);
                        });
                    }

                } else if (now_time >= market_info.closeTime1()) {
                    scheduler->addFuncAtTime(today + market_info.openTime2(), [&run_at]() {
                        run_at.func();
                        auto* sched = getScheduler();
                        sched->addDurationFunc(std::numeric_limits<int>::max(), run_at.delta,
                                               run_at.func);
                    });

                } else if (now_time < market_info.closeTime1() &&
                           now_time >= market_info.openTime1()) {
                    int64_t ticks = now_time.ticks() - market_info.openTime1().ticks();
                    int64_t delta_ticks = run_at.delta.ticks();
                    if (ticks % delta_ticks == 0) {
                        scheduler->addDurationFunc(std::numeric_limits<int>::max(), run_at.delta,
                                                   run_at.func);
                    } else {
                        auto delay =
                          TimeDelta::fromTicks((ticks / delta_ticks + 1) * delta_ticks - ticks);
                        scheduler->addFuncAtTime(now + delay, [&run_at]() {
                            run_at.func();
                            auto* sched = getScheduler();
                            sched->addDurationFunc(std::numeric_limits<int>::max(), run_at.delta,
                                                   run_at.func);
                        });
                    }

                } else if (now_time < market_info.openTime1()) {
                    scheduler->addFuncAtTime(today + market_info.openTime1(), [&run_at]() {
                        run_at.func();
                        auto* sched = getScheduler();
                        sched->addDurationFunc(std::numeric_limits<int>::max(), run_at.delta,
                                               run_at.func);
                    });

                } else {
                    CLS_ERROR("Unknown process! now_time: {}", now_time);
                }
            } catch (const std::exception& e) {
                CLS_THROW("{}", e.what());
            }
        }
    }
}

void Strategy::runDailyAt(const std::function<void(Strategy*)>& func, const TimeDelta& delta,
                          bool ignoreHoliday) {
    HKU_CHECK(func, "Invalid func!");
    HKU_CHECK(delta < Days(1), "TimeDelta must < Days(1)!");
    HKU_CHECK(m_run_daily_at_funcs.find(delta) == m_run_daily_at_funcs.end(),
              "A task already exists at this point in time!");

    std::function<void()> new_func;
    if (ignoreHoliday) {
        new_func = [this, f = std::move(func)]() {
            const auto& sm = StockManager::instance();
            auto today = Datetime::today();
            int day = today.dayOfWeek();
            if (day != 0 && day != 6 && !sm.isHoliday(today)) {
                event([this, f]() { f(this); });
            }
        };

    } else {
        new_func = [this, f = std::move(func)]() { event([this, f]() { f(this); }); };
    }

    m_run_daily_at_funcs[delta] = new_func;
}

void Strategy::_runDailyAt() {
    auto* scheduler = getScheduler();
    for (const auto& [time, func] : m_run_daily_at_funcs) {
        scheduler->addFuncAtTimeEveryDay(time, func);
    }
    m_run_daily_at_funcs.clear();
}

/*
 * 在主线程中处理事件队列，避免 python GIL
 */
void Strategy::_startEventLoop() {
    while (ms_keep_running) {
        event_type task;
        m_event_queue.wait_and_pop(task);
        if (task.isNullTask()) {
            ms_keep_running = false;
        } else {
            try {
                task();
            } catch (const std::exception& e) {
                CLS_ERROR("Failed run task! {}", e.what());
            } catch (...) {
                CLS_ERROR("Failed run task! Unknow error!");
            }
        }
    }
}

KData Strategy::getKData(const Stock& stk, const Datetime& start_date, const Datetime& end_date,
                         const KQuery::KType& ktype, KQuery::RecoverType recover_type) const {
    Datetime new_end_date = end_date;
    if (end_date.isNull() || end_date > now()) {
        new_end_date = nextDatetime();
    }
    return stk.getKData(KQueryByDate(start_date, new_end_date, ktype, recover_type));
}

KData Strategy::getLastKData(const Stock& stk, size_t lastnum, const KQuery::KType& ktype,
                             KQuery::RecoverType recover_type) const {
    KData ret;
    KQuery query = KQueryByDate(Datetime::min(), nextDatetime(), ktype);
    size_t out_start = 0, out_end = 0;
    HKU_IF_RETURN(!stk.getIndexRange(query, out_start, out_end), ret);

    int64_t startidx = 0, endidx = 0;
    endidx = out_end;
    int64_t num = static_cast<int64_t>(lastnum);
    startidx = (endidx > num) ? endidx - num : out_start;

    query = KQueryByIndex(startidx, endidx, ktype, recover_type);
    ret = stk.getKData(query);
    return ret;
}

void HKU_API runInStrategy(const SYSPtr& sys, const Stock& stk, const KQuery& query,
                           const OrderBrokerPtr& broker, const TradeCostPtr& costfunc,
                           const std::vector<OrderBrokerPtr>& other_brokers) {
    HKU_ASSERT(sys && broker && sys->getTM());
    HKU_ASSERT(!stk.isNull());
    HKU_ASSERT(query != Null<KQuery>());
    HKU_CHECK(!sys->getParam<bool>("buy_delay") && !sys->getParam<bool>("sell_delay"),
              "Thie method only support buy|sell on close!");

    auto tm = crtBrokerTM(broker, costfunc, sys->name(), other_brokers);
    tm->fetchAssetInfoFromBroker(broker);
    sys->setTM(tm);
    sys->setSP(SlippagePtr());  // 清除移滑价差算法
    sys->run(stk, query);
}

void HKU_API runInStrategy(const PFPtr& pf, const KQuery& query, const OrderBrokerPtr& broker,
                           const TradeCostPtr& costfunc,
                           const std::vector<OrderBrokerPtr>& other_brokers) {
    HKU_ASSERT(pf && broker && pf->getTM());
    HKU_ASSERT(query != Null<KQuery>());

    auto se = pf->getSE();
    HKU_ASSERT(se);
    const auto& sys_list = se->getProtoSystemList();
    for (const auto& sys : sys_list) {
        HKU_CHECK(!sys->getSP(), "Exist Slippage part in sys, You must clear it! {}", sys->name());
        HKU_CHECK(!sys->getParam<bool>("buy_delay") && !sys->getParam<bool>("sell_delay"),
                  "Thie method only support buy|sell on close!");
    }

    auto tm = crtBrokerTM(broker, costfunc, pf->name(), other_brokers);
    tm->fetchAssetInfoFromBroker(broker);
    pf->setTM(tm);
    pf->run(query, true);
}

void HKU_API getDataFromBufferServer(const std::string& addr, const StockList& stklist,
                                     const KQuery::KType& ktype) {
    // SPEND_TIME(getDataFromBufferServer);
    const auto& preload = StockManager::instance().getPreloadParameter();
    string low_ktype = ktype;
    to_lower(low_ktype);
    HKU_ERROR_IF_RETURN(!preload.tryGet<bool>(low_ktype, false), void(),
                        "The {} kdata is not preload! Can't update!", low_ktype);

    NodeClient client(addr);
    try {
        HKU_CHECK(client.dial(), "Failed dial server!");
        json req;
        req["cmd"] = "market";
        req["ktype"] = ktype;
        json code_list;
        json date_list;
        for (const auto& stk : stklist) {
            if (!stk.isNull()) {
                code_list.emplace_back(stk.market_code());
                auto k = stk.getKData(KQueryByIndex(-1, Null<int64_t>(), ktype));
                if (k.empty()) {
                    date_list.emplace_back(Datetime::min().str());
                } else {
                    date_list.emplace_back(k[k.size() - 1].datetime.str());
                }
            }
        }
        req["codes"] = std::move(code_list);
        req["dates"] = std::move(date_list);

        json res;
        client.post(req, res);
        HKU_ERROR_IF_RETURN(res["ret"] != NodeErrorCode::SUCCESS, void(),
                            "Recieved error: {}, msg: {}", res["ret"].get<int>(),
                            res["msg"].get<string>());

        const auto& jdata = res["data"];
        // HKU_INFO("{}", to_string(jdata));
        for (auto iter = jdata.cbegin(); iter != jdata.cend(); ++iter) {
            const auto& r = *iter;
            try {
                string market_code = r["code"].get<string>();
                Stock stk = getStock(market_code);
                if (stk.isNull()) {
                    continue;
                }

                const auto& jklist = r["data"];
                for (auto kiter = jklist.cbegin(); kiter != jklist.cend(); ++kiter) {
                    const auto& k = *kiter;
                    KRecord kr(Datetime(k[0].get<string>()), k[1], k[2], k[3], k[4], k[5], k[6]);
                    stk.realtimeUpdate(kr, ktype);
                }

            } catch (const std::exception& e) {
                HKU_ERROR("Failed decode json: {}! {}", to_string(r), e.what());
            }
        }

    } catch (const std::exception& e) {
        HKU_ERROR("Failed get data from buffer server! {}", e.what());
    } catch (...) {
        HKU_ERROR_UNKNOWN;
    }
}

}  // namespace hku