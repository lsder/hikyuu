/*
 *  Copyright (c) 2025 hikyuu.org
 *
 *  Created on: 2025-05-06
 *      Author: fasiondog
 */

#include "interface/plugins.h"
#include "KDataToHdf5Importer.h"

namespace hku {

KDataToHdf5Importer::KDataToHdf5Importer() {
    auto& sm = StockManager::instance();
    m_plugin = sm.getPlugin<ImportKDataToHdf5PluginInterface>(HKU_PLUGIN_IMPORTKDATATOHDF5);
    if (!m_plugin) {
        HKU_ERROR("Can't find {} plugin!", HKU_PLUGIN_IMPORTKDATATOHDF5);
    }
}

KDataToHdf5Importer::~KDataToHdf5Importer() {}

bool KDataToHdf5Importer::setConfig(const string& datapath, const vector<string>& markets) {
    return m_plugin ? m_plugin->setConfig(datapath, markets) : false;
}

Datetime KDataToHdf5Importer::getLastDatetime(const string& market, const string& code,
                                              const KQuery::KType& ktype) {
    return m_plugin ? m_plugin->getLastDatetime(market, code, ktype) : Null<Datetime>();
}

void KDataToHdf5Importer::addKRecordList(const string& market, const string& code,
                                         const vector<KRecord>& krecords,
                                         const KQuery::KType& ktype) {
    if (m_plugin) {
        m_plugin->addKRecordList(market, code, krecords, ktype);
    }
}

void KDataToHdf5Importer::updateIndex(const string& market, const string& code,
                                      const KQuery::KType& ktype) {
    if (m_plugin) {
        m_plugin->updateIndex(market, code, ktype);
    }
}

}  // namespace hku