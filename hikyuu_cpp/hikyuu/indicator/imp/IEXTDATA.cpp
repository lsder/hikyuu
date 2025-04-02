/*
 * IEXTDATA.cpp
 *
 *  Created on: 2013-2-11
 *      Author: fasiondog
 */

#include "IEXTDATA.h"
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <H5Cpp.h>
using namespace H5;
#if HKU_SUPPORT_SERIALIZATION
BOOST_CLASS_EXPORT(hku::IEXTDATA)
#endif

namespace hku {

IEXTDATA::IEXTDATA() : IndicatorImp("EXTDATA", 1) {
    setParam<int>("ndigits", 1);
    setParam<string>("filepath", "");
}

// 自定义数据结构

void IEXTDATA::_calculate(const Indicator& ind) {
    KData kdata = ind.getContext();
    size_t total = kdata.size();
    if (total == 0) {
        kdata = getContext();
        total = kdata.size();
        if (total == 0) {
            return;
        }
    }
    _readyBuffer(total, 1);
    Stock stock = kdata.getStock();

    m_discard = ind.discard();
    if (m_discard >= total) {
        m_discard = total;
        return;
    }

    int n = getParam<int>("ndigits");

    string FILE_NAME = getParam<string>("filepath");
    if (FILE_NAME.empty()) {
        std::cerr << "获取文件路径参数失败" << std::endl;
        return;
    }
    string tablename = stock.market_code();
    string GROUP_NAME = "data";

    try {
        H5::H5File h5file(FILE_NAME, H5F_ACC_RDONLY);
        H5::Group group = h5file.openGroup(GROUP_NAME);

        if (!H5Lexists(group.getId(), tablename.c_str(), H5P_DEFAULT)) {
            std::cerr << "数据集 " << tablename << " 不存在于组 " << GROUP_NAME << " 中。" << std::endl;
            return;
        }

        H5::DataSet dataset = group.openDataSet(tablename);
        H5::DataSpace dataspace = dataset.getSpace();

        size_t all_total = dataspace.getSelectNpoints();
        H5::DataType datatype = dataset.getDataType();

        // 获取每个数据点的大小（以字节为单位）
        size_t point_size = datatype.getSize();
        const int NUM_UINT32 = static_cast<int>((point_size - 8) / 4);

        if (n > NUM_UINT32+1) {
            std::cerr << "索引 n 超出范围" << std::endl;
            return;
        }

        // 使用 std::vector 动态存储 uint32_t 数据
        struct Record {
            uint32_t uint32_values1;
            uint32_t uint32_values2;
            uint32_t uint32_values3;
            uint32_t uint32_values4;
            uint32_t uint32_values5;
            uint32_t uint32_values6;
            uint64_t uint64_value;
        };

        std::unique_ptr<Record[]> pBuf = std::make_unique<Record[]>(all_total);
        // for (hsize_t i = 0; i < all_total; ++i) {
        //     pBuf[i].uint32_values.resize(NUM_UINT32);
        // }

        hsize_t offsets[1] = {0};
        hsize_t count[1] = {all_total};
        H5::DataSpace memspace(1, count);
        dataspace.selectHyperslab(H5S_SELECT_SET, count, offsets);

        dataset.read(pBuf.get(), datatype, memspace, dataspace);

        memspace.close();
        dataspace.close();
        dataset.close();
        group.close();
        h5file.close();
        size_t x_total = all_total;
        size_t x_start = 0;
        auto* dst = this->data();
        if (x_total < total) {
            dst = dst + total + m_discard - x_total;
        } else if (x_total > total) {
            x_start = x_total - total;
            dst = dst - x_start;
        }
        for (size_t i = x_start; i < x_total; ++i) {
            dst[i] = (pBuf[i].uint32_values1);
        }

    } catch (const H5::Exception& e) {
        std::cerr << "HDF5 操作出错: " << e.getDetailMsg() << std::endl;
    }
}


Indicator HKU_API EXTDATA(int n,const string& filepath ) {
    auto p = make_shared<IEXTDATA>();
    p->setParam<int>("ndigits", n);
    p->setParam<string>("filepath",filepath);
    return Indicator(p);
}

} /* namespace hku */
