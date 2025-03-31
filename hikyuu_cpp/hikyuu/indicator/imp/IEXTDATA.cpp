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
 

void IEXTDATA::_calculate(const Indicator& ind) {
    KData kdata = ind.getContext();
    size_t total = kdata.size();
    if (total == 0) {
        kdata = getContext();
        total = kdata.size();
        if (total == 0) {
            KData kdata = getContext();
            return;
        }
        
    }
    _readyBuffer(total, 1);
    Stock stock =kdata.getStock();
     
    m_discard = ind.discard();
    if (m_discard >= total) {
        m_discard = total;
        return;
    }

    int n = getParam<int>("ndigits"); 
    
    string FILE_NAME= getParam<string>("filepath");
    if (FILE_NAME.empty()) {
        std::cerr << "获取文件路径参数失败" << std::endl;
        return;
    }
    string DATASET_NAME=stock.market_code();
    string GROUP_NAME = "data";
    
    H5::H5File h5file(FILE_NAME, H5F_ACC_RDONLY);//读取hdf5文件中的数据
    H5::Group group = h5file.openGroup(GROUP_NAME);// 打开指定的 group
    H5::DataSet dataset = group.openDataSet(DATASET_NAME);
    H5::DataSpace dataspace = dataset.getSpace();// 获取数据集的数据空间

    // 获取数据集的维度信息
    hsize_t dims[2];
    int ndims = dataspace.getSimpleExtentDims(dims, NULL);
    if (ndims != 2 || dims[1] < 1 || static_cast<size_t>(n) > dims[1]) {
        std::cerr << "数据集不是二维、列数不足或者 n 超出范围" << dims[1]<<std::endl;
        return;
    }

    // 重新选择数据空间以读取第 n 列的数据
    hsize_t offset_data[2] = {0, static_cast<hsize_t>(n)};
    hsize_t count_data[2] = {dims[0], 1};
    dataspace.selectHyperslab(H5S_SELECT_SET, count_data, offset_data);

    // 创建用于存储第 n 列数据的内存数据空间
    hsize_t mem_dims_data[1] = {dims[0]};
    H5::DataSpace memspace_data(1, mem_dims_data);

    // 创建存储第 n 列数据的数组
    double* data = new double[dims[0]];

    // 读取第 n 列数据
    dataset.read(data, H5::PredType::NATIVE_DOUBLE, memspace_data, dataspace);

   //舍弃数量 m_discard 暂不考虑 这里处理m_discard=0的情况
    size_t x_total=dims[0];//输入数据数量
    size_t x_start = 0;
    auto* dst = this->data();
    if (x_total < total) {//RPS数据少于输出，dst+差值
        dst = dst + total + m_discard - x_total;
    } else if (x_total > total) {//RPS数据大于输出，dst+差值
        x_start = x_total - total;
        dst = dst - x_start;
    }
    for (size_t i = x_start; i < x_total; ++i) {
        dst[i] = data[i];
    }
    delete[] data;

}


Indicator HKU_API EXTDATA(int n,const string& filepath ) {
    auto p = make_shared<IEXTDATA>();
    p->setParam<int>("ndigits", n);
    p->setParam<string>("filepath",filepath);
    return Indicator(p);
}

} /* namespace hku */
