/*
 * IEXTDATA.h
 *
 *  Created on: 2013-2-14
 *      Author: fasiondog
 */

#pragma once
#include "../Indicator.h"

namespace hku {

/**
 * 包装RPS成Indicator，用于其他指标计算    
 * @ingroup Indicator
 */

 
Indicator HKU_API EXTDATA(int n ,const string&  file_path);
inline Indicator EXTDATA(const Indicator& ind,int n,const string&  file_path) {
    return EXTDATA(n,file_path)(ind);
}

}  // namespace hku
