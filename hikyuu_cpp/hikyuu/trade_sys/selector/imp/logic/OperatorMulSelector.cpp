/*
 *  Copyright (c) 2024 hikyuu.org
 *
 *  Created on: 2024-05-27
 *      Author: fasiondog
 */

#include "OperatorMulSelector.h"

#if HKU_SUPPORT_SERIALIZATION
BOOST_CLASS_EXPORT(hku::OperatorMulSelector)
#endif

namespace hku {

SystemWeightList OperatorMulSelector::_getSelected(Datetime date) {
    return getIntersectionSelected(date, [](double w1, double w2) { return w1 * w2; });
}

HKU_API SelectorPtr operator*(const SelectorPtr& se1, const SelectorPtr& se2) {
    return make_shared<OperatorMulSelector>(se1, se2);
}

}  // namespace hku