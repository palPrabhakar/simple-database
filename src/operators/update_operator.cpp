#include "operators/update_operator.h"

#include <iostream>
#include <numeric>

namespace sdb {
void UpdateOperator::Execute() {
  if (op) {
    args = op->GetArgResults(tables[0].get());
  } else {
    std::vector<size_t> v(tables[0]->GetRowSize());
    std::iota(v.begin(), v.end(), 0);
    args = std::move(v);
  }

  for (auto i = 0; i < column_names.size(); ++i) {
    auto col_idx = tables[0]->GetColumnIndex(column_names[i]);
    auto col_type = tables[0]->GetColumnType(col_idx);
    UpdateColumn(col_idx, args, col_type, column_values[i]);
  }
}
}  // namespace sdb
