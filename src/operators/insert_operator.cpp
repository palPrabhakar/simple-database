#include "operators/insert_operator.h"

#include <iostream>
#include <stdexcept>

namespace tdb {
void InsertOperator::Execute() {
  assert(tables.size() == 1 && "InsertOperator: Tables size != 1\n");

  auto n_cols = tables[0]->GetColumnSize();

  if (n_cols != column_values.size()) {
    throw std::runtime_error(
        "Unable to insert values into the table, the number of values do not "
        "match column size\n");
  }

  for (auto i = 0; i < n_cols; ++i) {
    auto type = tables[0]->GetColumnType(i);
    UpdateColumn(i, type, column_values[i]);
  }

  auto row_size = tables[0]->GetRowSize();
  tables[0]->SetRowSize(++row_size);
}
}  // namespace tdb
