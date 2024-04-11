#include "operators/project_operator.h"

#include <curses.h>

#include <memory>

namespace tdb {
void ProjectOperator::ProcessTable() {
  assert(input_tables.size() == 1 && "Project Operator: Tables size != 1\n");
  auto n_rows = input_tables[0]->GetRowSize();
  auto n_cols = column_names.size();
  std::vector<Data_Type> col_types;
  std::vector<size_t> col_idxs;

  if (n_cols != 0) {
    for (auto col_name : column_names) {
      auto idx = input_tables[0]->GetColumnIndex(col_name);
      col_idxs.push_back(idx);
      col_types.push_back(input_tables[0]->GetColumnType(idx));
    }
  } else {
    n_cols = input_tables[0]->GetColumnSize();
    for (auto i = 0; i < n_cols; ++i) {
      column_names.push_back(input_tables[0]->GetColumnName(i));
      col_idxs.push_back(i);
      col_types.push_back(input_tables[0]->GetColumnType(i));
    }
  }

  output_tables.push_back(std::make_unique<Table>(
      n_cols, n_rows, "temp-project", column_names, col_types));

  auto col_idx = 0;
  for (auto idx : col_idxs) {
    auto ptr = input_tables[0]->ReleaseColumn(idx);
    output_tables[0]->SetColumn(col_idx++, std::move(ptr));
  }
}
}  // namespace tdb
