#include "operators/operator.h"

#include <cassert>
#include <stdexcept>

#include "columns.h"
#include "data_types.h"
#include "table.h"

namespace sdb {
Table_Vec BinaryOperator::GetData() {
  assert(tables.size() == 1 && "Binary Operator: Tables size != 1\n");

  auto n_rows = arg_results.size();
  auto n_cols = tables[0]->GetColumnSize();
  auto table_name = tables[0]->GetTableName();
  std::vector<Data_Type> ctypes;
  std::vector<std::string> cnames;

  for (auto i = 0; i < n_cols; ++i) {
    ctypes.push_back(tables[0]->GetColumnType(i));
    cnames.push_back(tables[0]->GetColumnName(i));
  }

  output.push_back(
      std::make_unique<Table>(n_cols, n_rows, table_name, cnames, ctypes));

  for (auto i = 0; i < n_cols; ++i) {
    auto ptr = GetColumn(i, ctypes[i]);
    output[0]->SetColumn(i, std::move(ptr));
  }

  return std::move(output);
}

std::unique_ptr<BaseColumn> BinaryOperator::GetColumn(size_t idx,
                                                      Data_Type type) {
  switch (type) {
    case DT_INT:
      return GetColumn<sINT::type, Int64Column>(idx);
    case DT_DOUBLE:
      return GetColumn<sDOUBLE::type, DoubleColumn>(idx);
    case DT_STRING:
      return GetColumn<sSTRING::type, StringColumn>(idx);
    default:
      throw std::runtime_error("BinaryOperator: Invalid column type\n");
  }
}

}  // namespace sdb
