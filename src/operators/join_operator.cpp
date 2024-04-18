#include "operators/join_operator.h"

#include <format>

#include "data_types.h"
#include "operators/operator.h"

namespace sdb {
std::vector<std::pair<size_t, size_t>> ProcessTables(BaseColumn *left,
                                                     BaseColumn *right,
                                                     Data_Type type) {
  switch (type) {
    case DT_INT:
      return ProcessTables<sINT::type, Int64Column>(left, right);
    case DT_DOUBLE:
      return ProcessTables<sDOUBLE::type, DoubleColumn>(left, right);
    case DT_STRING:
      return ProcessTables<sSTRING::type, StringColumn>(left, right);
    default:
      throw std::runtime_error("JoinOperator: Invalid column type\n");
  }
}

void JoinOperator::Execute() {
  auto &ltable = input_tables[0];
  auto &rtable = input_tables[1];

  auto lidx = ltable->GetColumnIndex(left_col_name);
  auto ridx = rtable->GetColumnIndex(right_col_name);

  auto ltype = ltable->GetColumnType(lidx);
  auto rtype = rtable->GetColumnType(ridx);

  assert(ltype == rtype &&
         "JoinOperator: Left column type != Right column type\n");

  idxs = ProcessTables(ltable->GetColumn(lidx), rtable->GetColumn(ridx), ltype);
}

Table_Vec JoinOperator::GetData() {
  assert(idxs.size() != 0 && "Join resulted in empty table.\n");
  auto &ltable = input_tables[0];
  auto &rtable = input_tables[1];

  auto lcols = ltable->GetColumnSize();
  auto rcols = rtable->GetColumnSize();

  auto ncols = lcols + rcols;
  auto nrows = idxs.size();
  auto table_name = "temp-join";

  std::vector<std::string> col_names;
  std::vector<Data_Type> col_types;

  for (auto i = 0; i < lcols; ++i) {
    col_names.push_back(
        std::format("{}.{}", ltable->GetTableName(), ltable->GetColumnName(i)));
    col_types.push_back(ltable->GetColumnType(i));
  }

  for (auto i = 0; i < rcols; ++i) {
    col_names.push_back(
        std::format("{}.{}", rtable->GetTableName(), rtable->GetColumnName(i)));
    col_types.push_back(rtable->GetColumnType(i));
  }

  output_tables.push_back(
      std::make_unique<Table>(ncols, nrows, table_name, col_names, col_types));

  for (auto i = 0; i < lcols; ++i) {
    auto *col = ltable->GetColumn(i);
    auto type = ltable->GetColumnType(i);
    output_tables[0]->SetColumn(i, GetColumn<true>(col, type));
  }

  for (auto i = 0; i < rcols; ++i) {
    auto *col = rtable->GetColumn(i);
    auto type = rtable->GetColumnType(i);
    output_tables[0]->SetColumn(i + lcols, GetColumn<false>(col, type));
  }

  return std::move(output_tables);
}

template <bool left>
std::unique_ptr<BaseColumn> JoinOperator::GetColumn(BaseColumn *column,
                                                    Data_Type type) {
  switch (type) {
    case DT_INT:
      return GetColumn<sINT::type, Int64Column, left>(column);
    case DT_DOUBLE:
      return GetColumn<sDOUBLE::type, DoubleColumn, left>(column);
    case DT_STRING:
      return GetColumn<sSTRING::type, StringColumn, left>(column);
    default:
      throw std::runtime_error("BinaryOperator: Invalid column type\n");
  }
}

}  // namespace sdb
