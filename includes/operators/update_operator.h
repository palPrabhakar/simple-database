#pragma once

#include <algorithm>
#include <memory>
#include <utility>

#include "columns.h"
#include "data_types.h"
#include "operator.h"
#include "parser.h"

namespace sdb {
class UpdateOperator : public Operator {
 public:
  UpdateOperator(std::vector<std::string> cnames,
                 std::vector<std::string> cvals, BinaryOp_Ptr ptr = nullptr)
      : op(std::move(ptr)), column_names(cnames), column_values(cvals) {}

  void AddData(Table_Vec tables) {
    assert(tables.size() == 1 && "UpdateOperator: Tables size != 1\n");
    this->tables = std::move(tables);
  }

  Table_Vec GetData() { return std::move(tables); }

  void Execute();

 private:
  Table_Vec tables;
  BinaryOp_Ptr op;
  std::vector<std::string> column_names;
  std::vector<std::string> column_values;
  std::vector<size_t> args;

  template <typename T, typename V>
  void UpdateColumn(size_t col_idx, std::vector<size_t> row_idx, T val) {
    for (auto idx : row_idx) {
      auto col_ptr = static_cast<V *>(tables[0]->GetColumn(col_idx));
      (*col_ptr)[idx] = val;
    }
  }

  void UpdateColumn(size_t col_idx, std::vector<size_t> row_idx, Data_Type type,
                    std::string val) {
    switch (type) {
      case DT_INT:
        UpdateColumn<sINT::type, Int64Column>(col_idx, row_idx, std::stol(val));
        break;
      case DT_DOUBLE:
        UpdateColumn<sDOUBLE::type, DoubleColumn>(col_idx, row_idx,
                                                  std::stod(val));
        break;
      case DT_STRING:
        UpdateColumn<sSTRING::type, StringColumn>(col_idx, row_idx, val);
        break;
      default:
        throw std::runtime_error("UpdateOperator: Invalid type\n");
    }
  }
};
}  // namespace sdb
