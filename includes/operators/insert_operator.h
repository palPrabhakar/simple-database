#pragma once

#include <memory>
#include <utility>

#include "columns.h"
#include "data_types.h"
#include "operator.h"

namespace tdb {
class InsertOperator : public Operator {
 public:
  InsertOperator(std::vector<std::string> col_values)
      : column_values(col_values) {}

  void AddData(Table_Vec tables) {
    assert(tables.size() == 1 && "InsertOperator: Tables size != 1\n");
    this->tables = std::move(tables);
  }

  Table_Vec GetData() { return std::move(tables); }

  void Execute();

 private:
  Table_Vec tables;
  std::vector<std::string> column_values;

  template <typename T, typename V>
  void UpdateColumn(size_t col_idx, T val) {
    auto col_ptr = static_cast<V *>(tables[0]->GetColumn(col_idx));
    col_ptr->append(val);
  }

  void UpdateColumn(size_t col_idx, Data_Type type, std::string val) {
    switch (type) {
      case DT_INT:
        UpdateColumn<int64_t, Int64Column>(col_idx, std::stol(val));
        break;
      case DT_DOUBLE:
        UpdateColumn<double, DoubleColumn>(col_idx, std::stod(val));
        break;
      case DT_STRING:
        UpdateColumn<std::string, StringColumn>(col_idx, val);
        break;
      default:
        throw std::runtime_error("InsertOperator: Invalid type\n");
    }
  }
};
}  // namespace tdb
