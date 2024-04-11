#pragma once

#include <memory>

#include "data_types.h"
#include "operator.h"

namespace tdb {
class CreateOperator : public Operator {
 public:
  CreateOperator(std::string tbl_name, std::vector<std::string> col_name,
                 std::vector<Data_Type> col_type)
      : table_name(tbl_name), column_names(col_name), column_types(col_type) {}

  void AddData(Table_Vec tables) {}

  Table_Vec GetData() { return std::move(tables); }

  void Execute() {
    assert(column_names.size() == column_types.size() &&
           "CreateOperator column_names.size() != column_types.size()");

    tables.push_back(std::make_unique<Table>(
        column_names.size(), 0, table_name, column_names, column_types));
  }

 private:
  Table_Vec tables;
  std::string table_name;
  std::vector<std::string> column_names;
  std::vector<Data_Type> column_types;
};
}  // namespace tdb
