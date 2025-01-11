#pragma once

#include <memory>
#include <stdexcept>

#include "columns.h"
#include "data_types.h"
#include "operator.h"

namespace sdb {
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

    tables.push_back(std::make_unique<Table>(column_names.size(), 0, table_name,
                                             column_names, column_types));

    size_t tidx = tables.size() - 1;
    for (size_t i = 0; i < column_names.size(); ++i) {
      switch (column_types[i]) {
        case DT_INT: {
          tables[tidx]->SetColumn(i, std::make_unique<Int64Column>(0));
        } break;
        case DT_DOUBLE: {
          tables[tidx]->SetColumn(i, std::make_unique<DoubleColumn>(0));
        } break;
        case DT_STRING: {
          tables[tidx]->SetColumn(i, std::make_unique<StringColumn>(0));
        } break;
        default:
          throw std::runtime_error("Invalid column type - Create Operator\n");
      }
    }
  }

 private:
  Table_Vec tables;
  std::string table_name;
  std::vector<std::string> column_names;
  std::vector<Data_Type> column_types;
};
}  // namespace sdb
