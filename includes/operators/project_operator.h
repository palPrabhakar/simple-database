#pragma once

#include <string>
#include <vector>

#include "operator.h"

namespace tdb {
class ProjectOperator : public Operator {
 public:
  ProjectOperator(std::vector<std::string> col_names)
      : column_names(col_names) {}

  void AddData(Table_Vec tables) {
    assert(tables.size() == 1 && "ProjectOperator: Tables size > 1\n");
    input_tables = std::move(tables);
  }

  void Execute() { ProcessTable(); }

  Table_Vec GetData() { return std::move(output_tables); }

 private:
  std::vector<std::string> column_names;
  Table_Vec input_tables;
  Table_Vec output_tables;

  void ProcessTable();
};
}  // namespace tdb
