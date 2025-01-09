#pragma once

// #include <json/value.h>
#include <format>
#include <memory>
#include <string>

#include "columns.h"
#include "data_types.h"
#include "json.hpp"
#include "operator.h"
#include "parser.hpp"

namespace sdb {
class ReadOperator : public Operator {
 public:
  ReadOperator(std::string fname) {
    file_name = std::format(
        "/home/pal/workspace/"
        "simple-database/tables/{}.json",
        fname);
  }

  void AddData(Table_Vec tables) { this->tables = std::move(tables); }

  void Execute() { ReadTable(); }

  Table_Vec GetData() { return std::move(tables); }

 private:
  std::string file_name;
  Table_Vec tables;

  void ReadTable();
};

std::unique_ptr<BaseColumn> GetColumnValues(const sjp::Json &data, Data_Type type,
                                            const size_t size);
template <typename T>
std::unique_ptr<BaseColumn> GetColumn(const sjp::Json &data, const size_t size);
}  // namespace sdb
