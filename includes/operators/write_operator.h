#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "columns.h"
#include "data_types.h"
#include "operator.h"
#include "table.h"

#include "json.hpp"


namespace sdb {

template <typename T>
class WriteOperator : public Operator {
 public:
  void AddData(Table_Vec tables) {
    assert(tables.size() == 1 && "WriteOperator: Tables size > 1\n");
    this->tables = std::move(tables);
  }

  void Execute() { static_cast<T *>(this)->WriteTable(); }

  Table_Vec GetData() { return std::move(tables); }

 protected:
  Table_Vec tables;
};

class StdOutWriter : public WriteOperator<StdOutWriter> {
 public:
  void WriteTable();
};

class FileWriter : public WriteOperator<FileWriter> {
 public:
  void WriteTable();

 private:
  template <typename V>
  sjp::Json GetColumn(size_t col_idx) {
    auto json = sjp::JsonBuilder<sjp::JsonType::jarray>();
    auto row_size = tables[0]->GetRowSize();
    auto col_ptr = static_cast<V *>(tables[0]->GetColumn(col_idx));
    for (auto i = 0; i < row_size; ++i) {
      json.AppendOrUpdate(sjp::Json::end, (*col_ptr)[i]);
    }
    return json;
  }

  sjp::Json GetColumnObj(size_t col_idx, Data_Type type) {
    switch (type) {
      case DT_INT:
        return GetColumn<Int64Column>(col_idx);
      case DT_DOUBLE:
        return GetColumn<DoubleColumn>(col_idx);
      case DT_STRING:
        return GetColumn<StringColumn>(col_idx);
      default:
        throw std::runtime_error("FileWriter: Invalid type\n");
    }
  }
};
}  // namespace sdb
