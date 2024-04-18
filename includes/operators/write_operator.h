#pragma once

#include <json/value.h>

#include <memory>
#include <stdexcept>
#include <vector>

#include "columns.h"
#include "data_types.h"
#include "json/json.h"
#include "operator.h"
#include "table.h"

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
  Json::Value GetColumn(size_t col_idx) {
    Json::Value column(Json::arrayValue);
    auto row_size = tables[0]->GetRowSize();
    auto col_ptr = static_cast<V *>(tables[0]->GetColumn(col_idx));
    for (auto i = 0; i < row_size; ++i) {
      column.append((*col_ptr)[i]);
    }
    return column;
  }

  Json::Value GetColumnObj(size_t col_idx, Data_Type type) {
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
