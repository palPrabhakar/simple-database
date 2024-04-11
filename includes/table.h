#pragma once

#include <algorithm>
#include <cassert>
#include <memory>
#include <stdexcept>
#include <vector>

#include "columns.h"
#include "data_types.h"

namespace tdb {
class Table {
  // Table class cannot be copied
  // Since unique_ptr cannot be copied
 public:
  Table(size_t n_cols, size_t n_rows, std::string table_name,
        std::vector<std::string> col_names, std::vector<Data_Type> type_col)
      : n_cols(n_cols),
        n_rows(n_rows),
        table_name(table_name),
        column_names(col_names),
        type_col(type_col),
        columns(n_cols) {}

  size_t GetColumnSize() const { return n_cols; }
  size_t GetRowSize() const { return n_rows; }

  void SetRowSize(size_t size) { n_rows = size; }
  void SetColumnSize(size_t size) { n_cols = size; }

  template <typename T>
  T GetValue(size_t col, size_t row) {
    assert(col < n_cols && "Column index out of bounds");
    assert(row < n_rows && "Row index out of bounds");

    T val;
    columns[col]->get_value(row, val);
    return val;
  }

  template <typename T>
  void SetValue(size_t col, size_t row, T val) {
    assert(col < n_cols && "Column index out of bounds");
    assert(row < n_rows && "Row index out of bounds");

    columns[col]->set_value(row, val);
  }

  BaseColumn *GetColumn(size_t col) const { return columns[col].get(); }

  std::unique_ptr<BaseColumn> ReleaseColumn(size_t col) {
    // ReleaseColumn leaves the table in indeterminate state
    std::unique_ptr<BaseColumn> column;
    column.swap(columns[col]);
    return column;
  }

  void SetColumn(size_t col, std::unique_ptr<BaseColumn> column) {
    assert(col < n_cols && "Column index out of bounds");
    columns[col] = std::move(column);
  }

  std::string GetTableName() const { return table_name; }

  std::string GetColumnName(size_t col) const  {
    assert(col < n_cols && "Column index out of bounds");
    return column_names[col];
  }

  size_t GetColumnIndex(std::string col_name) const {
    if (auto it = std::find(column_names.begin(), column_names.end(), col_name);
        it != column_names.end()) {
      return std::distance(column_names.begin(), it);
    } else {
      throw std::runtime_error("Invalid column name\n");
    }
  }

  Data_Type GetColumnType(size_t col) const {
    assert(col < n_cols && "Column index out of bounds");
    return type_col[col];
  }

 private:
  size_t n_cols;
  size_t n_rows;
  std::string table_name;
  std::vector<std::string> column_names;
  std::vector<Data_Type> type_col;
  std::vector<std::unique_ptr<BaseColumn>> columns;
};
}  // namespace tdb
