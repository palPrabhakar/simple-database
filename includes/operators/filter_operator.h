#pragma once

#include <iostream>
#include <stdexcept>

#include "operator.h"

namespace tdb {

template <class A>
class FilterOperator : public BinaryOperator {
 public:
  FilterOperator(std::string name, std::string val)
      : col_name(name), value(val) {}

  void Execute() {
    FilterColumns(tables[0].get());
  }

  std::vector<size_t> GetArgResults(const Table *ptr) {
    FilterColumns(ptr);
    return std::move(arg_results);
  }

 protected:
  std::string col_name;
  std::string value;
  Data_Type type;

  void FilterColumns(const Table *ptr) {
    auto col_idx = ptr->GetColumnIndex(this->col_name);
    auto *col = ptr->GetColumn(col_idx);
    auto row_size = ptr->GetRowSize();
    try {
      switch (type) {
        case DT_INT:
          static_cast<A *>(this)->FilterInt64(col, std::stol(value), row_size);
          break;
        case DT_DOUBLE:
          static_cast<A *>(this)->FilterDouble(col, std::stod(value), row_size);
          break;
        case DT_STRING:
          static_cast<A *>(this)->FilterStrings(col, value, row_size);
          break;
        default:
          std::cout << "type: " << type << std::endl;
          throw std::runtime_error(
              "Filter Op: Invalid column type. Column name: " + col_name +
              "\n");
      }
    } catch (std::invalid_argument const &ex) {
      std::cout << "Filter Operator: Value and Column type mismatch\n";
    }
  }
};

class EqualityFilter : public FilterOperator<EqualityFilter> {
 public:
  EqualityFilter(std::string col_name, std::string col_val)
      : FilterOperator<EqualityFilter>(col_name, col_val) {}

  void FilterStrings(BaseColumn *ptr, std::string val, size_t row_size);
  void FilterInt64(BaseColumn *ptr, int64_t val, size_t row_size);
  void FilterDouble(BaseColumn *ptr, double val, size_t row_size);
};

class NonEqualityFilter : public FilterOperator<NonEqualityFilter> {
 public:
  NonEqualityFilter(std::string col_name, std::string col_val)
      : FilterOperator<NonEqualityFilter>(col_name, col_val) {}

  void FilterStrings(BaseColumn *ptr, std::string val, size_t row_size);
  void FilterInt64(BaseColumn *ptr, int64_t val, size_t row_size);
  void FilterDouble(BaseColumn *ptr, double val, size_t row_size);
};

class LessThanFilter : public FilterOperator<LessThanFilter> {
 public:
  LessThanFilter(std::string col_name, std::string col_val)
      : FilterOperator<LessThanFilter>(col_name, col_val) {}

  void FilterStrings(BaseColumn *ptr, std::string val, size_t row_size) {
    throw std::runtime_error("LessThanFilter doesn't support strings\n");
  }

  void FilterInt64(BaseColumn *ptr, int64_t val, size_t row_size);
  void FilterDouble(BaseColumn *ptr, double val, size_t row_size);
};

class GreaterThanFilter : public FilterOperator<GreaterThanFilter> {
 public:
  GreaterThanFilter(std::string col_name, std::string col_val)
      : FilterOperator<GreaterThanFilter>(col_name, col_val) {}

  void FilterStrings(BaseColumn *ptr, std::string val, size_t row_size) {
    throw std::runtime_error("GreaterThanFilter doesn't support strings\n");
  }

  void FilterInt64(BaseColumn *ptr, int64_t val, size_t row_size);
  void FilterDouble(BaseColumn *ptr, double val, size_t row_size);
};

class LessEqualFilter : public FilterOperator<LessEqualFilter> {
 public:
  LessEqualFilter(std::string col_name, std::string col_val)
      : FilterOperator<LessEqualFilter>(col_name, col_val) {}

  void FilterStrings(BaseColumn *ptr, std::string val, size_t row_size) {
    throw std::runtime_error("LessEqualFilter doesn't support strings\n");
  }

  void FilterInt64(BaseColumn *ptr, int64_t val, size_t row_size);
  void FilterDouble(BaseColumn *ptr, double val, size_t row_size);
};

class GreaterEqualFilter : public FilterOperator<GreaterEqualFilter> {
 public:
  GreaterEqualFilter(std::string col_name, std::string col_val)
      : FilterOperator<GreaterEqualFilter>(col_name, col_val) {}

  void FilterStrings(BaseColumn *ptr, std::string val, size_t row_size) {
    throw std::runtime_error("GreaterEqualFilter doesn't support strings\n");
  }

  void FilterInt64(BaseColumn *ptr, int64_t val, size_t row_size);
  void FilterDouble(BaseColumn *ptr, double val, size_t row_size);
};

}  // namespace tdb
