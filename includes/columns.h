#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace tdb {
class BaseColumn {
 public:
  virtual ~BaseColumn() = default;

  virtual void get_value(std::size_t index, int64_t &value) = 0;
  virtual void get_value(std::size_t index, double &value) = 0;
  virtual void get_value(std::size_t index, std::string &value) = 0;

  virtual void set_value(std::size_t index, int64_t value) = 0;
  virtual void set_value(std::size_t index, double value) = 0;
  virtual void set_value(std::size_t index, std::string value) = 0;

  virtual size_t size() const = 0;
};

template <typename T>
class Column : public BaseColumn {
 public:
  Column(size_t nsize) : vec(nsize), n_rows(nsize) {}
  Column(size_t nsize, std::vector<T> &&values) : vec(values), n_rows(nsize) {}

  void get_value(std::size_t index, int64_t &value) {
    if constexpr (std::is_same_v<T, int64_t>) {
      value = vec[index];
    } else {
      throw std::runtime_error("Invalid type. Expected int64_t\n");
    }
  }

  void get_value(std::size_t index, double &value) {
    if constexpr (std::is_same_v<T, double>) {
      value = vec[index];
    } else {
      throw std::runtime_error("Invalid type. Expected double\n");
    }
  }

  void get_value(std::size_t index, std::string &value) {
    if constexpr (std::is_same_v<T, std::string>) {
      value = vec[index];
    } else {
      throw std::runtime_error("Invalid type. Expected std::string\n");
    }
  }

  void set_value(std::size_t index, int64_t value) {
    if constexpr (std::is_same_v<T, int64_t>) {
      vec[index] = value;
    } else {
      throw std::runtime_error("Invalid type. Expected int64_t\n");
    }
  }

  void set_value(std::size_t index, double value) {
    if constexpr (std::is_same_v<T, double>) {
      vec[index] = value;
    } else {
      throw std::runtime_error("Invalid type. Expected double\n");
    }
  }

  void set_value(std::size_t index, std::string value) {
    if constexpr (std::is_same_v<T, std::string>) {
      vec[index] = value;
    } else {
      throw std::runtime_error("Invalid type. Expected std::string\n");
    }
  }

  size_t size() const { return n_rows; }

 protected:
  std::vector<T> vec;
  size_t n_rows;
};

class Int64Column : public Column<int64_t> {
 public:
  Int64Column(size_t nsize) : Column<int64_t>(nsize) {}
  Int64Column(size_t nsize, std::vector<int64_t> &&values)
      : Column<int64_t>(nsize, std::move(values)) {}

  int64_t &operator[](std::size_t idx) { return vec[idx]; }

  const int64_t &operator[](std::size_t idx) const { return vec[idx]; }

  void append(int64_t val) {
    vec.push_back(val);
    n_rows++;
  }
};

class DoubleColumn : public Column<double> {
 public:
  DoubleColumn(size_t nsize) : Column<double>(nsize) {}
  DoubleColumn(size_t nsize, std::vector<double> &&values)
      : Column<double>(nsize, std::move(values)) {}

  double &operator[](std::size_t idx) { return vec[idx]; }

  const double &operator[](std::size_t idx) const { return vec[idx]; }

  void append(double val) {
    vec.push_back(val);
    n_rows++;
  }
};

class StringColumn : public Column<std::string> {
 public:
  StringColumn(size_t nsize) : Column<std::string>(nsize) {}
  StringColumn(size_t nsize, std::vector<std::string> &&values)
      : Column<std::string>(nsize, std::move(values)) {}

  std::string &operator[](std::size_t idx) { return vec[idx]; }

  const std::string &operator[](std::size_t idx) const { return vec[idx]; }

  void append(std::string val) {
    vec.push_back(std::move(val));
    n_rows++;
  }
};

}  // namespace tdb
