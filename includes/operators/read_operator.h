#pragma once

#include <format>
#include <memory>
#include <string>

#include "columns.h"
#include "data_types.h"
#include "json.hpp"
#include "operator.h"

namespace sdb {
template <typename T>
class ReadOperator : public Operator {
 public:
  ReadOperator(std::string fname) : file_name(fname) {}

  void AddData(Table_Vec tables) { this->tables = std::move(tables); }

  void Execute() { static_cast<T *>(this)->ReadTable(); }

  Table_Vec GetData() { return std::move(tables); }

 protected:
  std::string file_name;
  Table_Vec tables;
};

class JsonReader : public ReadOperator<JsonReader> {
 public:
  JsonReader(std::string fname)
      : ReadOperator(std::format(
            "/home/pal/workspace/simple-database/tables/{}.json", fname)) {}
  void ReadTable();

 private:
  std::unique_ptr<BaseColumn> GetColumnValues(const sjp::Json &data,
                                              Data_Type type,
                                              const size_t size);
  template <typename ColType, typename JType>
  std::unique_ptr<BaseColumn> GetColumn(const sjp::Json &data,
                                        const size_t size) {
    using VecType = typename ColType::val_type;
    std::vector<VecType> vec;
    vec.reserve(size);

    assert(data.Size() == size &&
           "ReadOperator: Number of columns don't match given size\n");

    for (int i = 0; i < data.Size(); ++i) {
      vec.push_back(static_cast<VecType>(*data.Get(i)->Get<JType>()));
    }

    return std::make_unique<ColType>(size, std::move(vec));
  }
};

class FlatReader : public ReadOperator<FlatReader> {
 public:
  FlatReader(std::string fname)
      : ReadOperator(std::format(
            "/home/pal/workspace/simple-database/tables/{}.sdb", fname)) {}

  void ReadTable();
};

}  // namespace sdb
