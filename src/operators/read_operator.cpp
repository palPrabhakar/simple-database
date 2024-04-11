#include "operators/read_operator.h"

#include <cstdint>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>

#include "data_types.h"
#include "json/json.h"

namespace tdb {

template <>
std::unique_ptr<BaseColumn> GetColumn<int64_t>(const Json::Value &data,
                                               const size_t size) {
  std::vector<int64_t> vec;
  vec.reserve(size);

  assert(data.size() == size &&
         "ReadOperator: Number of columns don't match given size\n");

  for (int i = 0; i < data.size(); ++i) {
    vec.push_back(data[i].asInt64());
  }

  return std::make_unique<Int64Column>(size, std::move(vec));
}

template <>
std::unique_ptr<BaseColumn> GetColumn<double>(const Json::Value &data,
                                              const size_t size) {
  std::vector<double> vec;
  vec.reserve(size);

  assert(data.size() == size &&
         "ReadOperator: Number of columns don't match given size\n");

  for (int i = 0; i < data.size(); ++i) {
    vec.push_back(data[i].asDouble());
  }

  return std::make_unique<DoubleColumn>(size, std::move(vec));
}

template <>
std::unique_ptr<BaseColumn> GetColumn<std::string>(const Json::Value &data,
                                                   const size_t size) {
  std::vector<std::string> vec;
  vec.reserve(size);

  assert(data.size() == size &&
         "ReadOperator: Number of columns don't match given size\n");

  for (int i = 0; i < data.size(); ++i) {
    vec.push_back(data[i].asString());
  }

  return std::make_unique<StringColumn>(size, std::move(vec));
}

void ReadOperator::ReadTable() {
  Json::Value data;
  std::ifstream ifs;
  ifs.open(file_name);

  Json::CharReaderBuilder builder;
  builder["collectComments"] = false;
  JSONCPP_STRING errs;
  if (!parseFromStream(builder, ifs, &data, &errs)) {
    throw std::runtime_error(errs.c_str());
  }

  size_t ncols = data["schema"]["ncols"].asUInt64();
  size_t nrows = data["schema"]["nrows"].asUInt64();
  auto table_name = data["schema"]["name"].asString();

  const Json::Value cols = data["schema"]["columns"];
  const Json::Value types = data["schema"]["types"];

  std::vector<std::string> col_names;
  std::vector<Data_Type> col_types;

  assert(cols.size() == types.size() &&
         "Table: cols and type size does not match\n");

  for (int i = 0; i < cols.size(); ++i) {
    col_names.push_back(cols[i].asString());
    col_types.push_back(static_cast<Data_Type>(types[i].asInt()));
  }

  // assert(tables.size() == 0 && "Vec<Table> not empty\n");

  auto tidx = tables.size();
  tables.push_back(
      std::make_unique<Table>(ncols, nrows, table_name, col_names, col_types));

  std::vector<std::future<std::unique_ptr<BaseColumn>>> tasks;
  for (size_t i = 0; i < col_names.size(); ++i) {
    tasks.push_back(std::async(std::launch::async, GetColumnValues,
                                  data["data"][col_names[i]], col_types[i],
                                  nrows));
  }

  for (auto &task : tasks) {
    task.wait();
  }

  for (size_t i = 0; i < col_names.size(); ++i) {
    tables[tidx]->SetColumn(i, tasks[i].get());
  }
}

std::unique_ptr<BaseColumn> GetColumnValues(const Json::Value &data,
                                            const Data_Type type,
                                            const size_t size) {
  switch (type) {
    case DT_INT:
      return GetColumn<int64_t>(data, size);
    case DT_DOUBLE:
      return GetColumn<double>(data, size);
    case DT_STRING:
      return GetColumn<std::string>(data, size);
    default:
      throw std::runtime_error("Invalid Type\n");
  }
}
}  // namespace tdb
