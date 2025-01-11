#include "operators/read_operator.h"

#include <fstream>
#include <future>
#include <memory>
#include <stdexcept>

#include "columns.h"
#include "data_types.h"
#include "json.hpp"
#include "parser.hpp"

namespace sdb {
void JsonReader::ReadTable() {
  try {
    std::ifstream ifs(file_name);
    auto parser = sjp::Parser(ifs);
    auto data = parser.Parse();

    size_t ncols =
        static_cast<size_t>(*data.Get("schema")->Get("ncols")->Get<double>());
    size_t nrows =
        static_cast<size_t>(*data.Get("schema")->Get("nrows")->Get<double>());
    auto table_name = *data.Get("schema")->Get("name")->Get<std::string>();

    const sjp::Json cols = *data.Get("schema")->Get("columns");
    const sjp::Json types = *data.Get("schema")->Get("types");

    std::vector<std::string> col_names;
    std::vector<Data_Type> col_types;

    assert(cols.Size() == types.Size() &&
           "Table: cols and type size does not match\n");

    for (int i = 0; i < cols.Size(); ++i) {
      col_names.push_back(*cols.Get(i)->Get<std::string>());
      col_types.push_back(static_cast<Data_Type>(*types.Get(i)->Get<double>()));
    }

    auto tidx = tables.size();
    tables.push_back(std::make_unique<Table>(ncols, nrows, table_name,
                                             col_names, col_types));

    std::vector<std::future<std::unique_ptr<BaseColumn>>> tasks;
    for (size_t i = 0; i < col_names.size(); ++i) {
      tasks.push_back(std::async(
          std::launch::async, &JsonReader::GetColumnValues, this,
          *data.Get("data")->Get(col_names[i]), col_types[i], nrows));
    }

    for (auto &task : tasks) {
      task.wait();
    }

    for (size_t i = 0; i < col_names.size(); ++i) {
      tables[tidx]->SetColumn(i, tasks[i].get());
    }

  } catch (std::exception &e) {
    throw e;
  }
}

std::unique_ptr<BaseColumn> JsonReader::GetColumnValues(const sjp::Json &data,
                                                        const Data_Type type,
                                                        const size_t size) {
  switch (type) {
    case DT_INT:
      return GetColumn<Int64Column, double>(data, size);
    case DT_DOUBLE:
      return GetColumn<DoubleColumn, double>(data, size);
    case DT_STRING:
      return GetColumn<StringColumn, std::string>(data, size);
    default:
      throw std::runtime_error("Invalid Type\n");
  }
}

void FlatReader::ReadTable() {
  try {
    std::ifstream ifs(file_name);

    size_t len;
    ifs.read(reinterpret_cast<char *>(&len), sizeof(len));
    std::string table_name;
    table_name.resize(len);
    ifs.read(table_name.data(), len);

    size_t ncols;
    ifs.read(reinterpret_cast<char *>(&ncols), sizeof(ncols));

    size_t nrows;
    ifs.read(reinterpret_cast<char *>(&nrows), sizeof(nrows));

    std::vector<Data_Type> col_types(ncols);
    ifs.read(reinterpret_cast<char *>(col_types.data()), sizeof(int) * ncols);

    std::vector<std::string> col_names;
    col_names.reserve(ncols);
    for (int i = 0; i < ncols; ++i) {
      size_t len;
      ifs.read(reinterpret_cast<char *>(&len), sizeof(len));
      std::string col_name;
      col_name.resize(len);
      ifs.read(col_name.data(), len);
      col_names.push_back(std::move(col_name));
    }

    auto tidx = tables.size();
    tables.push_back(
        std::make_unique<Table>(ncols, nrows, std::move(table_name),
                                std::move(col_names), std::move(col_types)));

    for (size_t i = 0; i < ncols; ++i) {
      switch (tables[tidx]->GetColumnType(i)) {
        case DT_INT: {
          std::vector<sINT::type> vec;
          vec.resize(nrows);
          ifs.read(reinterpret_cast<char *>(vec.data()),
                   sizeof(sINT::type) * nrows);
          tables[tidx]->SetColumn(
              i, std::make_unique<Int64Column>(nrows, std::move(vec)));
        } break;
        case DT_DOUBLE: {
          std::vector<sDOUBLE::type> vec;
          vec.resize(nrows);
          ifs.read(reinterpret_cast<char *>(vec.data()),
                   sizeof(sINT::type) * nrows);
          tables[tidx]->SetColumn(
              i, std::make_unique<DoubleColumn>(nrows, std::move(vec)));
        } break;
        case DT_STRING: {
          std::vector<sSTRING::type> vec;
          vec.reserve(nrows);
          for (size_t i = 0; i < nrows; ++i) {
            size_t len;
            ifs.read(reinterpret_cast<char *>(&len), sizeof(len));
            std::string col_name;
            col_name.resize(len);
            ifs.read(col_name.data(), len);
            vec.push_back(std::move(col_name));
          }
          tables[tidx]->SetColumn(
              i, std::make_unique<StringColumn>(nrows, std::move(vec)));
        } break;
        default:
          throw std::runtime_error("Invalid column data type - FlatReader\n");
      }
    }
  } catch (std::exception &e) {
    throw e;
  }
}
}  // namespace sdb
