#include "operators/write_operator.h"

#include <format>
#include <fstream>
#include <future>
#include <iostream>
#include <sstream>

#include "json.hpp"

namespace sdb {
void StdOutWriter::WriteTable() {
  auto ncols = tables[0]->GetColumnSize();
  auto nrows = tables[0]->GetRowSize();

  std::cout << "\n";
  std::cout << tables[0]->GetTableName() << "\n";
  std::stringstream ss;
  for (size_t i = 0; i < ncols; ++i) {
    ss << tables[0]->GetColumnName(i) << "\t";
  }

  auto col_names = ss.str();
  std::string dash(1.5 * col_names.size(), '-');
  std::cout << dash << "\n";
  std::cout << col_names << "\n";
  std::cout << dash << "\n";
  for (size_t i = 0; i < nrows; ++i) {
    for (size_t j = 0; j < ncols; ++j) {
      switch (tables[0]->GetColumnType(j)) {
        case DT_INT:
          std::cout << tables[0]->GetValue<sINT::type>(j, i) << "\t";
          break;
        case DT_DOUBLE:
          std::cout << tables[0]->GetValue<sDOUBLE::type>(j, i) << "\t";
          break;
        case DT_STRING:
          std::cout << tables[0]->GetValue<sSTRING::type>(j, i) << "\t";
          break;
        default:
          break;
      }
    }
    std::cout << "\n";
  }
  std::cout << "\n";
}

void JsonWriter::WriteTable() {
  auto ncols = tables[0]->GetColumnSize();
  auto nrows = tables[0]->GetRowSize();

  auto json = sjp::JsonBuilder<sjp::JsonType::jobject>();

  auto schema = sjp::JsonBuilder<sjp::JsonType::jobject>();
  schema.InsertOrUpdate("name", tables[0]->GetTableName());
  schema.InsertOrUpdate("ncols", ncols);
  schema.InsertOrUpdate("nrows", nrows);

  auto columns = sjp::JsonBuilder<sjp::JsonType::jarray>();
  auto types = sjp::JsonBuilder<sjp::JsonType::jarray>();

  auto data = sjp::JsonBuilder<sjp::JsonType::jobject>();
  std::vector<std::future<sjp::Json>> tasks;
  for (auto i = 0; i < ncols; ++i) {
    auto col_name = tables[0]->GetColumnName(i);
    auto col_type = tables[0]->GetColumnType(i);
    columns.AppendOrUpdate(sjp::Json::end, col_name);
    types.AppendOrUpdate(sjp::Json::end, col_type);
    tasks.push_back(std::async(std::launch::async, &JsonWriter::GetColumnObj,
                               this, i, col_type));
  }

  for (auto &task : tasks) {
    task.wait();
  }

  for (size_t i = 0; i < ncols; ++i) {
    auto col_name = tables[0]->GetColumnName(i);
    data.InsertOrUpdate(col_name, tasks[i].get());
  }

  schema.InsertOrUpdate("columns", columns);
  schema.InsertOrUpdate("types", types);
  json.InsertOrUpdate("schema", schema);
  json.InsertOrUpdate("data", data);

  auto file_path =
      std::format("/home/pal/workspace/simple-database/tables/test.json",
                  tables[0]->GetTableName());
  std::ofstream ofs(file_path);
  json.Dump(ofs);
}

void FlatWriter::WriteTable() {
  auto file_path =
      std::format("/home/pal/workspace/simple-database/tables/test.sdb",
                  tables[0]->GetTableName());
  std::ofstream ofs(file_path, std::ios_base::binary);

  auto tbl_name = tables[0]->GetTableName();
  auto len = tbl_name.size();

  ofs.write(reinterpret_cast<char *>(&len), sizeof(len));
  ofs.write(tbl_name.data(), len);

  auto ncols = tables[0]->GetColumnSize();
  auto nrows = tables[0]->GetRowSize();

  ofs.write(reinterpret_cast<char *>(&ncols), sizeof(ncols));
  ofs.write(reinterpret_cast<char *>(&nrows), sizeof(nrows));

  for (size_t i = 0; i < ncols; ++i) {
    int type = tables[0]->GetColumnType(i);
    ofs.write(reinterpret_cast<char *>(&type), sizeof(type));
  }

  for (size_t i = 0; i < ncols; ++i) {
    auto col_name = tables[0]->GetColumnName(i);
    auto len = col_name.size();
    ofs.write(reinterpret_cast<char *>(&len), sizeof(len));
    ofs.write(col_name.data(), len);
  }

  for (size_t i = 0; i < ncols; ++i) {
    switch (tables[0]->GetColumnType(i)) {
      case DT_INT: {
        auto col = static_cast<Int64Column *>(tables[0]->GetColumn(i));
        ofs.write(reinterpret_cast<char *>(col->vec.data()),
                  sizeof(sINT::type) * col->size());
      } break;
      case DT_DOUBLE: {
        auto col = static_cast<DoubleColumn *>(tables[0]->GetColumn(i));
        ofs.write(reinterpret_cast<char *>(col->vec.data()),
                  sizeof(sDOUBLE::type) * col->size());
      } break;
      case DT_STRING: {
        auto col = static_cast<StringColumn *>(tables[0]->GetColumn(i));
        for (size_t j = 0; j < col->size(); ++j) {
          auto len = (*col)[j].size();
          ofs.write(reinterpret_cast<char *>(&len), sizeof(len));
          ofs.write((*col)[j].data(), len);
        }
      } break;
      default:
        throw std::runtime_error("Invalid column type\n");
    }
  }
}

}  // namespace sdb
