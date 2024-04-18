#include "operators/write_operator.h"

#include <json/value.h>

#include <format>
#include <fstream>
#include <future>
#include <iostream>
#include <sstream>

#include "json/json.h"

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

void FileWriter::WriteTable() {
  auto ncols = tables[0]->GetColumnSize();
  auto nrows = tables[0]->GetRowSize();

  Json::Value obj;
  Json::Value schema(Json::objectValue);

  schema["name"] = tables[0]->GetTableName();
  schema["ncols"] = ncols;
  schema["nrows"] = nrows;

  Json::Value columns(Json::arrayValue);
  Json::Value types(Json::arrayValue);

  Json::Value data;
  std::vector<std::future<Json::Value>> tasks;
  for (auto i = 0; i < ncols; ++i) {
    auto col_name = tables[0]->GetColumnName(i);
    auto col_type = tables[0]->GetColumnType(i);
    columns.append(col_name);
    types.append(col_type);
    tasks.push_back(std::async(std::launch::async, &FileWriter::GetColumnObj, this, i, col_type));
    // data[col_name] = GetColumn(i, col_type);
  }

  for (auto &task : tasks) {
    task.wait();
  }

  for (size_t i = 0; i < ncols; ++i) {
    auto col_name = tables[0]->GetColumnName(i);
    data[col_name] = tasks[i].get();
  }

  schema["columns"] = columns;
  schema["types"] = types;
  obj["schema"] = schema;
  obj["data"] = data;

  Json::StreamWriterBuilder builder;
  builder["commentStyle"] = "None";
  builder["indentation"] = "   ";

  std::unique_ptr<Json::StreamWriter> writer(builder.newStreamWriter());

  auto file_path = std::format(
      "/home/pal/workspace/simple-database/tables/{}.json",
      tables[0]->GetTableName());
  std::ofstream ofs(file_path);
  writer->write(obj, &ofs);
}

}  // namespace sdb
