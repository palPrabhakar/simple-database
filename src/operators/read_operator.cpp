#include "operators/read_operator.h"

#include <fstream>
#include <future>
#include <memory>
#include <stdexcept>

#include "data_types.h"
#include "json.hpp"
#include "parser.hpp"

namespace sdb {
void JsonReader::ReadTable() {
  std::ifstream ifs;
  ifs.open(file_name);

  try {
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
}  // namespace sdb
