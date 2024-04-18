#pragma once

#include <memory>
#include <vector>

#include "columns.h"
#include "data_types.h"
#include "table.h"

namespace sdb {
using Table_Vec = std::vector<std::unique_ptr<Table>>;
using Table_Ptr = std::unique_ptr<Table>;

class Operator {
 public:
  virtual ~Operator() = default;
  virtual void AddData(Table_Vec tables) = 0;
  virtual void Execute() = 0;
  virtual Table_Vec GetData() = 0;
};

class BinaryOperator : public Operator {
 public:
  virtual ~BinaryOperator() = default;

  void AddData(Table_Vec tables) {
    assert(tables.size() == 1 && "WriteOperator: Tables size > 1\n");
    this->tables = std::move(tables);
  }

  Table_Vec GetData();

  virtual void Execute() = 0;
  virtual std::vector<size_t> GetArgResults(const Table *ptr) = 0;

 protected:
  Table_Vec tables;
  Table_Vec output;
  std::vector<size_t> arg_results;

  std::unique_ptr<BaseColumn> GetColumn(size_t idx, Data_Type type);

  template <typename A, typename B>
  std::unique_ptr<BaseColumn> GetColumn(size_t idx) {
    std::vector<A> vec;
    vec.reserve(arg_results.size());

    B *pcol = static_cast<B *>(tables[0]->GetColumn(idx));

    for (auto arg : arg_results) {
      vec.push_back((*pcol)[arg]);
    }

    return std::make_unique<B>(vec.size(), std::move(vec));
  }
};

}  // namespace sdb
