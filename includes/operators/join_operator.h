#pragma once

#include <functional>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <vector>

#include "columns.h"
#include "data_types.h"
#include "operator.h"

namespace sdb {
// Join on Equality
class JoinOperator : public Operator {
 public:
  JoinOperator(std::string lt, std::string rt, std::string lc, std::string rc)
      : left_tbl_name(lt),
        right_tbl_name(rt),
        left_col_name(lc),
        right_col_name(rc) {}

  void AddData(Table_Vec tables) {
    assert(tables.size() == 2 && "JoinOperator: Tables size != 2\n");
    input_tables = std::move(tables);
  }

  Table_Vec GetData();

  void Execute();

 private:
  Table_Vec input_tables;
  Table_Vec output_tables;
  std::string left_tbl_name, right_tbl_name;
  std::string left_col_name, right_col_name;
  std::vector<std::pair<size_t, size_t>> idxs;
  // Token op_token;

  template <bool left>
  std::unique_ptr<BaseColumn> GetColumn(BaseColumn *column, Data_Type type);

  template <typename T, typename V, bool left>
  std::unique_ptr<BaseColumn> GetColumn(BaseColumn *column) {
    std::vector<T> vec;
    vec.reserve(idxs.size());

    V *pcol = static_cast<V *>(column);

    for (auto [l, r] : idxs) {
      if constexpr (left) {
        vec.push_back((*pcol)[l]);
      } else {
        vec.push_back((*pcol)[r]);
      }
    }

    return std::make_unique<V>(vec.size(), std::move(vec));
  }
};

template <typename T, typename V>
class HashColumn {
 public:
  HashColumn(BaseColumn *column) { ptr = static_cast<V *>(column); }

  void Process() {
    auto n_rows = ptr->size();
    for (auto i = 0; i < n_rows; ++i) {
      hash_map[(*ptr)[i]].push_back(i);
    }
  }

  std::vector<size_t> Probe(T val) { return hash_map[val]; }

 private:
  V *ptr;

  std::unordered_map<T, std::vector<size_t>> hash_map;
};

template <typename T, typename V>
std::vector<std::pair<size_t, size_t>> ProcessTables(BaseColumn *left,
                                                     BaseColumn *right) {
  std::vector<std::pair<size_t, size_t>> output;

  auto *lptr = static_cast<V *>(left);
  auto nrows = lptr->size();

  auto hasher = HashColumn<T, V>(right);
  hasher.Process();

  for (auto i = 0; i < nrows; ++i) {
    auto idxs = hasher.Probe((*lptr)[i]);
    for (auto idx : idxs) {
      output.emplace_back(i, idx);
    }
  }

  return output;
}

std::vector<std::pair<size_t, size_t>> ProcessTables(BaseColumn *left,
                                                     BaseColumn *right,
                                                     Data_Type type);

}  // namespace sdb
