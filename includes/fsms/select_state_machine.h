#pragma once

#include <functional>
#include <string>
#include <unordered_set>
#include <vector>

#include "state_machine.h"
#include "tokenizer.h"

namespace sdb {
class SelectStateMachine : StateMachine {
 public:
  SelectStateMachine() {
    current_state = begin;
    expected_next_state.insert(select);
  }

  void RegisterWhereCallBack(std::function<Operator_Ptr()> func) {
    where_cb_func = func;
  }

  void RegisterJoinCallBack(std::function<Operator_Vec()> func) {
    join_cb_func = func;
  }

  bool EOP() { return current_state == end; }

  bool CheckErrorState() {
    return current_state == error || current_state == undefined;
  }

  bool CheckTransition(Token token, std::string word);
  std::string GetErrorMsg();
  std::string table_name;
  // empty col_names => all columns
  Operator_Ptr where_op;
  Operator_Vec join_ops;
  std::vector<std::string> col_names;
  bool join_clause = false;

 private:
  // clang-format off
  enum State {
    begin,
    select,
    star,
    from,
    tbl_name,
    column_name,
    where,
    join,
    on,
    end,
    error,
    undefined
  };
  // clang-format on

  enum State current_state;
  std::unordered_set<State> expected_next_state;
  std::string err_msg;
  std::function<Operator_Ptr()> where_cb_func;
  std::function<Operator_Vec()> join_cb_func;

  bool check_select_state();
  bool check_star_state();
  bool check_from_state();
  bool check_tbl_name_state(std::string word);
  bool check_col_name_state(std::string word);
  bool check_where_state();
  bool check_end_state();
  bool check_join_state();
};
}  // namespace sdb
