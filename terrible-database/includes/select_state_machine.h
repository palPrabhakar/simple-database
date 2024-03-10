#pragma once

#include "state_machine.h"
#include "tokenizer.h"
#include <functional>
#include <string>
#include <unordered_set>
#include <vector>

namespace tdb {

class SelectStateMachine : StateMachine {
public:
  SelectStateMachine() {
    current_state = begin;
    expected_next_state.insert(select);
  }

  void RegisterCallBack(std::function<std::vector<std::string>()> func) {
    callback_func = func;
  }

  bool EOP() { return current_state == end; }

  bool CheckErrorState() {
    return current_state == error || current_state == undefined;
  }

  bool CheckTransition(Token token, std::string word);
  std::string GetErrorMsg();
  std::string table_name;
  // empty col_names => all columns
  std::vector<std::string> col_names;
  std::vector<std::string> where_clause;

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
    end,
    error,
    undefined
  };
  // clang-format on

  enum State current_state;
  std::unordered_set<State> expected_next_state;
  std::string err_msg;
  std::function<std::vector<std::string>()> callback_func;

  bool check_select_state();
  bool check_star_state();
  bool check_from_state();
  bool check_tbl_name_state(std::string word);
  bool check_col_name_state(std::string word);
  bool check_where_state();
  bool check_end_state();
};
} // namespace tdb

