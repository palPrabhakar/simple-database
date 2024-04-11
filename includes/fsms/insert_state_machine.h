#pragma once

#include <string>
#include <unordered_set>
#include <vector>

#include "state_machine.h"
#include "tokenizer.h"

namespace tdb {
class InsertStateMachine : StateMachine {
 public:
  InsertStateMachine() {
    current_state = begin;
    expected_next_state.insert(insert);
  }

  bool EOP() { return current_state == end; }

  bool CheckErrorState() {
    return current_state == error || current_state == undefined;
  }

  bool CheckTransition(Token token, std::string word);
  std::string GetErrorMsg();

  std::string table_name;
  std::vector<std::string> col_values;

 private:
  // clang-format off
  enum State {
    begin,
    insert,
    tbl_name,
    values,
    col_value,
    end,
    error,
    undefined
  };
  // clang-format on

  enum State current_state;
  std::unordered_set<State> expected_next_state;
  std::string err_msg;

  bool check_insert_state();
  bool check_tbl_name_state(std::string word);
  bool check_values_state();
  bool check_col_val_state(std::string word);
  bool check_end_state();
};
}  // namespace tdb
