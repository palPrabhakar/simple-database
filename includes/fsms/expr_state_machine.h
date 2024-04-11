#pragma once

#include <string>
#include <unordered_set>
#include <vector>

#include "state_machine.h"
#include "tokenizer.h"

namespace tdb {
class ExprStateMachine : StateMachine {
 public:
  ExprStateMachine() {
    current_state = begin;
    expected_next_state.insert(left_paren);
  }

  bool EOP() { return current_state == right_paren; }

  bool CheckErrorState() {
    return current_state == error || current_state == undefined;
  }

  bool CheckTransition(Token token, std::string word);
  std::string GetErrorMsg();

  std::string column_name;
  std::string str_op;
  Token op_token;
  std::string column_val;

  BinaryOp_Ptr GetOperator();

 private:
  enum State {
    begin,
    col_name,
    col_value,
    left_paren,
    right_paren,
    op,
    end,
    error,
    undefined,
  };

  enum State current_state;
  std::unordered_set<State> expected_next_state;
  std::string err_msg;

  bool check_col_name_state(std::string word);
  bool check_op_state(Token token, std::string word);
  bool check_col_val_state(std::string word);
  bool check_left_paren_state();
  bool check_right_paren_state();
};
}  // namespace tdb
