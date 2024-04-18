#pragma once

#include <stdexcept>
#include <unordered_set>

#include "state_machine.h"
#include "tokenizer.h"

namespace sdb {
class JoinStateMachine : StateMachine {
 public:
  JoinStateMachine() {
    current_state = begin;
    expected_next_state.insert(tbl_name);
  }

  bool EOP() { return current_state == right_paren; }

  bool CheckErrorState() {
    return current_state == error || current_state == undefined;
  }

  bool CheckTransition(Token token, std::string word);

  std::string GetErrorMsg();

  std::string left_table;
  std::string right_table;
  std::string left_column;
  std::string right_column;
  std::string str_op;
  Token op_token;

  // BinaryOp_Ptr GetOperator();

 private:
  enum State {
    begin,
    tbl_name,
    join,
    on,
    col_name,
    op,
    left_paren,
    right_paren,
    end,
    error,
    undefined
  };

  enum State current_state;
  std::unordered_set<State> expected_next_state;
  std::string err_msg;

  bool check_tbl_name_state(std::string word);
  bool check_join_state();
  bool check_on_state();
  bool check_col_name_state(std::string word);
  bool check_op_state(Token token, std::string word);
  bool check_left_paren_state();
  bool check_right_paren_state();
  bool check_where_state();
  bool check_end_state();

  std::string check_column_name(std::string col_name, std::string table_name);
};
}  // namespace sdb
