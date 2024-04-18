#include "fsms/join_state_machine.h"

#include <iostream>
#include <string>

#include "tokenizer.h"

namespace sdb {
bool JoinStateMachine::CheckTransition(Token token, std::string word) {
  // end of parsing
  if (current_state == right_paren) return false;

  switch (token) {
    case JOIN:
      return check_join_state();
    case ON:
      return check_on_state();
    case LEFT_PAREN:
      return check_left_paren_state();
    case RIGHT_PAREN:
      return check_right_paren_state();
    case TEXT:
      if (current_state == begin || current_state == join) {
        return check_tbl_name_state(word);
      } else {
        return check_col_name_state(word);
      }
    case OP_EQ:
      return check_op_state(token, word);
    default:
      current_state = undefined;
      return false;
  }
}

std::string JoinStateMachine::GetErrorMsg() {
  if (current_state == error || current_state == undefined) {
    err_msg = "Failed to parse join clause.";
    for (auto val : expected_next_state) {
      switch (val) {
        case tbl_name:
          err_msg += " Expected table name.";
          break;
        case col_name:
          err_msg += " Expected column name.";
          break;
        case op:
          err_msg += " Expected a binary operator.";
          break;
        case join:
          err_msg += " Expected keyword join.";
          break;
        case on:
          err_msg += " Expected keyword on.";
          break;
        case right_paren:
          err_msg += " Expected right parenthesis.";
          break;
        case left_paren:
          err_msg += " Expected left parenthesis.";
          break;
        default:
          break;
      }
    }
  }
  return err_msg;
}

bool JoinStateMachine::check_tbl_name_state(std::string word) {
  if (expected_next_state.contains(tbl_name)) {
    expected_next_state.clear();
    if (current_state == join) {
      expected_next_state.insert(on);
      right_table = word;
    } else {
      expected_next_state.insert(join);
      left_table = word;
    }
    current_state = tbl_name;
    return true;
  }
  current_state = error;
  return false;
}

bool JoinStateMachine::check_join_state() {
  if (expected_next_state.contains(join)) {
    current_state = join;
    expected_next_state.clear();
    expected_next_state.insert(tbl_name);
    return true;
  }

  current_state = error;
  return false;
}

bool JoinStateMachine::check_on_state() {
  if (expected_next_state.contains(on)) {
    current_state = on;
    expected_next_state.clear();
    expected_next_state.insert(left_paren);
    return true;
  }

  current_state = error;
  return false;
}

bool JoinStateMachine::check_col_name_state(std::string word) {
  if (expected_next_state.contains(col_name)) {
    expected_next_state.clear();
    if (current_state == left_paren) {
      expected_next_state.insert(op);
      left_column = check_column_name(word, left_table);
    } else {
      expected_next_state.insert(right_paren);
      right_column = check_column_name(word, right_table);
    }
    current_state = col_name;
    return true;
  }
  current_state = error;
  return false;
}

bool JoinStateMachine::check_op_state(Token token, std::string word) {
  if (expected_next_state.contains(op)) {
    current_state = op;
    expected_next_state.clear();
    expected_next_state.insert(col_name);
    op_token = token;
    str_op = word;
    return true;
  }
  current_state = error;
  return false;
}

bool JoinStateMachine::check_left_paren_state() {
  if (expected_next_state.contains(left_paren)) {
    current_state = left_paren;
    expected_next_state.clear();
    expected_next_state.insert(col_name);
    return true;
  }

  current_state = error;
  return false;
}

bool JoinStateMachine::check_right_paren_state() {
  if (expected_next_state.contains(right_paren)) {
    current_state = right_paren;
    expected_next_state.clear();
    return true;
  }

  current_state = error;
  return false;
}

std::string JoinStateMachine::check_column_name(std::string col_name,
                                                std::string table_name) {
  std::string tname;
  std::string cname;

  if (auto pos = col_name.find("."); pos != std::string::npos) {
    tname = col_name.substr(0, pos);
    cname = col_name.substr(pos + 1);
  } else {
    throw std::runtime_error(
        "JSM: Unable to parse join clause. Table name missing.\n");
  }

  if (tname != table_name) {
    throw std::runtime_error(
        "JSM: Unable to parse join clause. Table name and "
        "doesn't match in the column qualifier.\n");
  }

  return cname;
}

}  // namespace sdb
