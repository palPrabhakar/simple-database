#include "fsms/select_state_machine.h"

#include <cmath>

namespace sdb {
bool SelectStateMachine::CheckTransition(Token token, std::string word) {
  switch (token) {
    case SELECT:
      return check_select_state();
    case FROM:
      return check_from_state();
    case STAR:
      return check_star_state();
    case TEXT:
      if (current_state == from) {
        return check_tbl_name_state(word);
      } else {
        return check_col_name_state(word);
      }
    case WHERE:
      return check_where_state();
    case JOIN:
      return check_join_state();
    case END:
      return check_end_state();
    default:
      current_state = undefined;
      return false;
  }
}

std::string SelectStateMachine::GetErrorMsg() {
  if (current_state == error || current_state == undefined) {
    err_msg = "Failed to parse select query.";
    for (auto val : expected_next_state) {
      switch (val) {
        case select:
          err_msg += " Expected keyword select.";
          break;
        case tbl_name:
          err_msg += " Expected table name.";
          break;
        case star:
          err_msg += " Expected keyword star.";
          break;
        case column_name:
          err_msg += " Expected column name.";
          break;
        case where:
          err_msg += " Expected keyword where.";
          break;
        case from:
          err_msg += " Expected keyword from.";
          break;
        case join:
          err_msg += " Expected keyword join.";
          break;
        case end:
          err_msg += " Expected endline character.";
        default:
          break;
      }
    }
  }

  return err_msg;
}

bool SelectStateMachine::check_select_state() {
  if (expected_next_state.contains(select)) {
    expected_next_state.clear();
    expected_next_state.insert(star);
    expected_next_state.insert(column_name);
    current_state = select;
    return true;
  }

  current_state = error;
  return false;
}

bool SelectStateMachine::check_star_state() {
  if (expected_next_state.contains(star)) {
    expected_next_state.clear();
    expected_next_state.insert(from);
    current_state = star;
    return true;
  }

  current_state = error;
  return false;
}

bool SelectStateMachine::check_from_state() {
  if (expected_next_state.contains(from)) {
    expected_next_state.clear();
    expected_next_state.insert(tbl_name);
    current_state = from;
    return true;
  }

  current_state = error;
  return false;
}

bool SelectStateMachine::check_tbl_name_state(std::string word) {
  if (expected_next_state.contains(tbl_name)) {
    expected_next_state.clear();
    expected_next_state.insert(where);
    expected_next_state.insert(end);
    expected_next_state.insert(join);
    current_state = tbl_name;
    table_name = word;
    return true;
  }

  current_state = error;
  return false;
}

bool SelectStateMachine::check_col_name_state(std::string word) {
  if (expected_next_state.contains(column_name)) {
    expected_next_state.clear();
    expected_next_state.insert(column_name);
    expected_next_state.insert(from);
    current_state = column_name;
    col_names.push_back(word);
    return true;
  }

  current_state = error;
  return false;
}

bool SelectStateMachine::check_where_state() {
  if (expected_next_state.contains(where)) {
    current_state = where;
    expected_next_state.clear();
    expected_next_state.insert(end);
    where_op = where_cb_func();
    return true;
  }

  current_state = error;
  return false;
}

bool SelectStateMachine::check_join_state() {
  if (expected_next_state.contains(join)) {
    current_state = join;
    expected_next_state.clear();
    expected_next_state.insert(where);
    expected_next_state.insert(end);
    join_ops = join_cb_func();
    join_clause = true;
    return true;
  }

  current_state = error;
  return false;
}

bool SelectStateMachine::check_end_state() {
  if (expected_next_state.contains(end)) {
    current_state = end;
    expected_next_state.clear();
    return true;
  }

  current_state = error;
  return false;
}

}  // namespace sdb
