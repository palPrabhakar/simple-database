#include "fsms/update_state_machine.h"

namespace tdb {
bool UpdateStateMachine::CheckTransition(Token token, std::string word) {
  switch (token) {
    case UPDATE:
      return check_update_state();
    case TEXT:
      if (current_state == update) {
        return check_tbl_name_state(word);
      } else {
        return check_col_name_state(word) || check_col_val_state(word);
      }
    case VALUES:
      return check_values_state();
    case WHERE:
      return check_where_state();
    case END:
      return check_end_state();
    default:
      current_state = undefined;
      return false;
  }
}

std::string UpdateStateMachine::GetErrorMsg() {
  if (current_state == error || current_state == undefined) {
    err_msg = "Failed to parse update query.";
    for (auto val : expected_next_state) {
      switch (val) {
        case update:
          err_msg += " Expected keyword update.";
          break;
        case tbl_name:
          err_msg += " Expected table name.";
          break;
        case values:
          err_msg += " Expected keyword values.";
          break;
        case column_name:
          err_msg += " Expected column name.";
          break;
        case where:
          err_msg += " Expected keyword where.";
          break;
        case column_value:
          err_msg += " Expected column value.";
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

bool UpdateStateMachine::check_update_state() {
  if (expected_next_state.contains(update)) {
    expected_next_state.clear();
    expected_next_state.insert(tbl_name);
    current_state = update;
    return true;
  }

  current_state = error;
  return false;
}

bool UpdateStateMachine::check_tbl_name_state(std::string word) {
  if (expected_next_state.contains(tbl_name)) {
    expected_next_state.clear();
    expected_next_state.insert(values);
    current_state = tbl_name;
    table_name = word;
    return true;
  }

  current_state = error;
  return false;
}

bool UpdateStateMachine::check_values_state() {
  if (expected_next_state.contains(values)) {
    expected_next_state.clear();
    expected_next_state.insert(column_name);
    current_state = values;
    return true;
  }

  current_state = error;
  return false;
}

bool UpdateStateMachine::check_col_name_state(std::string word) {
  if (expected_next_state.contains(column_name)) {
    expected_next_state.clear();
    expected_next_state.insert(column_value);
    current_state = column_value;
    col_names.push_back(word);
    return true;
  }

  current_state = error;
  return false;
}

bool UpdateStateMachine::check_col_val_state(std::string word) {
  if (expected_next_state.contains(column_value)) {
    expected_next_state.clear();
    expected_next_state.insert(end);
    expected_next_state.insert(column_name);
    expected_next_state.insert(where);
    current_state = column_value;
    col_values.push_back(word);
    return true;
  }

  current_state = error;
  return false;
}

bool UpdateStateMachine::check_where_state() {
  if (expected_next_state.contains(where)) {
    current_state = where;
    expected_next_state.clear();
    expected_next_state.insert(end);
    where_op = callback_func();
    return true;
  }

  current_state = error;
  return false;
}

bool UpdateStateMachine::check_end_state() {
  if (expected_next_state.contains(end)) {
    current_state = end;
    expected_next_state.clear();
    return true;
  }

  current_state = error;
  return false;
}

}  // namespace tdb
