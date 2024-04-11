#include "fsms/insert_state_machine.h"

namespace tdb {
bool InsertStateMachine::CheckTransition(Token token, std::string word) {
  switch (token) {
    case INSERT:
      return check_insert_state();
    case TEXT:
      if (current_state == insert) {
        return check_tbl_name_state(word);
      } else {
        return check_col_val_state(word);
      }
    case VALUES:
      return check_values_state();
    case END:
      return check_end_state();
    default:
      current_state = undefined;
      return false;
  }
}

std::string InsertStateMachine::GetErrorMsg() {
  if (current_state == error || current_state == undefined) {
    err_msg = "Failed to parse create query.";
    for (auto val : expected_next_state) {
      switch (val) {
        case insert:
          err_msg += " Expected keyword create.";
          break;
        case tbl_name:
          err_msg += " Expected table name.";
          break;
        case values:
          err_msg += " Expected keyword values.";
          break;
        case col_value:
          err_msg += " Expected column values.";
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

bool InsertStateMachine::check_insert_state() {
  if (expected_next_state.size() == 1 && expected_next_state.contains(insert)) {
    expected_next_state.clear();
    expected_next_state.insert(tbl_name);
    current_state = insert;
    return true;
  }

  current_state = error;
  return false;
}

bool InsertStateMachine::check_tbl_name_state(std::string word) {
  if (expected_next_state.size() == 1 &&
      expected_next_state.contains(tbl_name)) {
    expected_next_state.clear();
    expected_next_state.insert(values);
    current_state = tbl_name;
    table_name = word;
    return true;
  }

  current_state = error;
  return false;
}

bool InsertStateMachine::check_values_state() {
  if (expected_next_state.size() == 1 && expected_next_state.contains(values)) {
    expected_next_state.clear();
    expected_next_state.insert(col_value);
    current_state = values;
    return true;
  }

  current_state = error;
  return false;
}

bool InsertStateMachine::check_col_val_state(std::string word) {
  if ((expected_next_state.size() == 1 || expected_next_state.size() == 2) &&
      expected_next_state.contains(col_value)) {
    expected_next_state.clear();
    expected_next_state.insert(end);
    expected_next_state.insert(col_value);
    current_state = col_value;
    col_values.push_back(word);
    return true;
  }

  current_state = error;
  return false;
}

bool InsertStateMachine::check_end_state() {
  if (current_state == col_value && expected_next_state.contains(end)) {
    current_state = end;
    expected_next_state.clear();
    return true;
  }

  current_state = error;
  return false;
}

}  // namespace tdb
