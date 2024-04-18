#include "fsms/create_state_machine.h"

namespace sdb {

bool CreateStateMachine::CheckTransition(Token token, std::string word) {
  switch (token) {
    case CREATE:
      return check_create_state();
    case TEXT:
      if (current_state == create) {
        return check_tbl_name_state(word);
      } else {
        return check_col_name_state(word);
      }
    case WITH:
      return check_with_state();
    case TYPE_INT:
      return check_col_type_state(DT_INT);
    case TYPE_DOUBLE:
      return check_col_type_state(DT_DOUBLE);
    case TYPE_STRING:
      return check_col_type_state(DT_STRING);
    case END:
      return check_end_state();
    default:
      current_state = undefined;
      return false;
  }
}

std::string CreateStateMachine::GetErrorMsg() {
  if (current_state == error || current_state == undefined) {
    err_msg = "Failed to parse create query.";
    for (auto val : expected_next_state) {
      switch (val) {
        case create:
          err_msg += " Expected keyword create.";
          break;
        case tbl_name:
          err_msg += " Expected table name.";
          break;
        case with:
          err_msg += " Expected keyword with.";
          break;
        case col_name:
          err_msg += " Expected column name.";
          break;
        case col_type:
          err_msg += " Expected column type.";
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

bool CreateStateMachine::check_create_state() {
  if (expected_next_state.size() == 1 && expected_next_state.contains(create)) {
    expected_next_state.clear();
    expected_next_state.insert(tbl_name);
    current_state = create;
    return true;
  }

  current_state = error;
  return false;
}

bool CreateStateMachine::check_tbl_name_state(std::string word) {
  if (expected_next_state.size() == 1 &&
      expected_next_state.contains(tbl_name)) {
    expected_next_state.clear();
    expected_next_state.insert(with);
    current_state = tbl_name;
    table_name = word;
    return true;
  }

  current_state = error;
  return false;
}

bool CreateStateMachine::check_col_name_state(std::string word) {
  if ((expected_next_state.size() == 1 || expected_next_state.size() == 2) &&
      expected_next_state.contains(col_name)) {
    expected_next_state.clear();
    expected_next_state.insert(col_type);
    current_state = col_name;
    col_names.push_back(word);
    return true;
  }

  current_state = error;
  return false;
}

bool CreateStateMachine::check_col_type_state(Data_Type type) {
  if (expected_next_state.size() == 1 &&
      expected_next_state.contains(col_type)) {
    expected_next_state.clear();
    expected_next_state.insert(col_name);
    expected_next_state.insert(end);
    current_state = col_type;
    col_types.push_back(type);
    return true;
  }

  current_state = error;
  return false;
}

bool CreateStateMachine::check_with_state() {
  if (expected_next_state.size() == 1 && expected_next_state.contains(with)) {
    expected_next_state.clear();
    expected_next_state.insert(col_name);
    current_state = with;
    return true;
  }

  current_state = error;
  return false;
}

bool CreateStateMachine::check_end_state() {
  if (current_state == col_type && expected_next_state.contains(end)) {
    current_state = end;
    expected_next_state.clear();
    return true;
  }

  current_state = error;
  return false;
}

}  // namespace tdb
