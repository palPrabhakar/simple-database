#include "fsms/expr_state_machine.h"

#include <math.h>

#include <cmath>
#include <memory>
#include <stdexcept>

#include "operators/filter_operator.h"
#include "tokenizer.h"

namespace tdb {
bool ExprStateMachine::CheckTransition(Token token, std::string word) {
  // if the current state is righ_paren
  // then Expr has been parsed or
  if (current_state == right_paren) return false;

  switch (token) {
    case LEFT_PAREN:
      return check_left_paren_state();
    case RIGHT_PAREN:
      return check_right_paren_state();
    case TEXT:
      if (current_state == left_paren) {
        return check_col_name_state(word);
      } else {
        return check_col_val_state(word);
      }
    case OP_EQ:
    case OP_NEQ:
    case OP_LT:
    case OP_GT:
    case OP_EQLT:
    case OP_EQGT:
      return check_op_state(token, word);
    default:
      current_state = undefined;
      return false;
  }
}

std::string ExprStateMachine::GetErrorMsg() {
  if (current_state == error || current_state == undefined) {
    err_msg = "Failed to parse expression.";
    for (auto val : expected_next_state) {
      switch (val) {
        case col_name:
          err_msg += " Expected column name.";
          break;
        case op:
          err_msg += " Expected a binary operator.";
          break;
        case col_value:
          err_msg += " Expected column value.";
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

bool ExprStateMachine::check_col_name_state(std::string word) {
  if (expected_next_state.size() == 1 &&
      expected_next_state.contains(col_name)) {
    current_state = col_name;
    expected_next_state.clear();
    expected_next_state.insert(op);
    column_name = word;
    return true;
  }
  current_state = error;
  return false;
}

bool ExprStateMachine::check_op_state(Token token, std::string word) {
  if (expected_next_state.size() == 1 && expected_next_state.contains(op)) {
    current_state = op;
    expected_next_state.clear();
    expected_next_state.insert(col_value);
    op_token = token;
    str_op = word;
    return true;
  }
  current_state = error;
  return false;
}

bool ExprStateMachine::check_col_val_state(std::string word) {
  if (expected_next_state.size() == 1 &&
      expected_next_state.contains(col_value)) {
    current_state = col_value;
    expected_next_state.clear();
    expected_next_state.insert(right_paren);
    column_val = word;
    return true;
  }
  current_state = error;
  return false;
}

bool ExprStateMachine::check_left_paren_state() {
  if (expected_next_state.contains(left_paren)) {
    current_state = left_paren;
    expected_next_state.clear();
    expected_next_state.insert(col_name);
    return true;
  }
  current_state = error;
  return false;
}

bool ExprStateMachine::check_right_paren_state() {
  if (expected_next_state.contains(right_paren)) {
    current_state = right_paren;
    expected_next_state.clear();
    return true;
  }
  current_state = error;
  return false;
}

BinaryOp_Ptr ExprStateMachine::GetOperator() {
  switch (op_token) {
    case OP_EQ:
      return std::make_unique<EqualityFilter>(column_name, column_val);
    case OP_NEQ:
      return std::make_unique<NonEqualityFilter>(column_name, column_val);
    case OP_GT:
      return std::make_unique<GreaterThanFilter>(column_name, column_val);
    case OP_LT:
      return std::make_unique<LessThanFilter>(column_name, column_val);
      break;
    case OP_EQGT:
      return std::make_unique<GreaterEqualFilter>(column_name, column_val);
      break;
    case OP_EQLT:
      return std::make_unique<LessEqualFilter>(column_name, column_val);
      break;
    default:
      throw std::runtime_error("ESM: Invalid Operator\n");
  }
}
}  // namespace tdb
