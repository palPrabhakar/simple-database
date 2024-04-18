#include "parser.h"

#include <cassert>
#include <format>
#include <iostream>
#include <memory>
#include <stdexcept>

#include "fsms/create_state_machine.h"
#include "fsms/expr_state_machine.h"
#include "fsms/insert_state_machine.h"
#include "fsms/join_state_machine.h"
#include "fsms/select_state_machine.h"
#include "fsms/update_state_machine.h"
#include "operators/create_operator.h"
#include "operators/insert_operator.h"
#include "operators/join_operator.h"
#include "operators/operator.h"
#include "operators/project_operator.h"
#include "operators/read_operator.h"
#include "operators/set_operator.h"
#include "operators/update_operator.h"
#include "operators/write_operator.h"
#include "tokenizer.h"

namespace sdb {

Operator_Ptr ParseWhereClause(Token_Vector &tokens, size_t &index) {
  auto operations = ParseLogicalOP(tokens, ++index);

  assert(index < tokens.size() && "ParseWhereClause: Index out of range");

  if (tokens[index].first != END) {
    std::runtime_error(
        "Failed to parse where clause. Expected end of line "
        "character. But found " +
        tokens[index].second);
  }

  return operations;
}

Operator_Vec ParseJoinClause(Token_Vector &tokens, size_t &index) {
  Operator_Vec operators;

  JoinStateMachine jsm;

  for (; index < tokens.size(); ++index) {
    if (!jsm.CheckTransition(tokens[index].first, tokens[index].second)) {
      if (jsm.CheckErrorState()) {
        throw std::runtime_error(jsm.GetErrorMsg() + " But found " +
                                 tokens[index].second);
      }
      break;
    }
  }

  if (!jsm.EOP()) {
    throw std::runtime_error("Failed to parse join clause\n");
  }

  // the order should be this
  // the join operator implicity assumes index - 0 for left table
  operators.push_back(std::make_unique<ReadOperator>(jsm.left_table));
  operators.push_back(std::make_unique<ReadOperator>(jsm.right_table));
  operators.push_back(std::make_unique<JoinOperator>(
      jsm.left_table, jsm.right_table, jsm.left_column, jsm.right_column));

  return operators;
}

BinaryOp_Ptr ParseExpression(Token_Vector &tokens, size_t &index) {
  ExprStateMachine esm;

  while (index < tokens.size()) {
    if (esm.CheckTransition(tokens[index].first, tokens[index].second)) {
      index++;
    } else {
      if (esm.CheckErrorState()) {
        throw std::runtime_error(esm.GetErrorMsg() + " But found " +
                                 tokens[index].second);
      }
      break;
    }
  }

  if (!esm.EOP()) {
    throw std::runtime_error("Failed to parse expression\n");
  }

  return esm.GetOperator();
}

BinaryOp_Ptr ParseLogicalOP(Token_Vector &tokens, size_t &index) {
  BinaryOp_Ptr output;
  BinaryOp_Ptr left;
  BinaryOp_Ptr right;
  std::string str_op;
  Token op_token;

  assert(index + 1 < tokens.size() && "ParseLogicalOP: Index out of range");

  if (tokens[index + 1].first == TEXT) {
    // parse expression returns one past right paren
    // does both left and right parsing
    // leaf element found
    return ParseExpression(tokens, index);
  }

  if (tokens[index + 1].first == LEFT_PAREN) {
    left = ParseLogicalOP(tokens, ++index);
  } else {
    throw std::runtime_error(
        "Failed to parse where clause. Expected left "
        "parenthesis. But found " +
        tokens[index + 1].second);
  }

  assert(index < tokens.size() && "ParseLogicalOP: Index out of range");

  if (tokens[index].first == AND || tokens[index].first == OR) {
    op_token = tokens[index].first;
    str_op = tokens[index].second;
  } else {
    throw std::runtime_error(
        "Failed to parse where clause. Expected AND or OR "
        "operator. But found " +
        tokens[index].second);
  }

  assert(index + 1 < tokens.size() && "ParseLogicalOP: Index out of range");

  if (tokens[index + 1].first == LEFT_PAREN) {
    right = ParseLogicalOP(tokens, ++index);
  } else {
    throw std::runtime_error(
        "Failed to parse where clause. Expected left "
        "parenthesis. But found " +
        tokens[index + 1].second);
  }

  assert(index < tokens.size() && "ParseLogicalOP: Index out of range");

  if (tokens[index].first != RIGHT_PAREN) {
    throw std::runtime_error(
        "Failed to parse where clause. Expected right parenthesis. But found " +
        tokens[index].second);
  }
  ++index;

  switch (op_token) {
    case AND:
      output = std::make_unique<IntersectionOperator>(std::move(left),
                                                      std::move(right));
      break;
    case OR:
      output =
          std::make_unique<UnionOperator>(std::move(left), std::move(right));
      break;
    default:
      break;
  }

  return output;
}

Operator_Vec ParseUpdateQuery(Token_Vector &tokens, size_t &index) {
  Operator_Vec operators;

  UpdateStateMachine usm;
  usm.RegisterCallBack([&tokens, &index]() {
    auto operations = ParseWhereClause(tokens, index);
    --index;
    return operations;
  });

  assert(index < tokens.size() && "ParseUpdateQuery: Index out of range\n");

  for (; index < tokens.size(); ++index) {
    if (!usm.CheckTransition(tokens[index].first, tokens[index].second)) {
      if (usm.CheckErrorState()) {
        throw std::runtime_error(usm.GetErrorMsg() + " But found " +
                                 tokens[index].second);
      }
      break;
    }
  }

  if (!usm.EOP()) {
    throw std::runtime_error("Failed to parse update query\n");
  }

  operators.push_back(std::make_unique<ReadOperator>(usm.table_name));

  // TODO:
  // Update Op
  BinaryOp_Ptr op(static_cast<BinaryOperator *>(usm.where_op.release()));
  operators.push_back(std::make_unique<UpdateOperator>(
      usm.col_names, usm.col_values, std::move(op)));

  operators.push_back(std::make_unique<FileWriter>());

  return operators;
}

Operator_Vec ParseSelectQuery(Token_Vector &tokens, size_t &index) {
  Operator_Vec operators;

  SelectStateMachine ssm;
  ssm.RegisterWhereCallBack([&tokens, &index]() {
    auto op = ParseWhereClause(tokens, index);
    --index;
    return op;
  });

  ssm.RegisterJoinCallBack([&tokens, &index]() {
    --index;
    auto operators = ParseJoinClause(tokens, index);
    --index;
    return operators;
  });

  assert(index < tokens.size() && "ParseSelectQuery: Index out of range\n");

  for (; index < tokens.size(); ++index) {
    if (!ssm.CheckTransition(tokens[index].first, tokens[index].second)) {
      if (ssm.CheckErrorState()) {
        throw std::runtime_error(ssm.GetErrorMsg() + " But found " +
                                 tokens[index].second);
      }
      break;
    }
  }

  if (!ssm.EOP()) {
    throw std::runtime_error("Failed to parse select query\n");
  }

  if (!ssm.join_clause) {
    operators.push_back(std::make_unique<ReadOperator>(ssm.table_name));
  } else {
    operators = std::move(ssm.join_ops);
  }

  // operators.emplace_back(std::make_unique<StdOutWriter>());

  // If where clause add Binary Operator
  if (ssm.where_op) {
    operators.push_back(std::move(ssm.where_op));
  }

  // Add Project Operator
  operators.push_back(std::make_unique<ProjectOperator>(ssm.col_names));

  // Add Write Operator
  operators.push_back(std::make_unique<StdOutWriter>());

  return operators;
}

Operator_Vec ParseCreateQuery(Token_Vector &tokens, size_t &index) {
  Operator_Vec operators;

  CreateStateMachine csm;

  for (auto [token, word] : tokens) {
    if (!csm.CheckTransition(token, word)) {
      throw std::runtime_error(csm.GetErrorMsg() + " But found " + word);
    }
  }

  if (!csm.EOP()) {
    throw std::runtime_error("Failed to parse create query\n");
  }

  operators.push_back(std::make_unique<CreateOperator>(
      csm.table_name, csm.col_names, csm.col_types));

  operators.push_back(std::make_unique<FileWriter>());

  return operators;
}

Operator_Vec ParseInsertQuery(Token_Vector &tokens, size_t &index) {
  Operator_Vec operators;

  InsertStateMachine ism;

  for (auto [token, word] : tokens) {
    if (!ism.CheckTransition(token, word)) {
      throw std::runtime_error(ism.GetErrorMsg() + " But found " + word);
    }
  }

  if (!ism.EOP()) {
    throw std::runtime_error("Failed to parse insert query\n");
  }

  operators.push_back(std::make_unique<ReadOperator>(ism.table_name));
  operators.push_back(std::make_unique<InsertOperator>(ism.col_values));
  operators.push_back(std::make_unique<FileWriter>());

  return operators;
}

Operator_Vec ParseInputQuery(std::string input_query) {
  auto tokens = ReadInputQuery(input_query);

  // for (auto [token, word] : tokens) {
  //   std::cout << "word: " << word << ", token: " << token << std::endl;
  // }

  Operator_Vec operators;
  std::size_t index = 0;

  if (tokens.size() != 0) {
    switch (tokens[0].first) {
      case CREATE:
        operators = ParseCreateQuery(tokens, index);
        break;
      case INSERT:
        operators = ParseInsertQuery(tokens, index);
        break;
      case UPDATE:
        operators = ParseUpdateQuery(tokens, index);
        break;
      case SELECT:
        return ParseSelectQuery(tokens, index);
      default:
        throw std::runtime_error("invalid query");
    }
  }

  return operators;
}
}  // namespace sdb
