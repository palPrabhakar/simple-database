#pragma once

#include <memory>
#include <string>
#include <vector>

#include "operators/operator.h"
#include "tokenizer.h"

namespace sdb {

using Token_Vector = std::vector<std::pair<Token, std::string>>;
using Operator_Ptr = std::unique_ptr<Operator>;
using BinaryOp_Ptr = std::unique_ptr<BinaryOperator>;
using Operator_Vec = std::vector<Operator_Ptr>;

Operator_Ptr ParseWhereClause(Token_Vector &tokens, size_t &index);
BinaryOp_Ptr ParseExpression(Token_Vector &tokens, size_t &index);
BinaryOp_Ptr ParseLogicalOP(Token_Vector &tokens, size_t &index);

Operator_Vec ParseInputQuery(std::string input_query);
Operator_Vec ParseCreateQuery(Token_Vector &tokens, size_t &index);
Operator_Vec ParseInsertQuery(Token_Vector &tokens, size_t &index);
Operator_Vec ParseUpdateQuery(Token_Vector &tokens, size_t &index);
Operator_Vec ParseSelectQuery(Token_Vector &tokens, size_t &index);
Operator_Vec ParseJoinClause(Token_Vector &tokens, size_t &index);

}  // namespace sdb
