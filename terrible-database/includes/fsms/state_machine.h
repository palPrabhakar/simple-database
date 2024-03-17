#pragma once

#include "parser.h"
#include "tokenizer.h"
#include <operators/operator.h>

namespace tdb {

using Operator_Ptr = std::unique_ptr<Operator>;
using BinaryOp_Ptr = std::unique_ptr<BinaryOperator>;
using Operator_Vec = std::vector<std::unique_ptr<Operator>>;

class StateMachine {
public:
  virtual bool CheckTransition(Token token, std::string word) = 0;
  virtual std::string GetErrorMsg() = 0;
  virtual bool EOP() = 0;
  virtual bool CheckErrorState() = 0;
};
} // namespace tdb
