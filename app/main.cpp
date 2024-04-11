#include "operators/operator.h"
#include "operators/write_operator.h"
#include "parser.h"
#include "table.h"

int main() {
  // auto operators = tdb::ParseInputQuery("select * from jl_table join jr_table
  // on ( jl_table.gid == jr_table.id )");

  // assert(operators.size() == 5);

  // tdb::Table_Vec vec;
  // for (auto &op : operators) {
  //   op->AddData(std::move(vec));
  //   op->Execute();
  //   vec = op->GetData();
  // }

  // auto operators = tdb::ParseInputQuery("Update u2_table values name lol age
  // 11 where ( age > 40 )");

  // tdb::Table_Vec vec;
  // for (auto &op : operators) {
  //   op->AddData(std::move(vec));
  //   op->Execute();
  //   vec = op->GetData();
  // }

  auto operators =
      tdb::ParseInputQuery("select * from jl_table join jr_table on ( "
                           "jl_table.gid == jr_table.id ) where ( ( "
                           "jl_table.age > 40 ) and ( jr_table.id == 1 ) )");

  tdb::Table_Vec vec;
  for (auto &op : operators) {
    op->AddData(std::move(vec));
    op->Execute();
    vec = op->GetData();
  }

  return 0;
}
