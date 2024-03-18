#include "operators/filter_operator.h"
#include "operators/project_operator.h"
#include "operators/read_operator.h"
#include "operators/set_operator.h"
#include "operators/write_operator.h"
#include "parser.h"
#include "gtest/gtest.h"
#include <format>

TEST(OperatorTest, ReadOperator) {
  tdb::ReadOperator read_op("/home/pal/workspace/terrible-softwares/"
                            "terrible-database/tables/simple_table.json");
  read_op.Execute();

  auto un_op =
      tdb::UnionOperator(std::make_unique<tdb::GreaterThanFilter>("age", "40"),
                         std::make_unique<tdb::EqualityFilter>("name", "John"));
  un_op.AddData(read_op.GetData());
  un_op.Execute();

  // tdb::EqualityFilter eq_op("age", "44");
  // eq_op.AddData(read_op.GetData());
  // eq_op.Execute();

  tdb::ProjectOperator proj_op({"age"});
  proj_op.AddData(un_op.GetData());
  proj_op.Execute();

  auto write_op = tdb::StdOutWriter();
  write_op.AddData(proj_op.GetData());
  write_op.Execute();
  // EXPECT_NO_THROW(op.ReadTable());
}

TEST(OperatorTest, TestPipeline) {
  {
    auto operators = tdb::ParseInputQuery("Select * from simple_table");

    EXPECT_TRUE(operators.size() == 3);
    tdb::Table_Vec vec;
    for (auto &op : operators) {
      op->AddData(std::move(vec));
      op->Execute();
      vec = std::move(op->GetData());
    }
  }

  {
    auto operators = tdb::ParseInputQuery("Select * from simple_table Where ( "
                                          "( name == john ) OR ( age > 40 ) )");

    EXPECT_TRUE(operators.size() == 4);
    tdb::Table_Vec vec;
    for (auto &op : operators) {
      op->AddData(std::move(vec));
      op->Execute();
      vec = std::move(op->GetData());
    }
  }
}

TEST(OperatorTest, TestCreatePipeline) {
  {
    auto operators = tdb::ParseInputQuery("create test_table with col1 int col2 double col3 string");

    EXPECT_TRUE(operators.size() == 2);
    tdb::Table_Vec vec;
    for (auto &op : operators) {
      op->AddData(std::move(vec));
      op->Execute();
      vec = std::move(op->GetData());
    }
  }
}
