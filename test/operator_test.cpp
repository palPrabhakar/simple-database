#include <format>

#include "gtest/gtest.h"
#include "operators/filter_operator.h"
#include "operators/project_operator.h"
#include "operators/read_operator.h"
#include "operators/set_operator.h"
#include "operators/write_operator.h"
#include "parser.h"


TEST(OperatorTest, ReadOperator) {
  GTEST_SKIP();
  sdb::ReadOperator read_op("large_table");
  read_op.Execute();
  auto write_op = sdb::StdOutWriter();
  write_op.AddData(read_op.GetData());
  write_op.Execute();
}

TEST(OperatorTest, TestPipeline) {
  {
    auto operators = sdb::ParseInputQuery("Select * from simple_table");

    EXPECT_TRUE(operators.size() == 3);
    sdb::Table_Vec vec;
    for (auto &op : operators) {
      op->AddData(std::move(vec));
      op->Execute();
      vec = op->GetData();
    }
  }

  {
    auto operators = sdb::ParseInputQuery(
        "Select * from simple_table Where ( "
        "( name == john ) OR ( age > 40 ) )");

    EXPECT_TRUE(operators.size() == 4);
    sdb::Table_Vec vec;
    for (auto &op : operators) {
      op->AddData(std::move(vec));
      op->Execute();
      vec = op->GetData();
    }
  }
}

TEST(OperatorTest, TestCreatePipeline) {
  {
    auto operators = sdb::ParseInputQuery(
        "create test_table with col1 int col2 double col3 string");

    EXPECT_TRUE(operators.size() == 2);
    sdb::Table_Vec vec;
    for (auto &op : operators) {
      op->AddData(std::move(vec));
      op->Execute();
      vec = op->GetData();
    }
  }
}

TEST(OperatorTest, TestInsertPipeline) {
  {
    auto operators =
        sdb::ParseInputQuery("insert test_table values 42 2.5 lol");

    EXPECT_TRUE(operators.size() == 3);
    sdb::Table_Vec vec;
    for (auto &op : operators) {
      op->AddData(std::move(vec));
      op->Execute();
      vec = op->GetData();
    }
  }
}

TEST(OperatorTest, TestJoinPipeline) {
  {
    auto operators = sdb::ParseInputQuery(
        "select * from jl_table join jr_table on ( "
        "jl_table.gid == jr_table.id )");

    EXPECT_TRUE(operators.size() == 5);
    sdb::Table_Vec vec;
    for (auto &op : operators) {
      op->AddData(std::move(vec));
      op->Execute();
      vec = op->GetData();
    }
  }

  {
    auto operators = sdb::ParseInputQuery(
        "select * from jl_table join jr_table on ( "
        "jl_table.gid == jr_table.id ) where ( ( jl_table.age > 40 ) and ( "
        "jr_table.id == 1 ) )");

    EXPECT_TRUE(operators.size() == 6);
    sdb::Table_Vec vec;
    for (auto &op : operators) {
      op->AddData(std::move(vec));
      op->Execute();
      vec = op->GetData();
    }
  }
}

TEST(OperatorTest, TestUpdatePipeline) {
  {
    auto operators =
        sdb::ParseInputQuery("Update u1_table values name lol age 11");

    EXPECT_TRUE(operators.size() == 3);
    sdb::Table_Vec vec;
    for (auto &op : operators) {
      op->AddData(std::move(vec));
      op->Execute();
      vec = op->GetData();
    }
  }

  {
    auto operators = sdb::ParseInputQuery(
        "Update u2_table values name lol age 11 where ( age > 40 )");

    EXPECT_TRUE(operators.size() == 3);
    sdb::Table_Vec vec;
    for (auto &op : operators) {
      op->AddData(std::move(vec));
      op->Execute();
      vec = op->GetData();
    }
  }
}
