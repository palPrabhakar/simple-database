#include <parser.h>

#include <exception>
#include <iostream>
#include <stdexcept>

#include "gtest/gtest.h"
#include "tokenizer.h"

TEST(ParserTest, CreateQueriesSuccess) {
  EXPECT_NO_THROW(sdb::ParseInputQuery(
      "Create random_table with col1 int col2 double col3 string"));
}

TEST(ParserTest, CreateQueriesFailure0) {
  EXPECT_THROW(
      {
        try {
          sdb::ParseInputQuery("Create some_table with col1 int.");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected column type.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, CreateQueriesFailure1) {
  EXPECT_THROW(
      {
        try {
          sdb::ParseInputQuery("Create random_table with col1 int col2 far");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected column type.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, CreateQueriesFailure2) {
  EXPECT_THROW(
      {
        try {
          sdb::ParseInputQuery("Create random_table with col1 int double");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected column name.") !=
                      std::string::npos);
          EXPECT_TRUE(
              std::string(err.what()).find("Expected endline character.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, CreateQueriesFailure3) {
  EXPECT_THROW(
      {
        try {
          sdb::ParseInputQuery("Create with col1 int col2 double");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected table name.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, CreateQueriesFailure4) {
  EXPECT_THROW(
      {
        try {
          sdb::ParseInputQuery("Create random_table col1 int col2 double");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected keyword with.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, InsertQueriesSuccess0) {
  EXPECT_NO_THROW(
      sdb::ParseInputQuery("Insert random_table values 100 250 lol"));
}

TEST(ParserTest, InsertQueriesSuccess1) {
  EXPECT_NO_THROW(
      sdb::ParseInputQuery("Insert some_table values 250.00 very-lol\n"));
}

TEST(ParserTest, InsertQueriesSuccess2) {
  EXPECT_NO_THROW(
      sdb::ParseInputQuery("Insert some_table values lol1 lol...lol"));
}

TEST(ParserTest, InsertQueriesSuccess3) {
  EXPECT_THROW(
      {
        try {
          sdb::ParseInputQuery("Insert values col1 int col2 far");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected table name.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, InsertQueriesFailure0) {
  EXPECT_THROW(
      {
        try {
          sdb::ParseInputQuery("Insert random_table values col1 int");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected column values.") !=
                      std::string::npos);
          EXPECT_TRUE(
              std::string(err.what()).find("Expected endline character.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, InsertQueriesFailure1) {
  EXPECT_THROW(
      {
        try {
          sdb::ParseInputQuery("Insert random_table with col1 int col2 double");
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected keyword values.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, ExprParsingSuccess0) {
  auto tokens = sdb::ReadInputQuery("( col1 == 100 )");
  size_t index = 0;
  EXPECT_NO_THROW(sdb::ParseExpression(tokens, index));
}

TEST(ParserTest, ExprParsingSuccess1) {
  auto tokens = sdb::ReadInputQuery("( col1 > 100 )");
  size_t index = 0;
  EXPECT_NO_THROW(sdb::ParseExpression(tokens, index));
}

TEST(ParserTest, ExprParsingFailure0) {
  auto tokens = sdb::ReadInputQuery(" col1 > 100 )");
  size_t index = 0;
  EXPECT_THROW(
      {
        try {
          sdb::ParseExpression(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected left parenthesis.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, ExprParsingFailure1) {
  auto tokens = sdb::ReadInputQuery(" (  > 100 )");
  size_t index = 0;
  EXPECT_THROW(
      {
        try {
          sdb::ParseExpression(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected column name.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, ExprParsingFailure2) {
  auto tokens = sdb::ReadInputQuery(" ( col1 100 )");
  size_t index = 0;
  EXPECT_THROW(
      {
        try {
          sdb::ParseExpression(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected a binary operator.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, ExprParsingFailure3) {
  auto tokens = sdb::ReadInputQuery(" ( col1 <  )");
  size_t index = 0;
  EXPECT_THROW(
      {
        try {
          sdb::ParseExpression(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected column value.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, ExprParsingFailure4) {
  auto tokens = sdb::ReadInputQuery(" ( col1 < 100");
  size_t index = 0;
  EXPECT_THROW(
      {
        try {
          sdb::ParseExpression(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected right parenthesis.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, WhereClauseSuccess0) {
  auto tokens = sdb::ReadInputQuery("where ( col1 == 100 )");
  size_t index = 0;
  EXPECT_NO_THROW(sdb::ParseWhereClause(tokens, index));
}

TEST(ParserTest, WhereClauseSuccess1) {
  auto tokens =
      sdb::ReadInputQuery("where ( ( col1 == 100 ) and ( col2 == 50 ) )");
  size_t index = 0;
  EXPECT_NO_THROW(sdb::ParseWhereClause(tokens, index));
}

TEST(ParserTest, WhereClauseSuccess2) {
  auto tokens = sdb::ReadInputQuery(
      "where ( ( ( col1 == 100 ) and ( col2 == lol ) ) or ( col3 != 50 ) )");
  size_t index = 0;
  EXPECT_NO_THROW(sdb::ParseWhereClause(tokens, index));
}

TEST(ParserTest, WhereClauseSuccess3) {
  auto tokens = sdb::ReadInputQuery(
      "where ( ( ( col1 == 100 ) and ( col2 == lol ) ) or "
      "( ( col3 != 50 ) and ( col4 > 11 ) ) )");
  size_t index = 0;
  EXPECT_NO_THROW(sdb::ParseWhereClause(tokens, index));
}

TEST(ParserTest, WhereClauseFailure4) {
  auto tokens = sdb::ReadInputQuery("where ( ( ( col1 == 100 ) ) )");
  size_t index = 0;
  EXPECT_THROW(sdb::ParseWhereClause(tokens, index), std::runtime_error);
}

TEST(ParserTest, WhereClauseFailure5) {
  EXPECT_THROW(
      {
        try {
          auto tokens = sdb::ReadInputQuery("where ( ( col1 == 100 ) and )");
          size_t index = 0;
          auto op = sdb::ParseWhereClause(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected left parenthesis.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, WhereClauseFailure6) {
  EXPECT_THROW(
      {
        try {
          auto tokens =
              sdb::ReadInputQuery("where ( ( col1 == 100 ) and ( ) )");
          size_t index = 0;
          auto op = sdb::ParseWhereClause(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected left parenthesis.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, WhereClauseFailure7) {
  EXPECT_THROW(
      {
        try {
          auto tokens =
              sdb::ReadInputQuery("where ( ( col1 == 100 ) and ( col2 ) )");
          size_t index = 0;
          auto op = sdb::ParseWhereClause(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected a binary operator.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, WhereClauseFailure8) {
  EXPECT_THROW(
      {
        try {
          auto tokens = sdb::ReadInputQuery(
              "where ( ( ( col1 == 100 ) and ( col2 == lol ) ) or "
              "( ( col3 != 50 ) and ( col4 > 11 ) )");
          size_t index = 0;
          auto op = sdb::ParseWhereClause(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected right parenthesis.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, UpdateQueriesSuccess0) {
  EXPECT_NO_THROW(sdb::ParseInputQuery(
      "Update random_table values col1 100 col2 200 where ( col1 < 100 )"));
}

TEST(ParserTest, UpdateQueriesSuccess1) {
  EXPECT_NO_THROW(
      sdb::ParseInputQuery("Update random_table values col1 100 col2 200"));
}

TEST(ParserTest, UpdateQueriesSuccess2) {
  EXPECT_NO_THROW(
      sdb::ParseInputQuery("Update random_table values col1 100 col2 200 where "
                           "( ( col1 < 100 ) and ( col2 != lol ) )"));
}

TEST(ParserTest, UpdateQueriesFailure0) {
  EXPECT_THROW(
      {
        try {
          sdb::ParseInputQuery("Update random_table col1 100");
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected keyword values.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, UpdateQueriesFailure1) {
  EXPECT_THROW(
      {
        try {
          sdb::ParseInputQuery(
              "Update random_table values col1 where ( col1 > 100 )");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected column value.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, UpdateQueriesFailure2) {
  EXPECT_THROW(
      {
        try {
          sdb::ParseInputQuery(
              "Update random_table values where ( col1 > 100 )");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected column name.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, SelectQuerySuccess0) {
  EXPECT_NO_THROW(sdb::ParseInputQuery("select col1 col2 from some_table"));
}

TEST(ParserTest, SelectQuerySuccess1) {
  EXPECT_NO_THROW(sdb::ParseInputQuery("select * from some_table"));
}

TEST(ParserTest, SelectQuerySuccess2) {
  EXPECT_NO_THROW(sdb::ParseInputQuery(
      "select col1 col2 from some_table where ( col1 > 100 )"));
}

TEST(ParserTest, SelectQuerySuccess3) {
  EXPECT_NO_THROW(
      sdb::ParseInputQuery("select * from some_table where ( col1 > 100 )"));
}

TEST(ParserTest, SelectQueryFailure0) {
  EXPECT_THROW(
      {
        try {
          auto op = sdb::ParseInputQuery("select from some_table");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected keyword star.") !=
                      std::string::npos);
          EXPECT_TRUE(std::string(err.what()).find("Expected column name.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, SelectQueryFailure1) {
  EXPECT_THROW(
      {
        try {
          auto op = sdb::ParseInputQuery("select * from");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected table name.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, SelectQueryFailure2) {
  EXPECT_THROW(
      {
        try {
          auto op = sdb::ParseInputQuery("select col1 from");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected table name.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, JoinQueriesSuccess0) {
  EXPECT_NO_THROW(sdb::ParseInputQuery(
      "Select * from t1 join t2 on ( t1.col1 == t2.col2 )"));
}

TEST(ParserTest, JoinQueriesSuccess1) {
  EXPECT_NO_THROW(
      sdb::ParseInputQuery("Select * from t1 join t2 on ( t1.col1 == t2.col2 ) "
                           "where ( ( t1.col1 > 100 ) and ( t2.col2 < 50 ) )"));
}
