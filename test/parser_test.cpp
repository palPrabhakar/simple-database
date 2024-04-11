#include <parser.h>

#include <exception>
#include <iostream>
#include <stdexcept>

#include "gtest/gtest.h"
#include "tokenizer.h"

TEST(ParserTest, CreateQueries) {
  EXPECT_NO_THROW(tdb::ParseInputQuery(
      "Create random_table with col1 int col2 double col3 string"));

  EXPECT_NO_THROW(tdb::ParseInputQuery("Create some_table with col1 int\n"));

  EXPECT_THROW(
      {
        try {
          tdb::ParseInputQuery("Create some_table with col1 int.");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected column type.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  EXPECT_THROW(
      {
        try {
          tdb::ParseInputQuery("Create random_table with col1 int col2 far");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected column type.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  EXPECT_THROW(
      {
        try {
          tdb::ParseInputQuery("Create random_table with col1 int double");
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

  EXPECT_THROW(
      {
        try {
          tdb::ParseInputQuery("Create with col1 int col2 double");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected table name.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  EXPECT_THROW(
      {
        try {
          tdb::ParseInputQuery("Create random_table col1 int col2 double");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected keyword with.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, InsertQueries) {
  EXPECT_NO_THROW(
      tdb::ParseInputQuery("Insert random_table values 100 250 lol"));

  EXPECT_NO_THROW(
      tdb::ParseInputQuery("Insert some_table values 250.00 very-lol\n"));

  EXPECT_NO_THROW(
      tdb::ParseInputQuery("Insert some_table values lol1 lol...lol"));

  EXPECT_THROW(
      {
        try {
          tdb::ParseInputQuery("Insert values col1 int col2 far");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected table name.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  EXPECT_THROW(
      {
        try {
          tdb::ParseInputQuery("Insert random_table values col1 int");
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

  EXPECT_THROW(
      {
        try {
          tdb::ParseInputQuery("Insert random_table with col1 int col2 double");
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected keyword values.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, ExprParsing) {
  auto tokens = tdb::ReadInputQuery("( col1 == 100 )");
  size_t index = 0;
  EXPECT_NO_THROW(tdb::ParseExpression(tokens, index));

  tokens = tdb::ReadInputQuery("( col1 > 100 )");
  index = 0;
  EXPECT_NO_THROW(tdb::ParseExpression(tokens, index));

  tokens = tdb::ReadInputQuery(" col1 > 100 )");
  index = 0;
  EXPECT_THROW(
      {
        try {
          tdb::ParseExpression(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected left parenthesis.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  tokens = tdb::ReadInputQuery(" (  > 100 )");
  index = 0;
  EXPECT_THROW(
      {
        try {
          tdb::ParseExpression(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected column name.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  tokens = tdb::ReadInputQuery(" ( col1 100 )");
  index = 0;
  EXPECT_THROW(
      {
        try {
          tdb::ParseExpression(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected a binary operator.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  tokens = tdb::ReadInputQuery(" ( col1 <  )");
  index = 0;
  EXPECT_THROW(
      {
        try {
          tdb::ParseExpression(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected column value.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  tokens = tdb::ReadInputQuery(" ( col1 < 100");
  index = 0;
  EXPECT_THROW(
      {
        try {
          tdb::ParseExpression(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected right parenthesis.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, WhereClause) {
  auto tokens = tdb::ReadInputQuery("where ( col1 == 100 )");
  size_t index = 0;
  EXPECT_NO_THROW(tdb::ParseWhereClause(tokens, index));

  tokens = tdb::ReadInputQuery("where ( ( col1 == 100 ) and ( col2 == 50 ) )");
  index = 0;
  EXPECT_NO_THROW(tdb::ParseWhereClause(tokens, index));

  tokens = tdb::ReadInputQuery(
      "where ( ( ( col1 == 100 ) and ( col2 == lol ) ) or ( col3 != 50 ) )");
  index = 0;
  EXPECT_NO_THROW(tdb::ParseWhereClause(tokens, index));

  tokens = tdb::ReadInputQuery(
      "where ( ( ( col1 == 100 ) and ( col2 == lol ) ) or "
      "( ( col3 != 50 ) and ( col4 > 11 ) ) )");
  index = 0;
  EXPECT_NO_THROW(tdb::ParseWhereClause(tokens, index));

  tokens = tdb::ReadInputQuery("where ( ( ( col1 == 100 ) ) )");
  index = 0;
  EXPECT_THROW(tdb::ParseWhereClause(tokens, index), std::runtime_error);

  EXPECT_THROW(
      {
        try {
          auto tokens = tdb::ReadInputQuery("where ( ( col1 == 100 ) and )");
          size_t index = 0;
          auto op = tdb::ParseWhereClause(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected left parenthesis.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  EXPECT_THROW(
      {
        try {
          auto tokens =
              tdb::ReadInputQuery("where ( ( col1 == 100 ) and ( ) )");
          size_t index = 0;
          auto op = tdb::ParseWhereClause(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected left parenthesis.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  EXPECT_THROW(
      {
        try {
          auto tokens =
              tdb::ReadInputQuery("where ( ( col1 == 100 ) and ( col2 ) )");
          size_t index = 0;
          auto op = tdb::ParseWhereClause(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected a binary operator.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  EXPECT_THROW(
      {
        try {
          auto tokens = tdb::ReadInputQuery(
              "where ( ( ( col1 == 100 ) and ( col2 == lol ) ) or "
              "( ( col3 != 50 ) and ( col4 > 11 ) )");
          size_t index = 0;
          auto op = tdb::ParseWhereClause(tokens, index);
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected right parenthesis.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, UpdateQueries) {
  EXPECT_NO_THROW(tdb::ParseInputQuery(
      "Update random_table values col1 100 col2 200 where ( col1 < 100 )"));

  EXPECT_NO_THROW(
      tdb::ParseInputQuery("Update random_table values col1 100 col2 200"));

  EXPECT_NO_THROW(
      tdb::ParseInputQuery("Update random_table values col1 100 col2 200 where "
                           "( ( col1 < 100 ) and ( col2 != lol ) )"));

  EXPECT_THROW(
      {
        try {
          tdb::ParseInputQuery("Update random_table col1 100");
        } catch (std::exception &err) {
          EXPECT_TRUE(
              std::string(err.what()).find("Expected keyword values.") !=
              std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  EXPECT_THROW(
      {
        try {
          tdb::ParseInputQuery(
              "Update random_table values col1 where ( col1 > 100 )");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected column value.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  EXPECT_THROW(
      {
        try {
          tdb::ParseInputQuery(
              "Update random_table values where ( col1 > 100 )");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected column name.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, SelectQuery) {
  EXPECT_NO_THROW(tdb::ParseInputQuery("select col1 col2 from some_table"));

  EXPECT_NO_THROW(tdb::ParseInputQuery("select * from some_table"));

  EXPECT_NO_THROW(tdb::ParseInputQuery(
      "select col1 col2 from some_table where ( col1 > 100 )"));

  EXPECT_NO_THROW(
      tdb::ParseInputQuery("select * from some_table where ( col1 > 100 )"));

  EXPECT_THROW(
      {
        try {
          auto op = tdb::ParseInputQuery("select from some_table");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected keyword star.") !=
                      std::string::npos);
          EXPECT_TRUE(std::string(err.what()).find("Expected column name.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  EXPECT_THROW(
      {
        try {
          auto op = tdb::ParseInputQuery("select * from");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected table name.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);

  EXPECT_THROW(
      {
        try {
          auto op = tdb::ParseInputQuery("select col1 from");
        } catch (std::exception &err) {
          EXPECT_TRUE(std::string(err.what()).find("Expected table name.") !=
                      std::string::npos);
          throw;
        }
      },
      std::runtime_error);
}

TEST(ParserTest, JoinQueries) {
  EXPECT_NO_THROW(tdb::ParseInputQuery(
      "Select * from t1 join t2 on ( t1.col1 == t2.col2 )"));

  EXPECT_NO_THROW(
      tdb::ParseInputQuery("Select * from t1 join t2 on ( t1.col1 == t2.col2 ) "
                           "where ( ( t1.col1 > 100 ) and ( t2.col2 < 50 ) )"));
}
