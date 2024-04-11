#pragma once

#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tdb {
// clang-format off
enum Token {
    TEXT,
    CREATE,
    INSERT,
    DELETE,
    UPDATE,
    SELECT,
    WHERE,
    FROM,
    STAR,
    OP_EQ,  // ==
    OP_NEQ, // !=
    OP_GT,  // >
    OP_LT,  // <
    OP_EQGT, // >=
    OP_EQLT, // <=
    AND,
    OR,
    LEFT_PAREN,
    RIGHT_PAREN,
    TYPE_INT,
    TYPE_DOUBLE,
    TYPE_STRING,
    VALUES,
    WITH,
    JOIN,
    ON,
    END
};

const std::unordered_map<std::string, Token> token_lookup = {
    { "create", CREATE },
    { "insert", INSERT },
    { "update", UPDATE },
    { "delete", DELETE },
    { "select", SELECT },
    { "where", WHERE },
    { "from", FROM },
    { "*", STAR },
    { "==", OP_EQ },
    { "!=", OP_NEQ },
    { ">", OP_GT },
    { "<", OP_LT },
    { ">=", OP_EQGT },
    { "<=", OP_EQLT },
    { "and", AND },
    { "or", OR },
    { "(", LEFT_PAREN },
    { ")", RIGHT_PAREN },
    { "int", TYPE_INT },
    { "double", TYPE_DOUBLE},
    { "string", TYPE_STRING},
    { "values", VALUES },
    { "with", WITH },
    { "join", JOIN },
    { "on", ON }
};
// clang-format on

std::vector<std::pair<Token, std::string>> ReadInputQuery(
    std::string input_query);
}  // namespace tdb
