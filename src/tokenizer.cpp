#include "tokenizer.h"

#include <algorithm>
#include <cctype>
#include <iostream>

namespace tdb {
// Every token needs to separated by whitespace
std::vector<std::pair<Token, std::string>> ReadInputQuery(
    std::string input_query) {
  std::transform(input_query.cbegin(), input_query.cend(), input_query.begin(),
                 [](unsigned char x) { return std::tolower(x); });
  std::stringstream query_stream(input_query);
  std::vector<std::pair<Token, std::string>> tokens;
  std::string word;

  while (query_stream >> word) {
    if (token_lookup.contains(word)) {
      auto token = token_lookup.at(word);
      tokens.push_back({token, word});
    } else {
      tokens.push_back({TEXT, word});
    }
  }

  tokens.push_back({END, "end"});

  return tokens;
}

}  // namespace tdb
