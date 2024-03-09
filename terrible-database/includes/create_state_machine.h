#ifndef CREATE_STATE_MACHINE_H
#define CREATE_STATE_MACHINE_H

#include "tokenizer.h"
#include "data_types.h"
#include <string>
#include <unordered_set>
#include <vector>

namespace tdb {
class CreateStateMachine {
public:
  CreateStateMachine() {
    current_state = begin;
    expected_next_state.insert(create);
  }

  // end of parsing
  bool EOP() { return current_state == end; }
  bool CheckTransition(Token token, std::string word);
  std::string GetErrorMsg();

  std::string table_name;
  std::vector<std::string> col_names;
  std::vector<Data_Type> col_types;

private:
  // clang-format off
        enum State {
            begin,
            create,
            tbl_name,
            with,
            col_name,
            col_type,
            end,
            error
        };
  // clang-format on

  enum State current_state;
  std::unordered_set<State> expected_next_state;
  std::string err_msg;

  bool check_create_state();
  bool check_tbl_name_state(std::string word);
  bool check_col_name_state(std::string word);
  bool check_col_type_state(Data_Type type);
  bool check_with_state();
  bool check_end_state();
};
} // namespace tdb

#endif
