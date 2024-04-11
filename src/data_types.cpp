#include "data_types.h"

#include <stdexcept>
#include <string>
#include <type_traits>

namespace tdb {
template <typename T>
Data_Type get_tdb_type() {
  if constexpr (std::is_same_v<T, int64_t>) {
    return DT_INT;
  } else if (std::is_same_v<T, double>) {
    return DT_DOUBLE;
  } else if (std::is_same_v<T, std::string>) {
    return DT_STRING;
  }

  return DT_INVALID;
}
}  // namespace tdb
