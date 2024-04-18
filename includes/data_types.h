#pragma once

#include <cstdint>
#include <string>

namespace sdb {
// clang-format off
enum Data_Type {
    DT_INT,
    DT_DOUBLE,
    DT_STRING,
    DT_INVALID
};

struct sINT {
  using type = int64_t;
};

struct sDOUBLE {
  using type = double;
};

struct sSTRING {
  using type = std::string;
};

template<typename T>
Data_Type get_tdb_type();
// clang-format on
}  // namespace sdb
