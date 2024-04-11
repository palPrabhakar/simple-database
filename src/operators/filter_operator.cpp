#include "operators/filter_operator.h"

namespace tdb {
void EqualityFilter::FilterStrings(BaseColumn *ptr, std::string val,
                                   size_t row_size) {
  StringColumn *scols = static_cast<StringColumn *>(ptr);
  for (auto i = 0; i < row_size; ++i) {
    if ((*scols)[i] == val) this->arg_results.push_back(i);
  }
}

void EqualityFilter::FilterInt64(BaseColumn *ptr, int64_t val,
                                 size_t row_size) {
  Int64Column *icols = static_cast<Int64Column *>(ptr);
  for (auto i = 0; i < row_size; ++i) {
    if ((*icols)[i] == val) this->arg_results.push_back(i);
  }
}

void EqualityFilter::FilterDouble(BaseColumn *ptr, double val,
                                  size_t row_size) {
  DoubleColumn *icols = static_cast<DoubleColumn *>(ptr);
  for (auto i = 0; i < row_size; ++i) {
    if ((*icols)[i] == val) this->arg_results.push_back(i);
  }
}

void NonEqualityFilter::FilterStrings(BaseColumn *ptr, std::string val,
                                      size_t row_size) {
  StringColumn *scols = static_cast<StringColumn *>(ptr);
  for (auto i = 0; i < row_size; ++i) {
    if ((*scols)[i] != val) this->arg_results.push_back(i);
  }
}

void NonEqualityFilter::FilterInt64(BaseColumn *ptr, int64_t val,
                                    size_t row_size) {
  Int64Column *icols = static_cast<Int64Column *>(ptr);
  for (auto i = 0; i < row_size; ++i) {
    if ((*icols)[i] != val) this->arg_results.push_back(i);
  }
}

void NonEqualityFilter::FilterDouble(BaseColumn *ptr, double val,
                                     size_t row_size) {
  DoubleColumn *icols = static_cast<DoubleColumn *>(ptr);
  for (auto i = 0; i < row_size; ++i) {
    if ((*icols)[i] != val) this->arg_results.push_back(i);
  }
}

void LessThanFilter::FilterInt64(BaseColumn *ptr, int64_t val,
                                 size_t row_size) {
  Int64Column *icols = static_cast<Int64Column *>(ptr);
  for (auto i = 0; i < row_size; ++i) {
    if ((*icols)[i] < val) this->arg_results.push_back(i);
  }
}

void LessThanFilter::FilterDouble(BaseColumn *ptr, double val,
                                  size_t row_size) {
  DoubleColumn *icols = static_cast<DoubleColumn *>(ptr);
  for (auto i = 0; i < row_size; ++i) {
    if ((*icols)[i] < val) this->arg_results.push_back(i);
  }
}

void GreaterThanFilter::FilterInt64(BaseColumn *ptr, int64_t val,
                                    size_t row_size) {
  Int64Column *icols = static_cast<Int64Column *>(ptr);
  for (auto i = 0; i < row_size; ++i) {
    if ((*icols)[i] > val) this->arg_results.push_back(i);
  }
}

void GreaterThanFilter::FilterDouble(BaseColumn *ptr, double val,
                                     size_t row_size) {
  DoubleColumn *icols = static_cast<DoubleColumn *>(ptr);
  for (auto i = 0; i < row_size; ++i) {
    if ((*icols)[i] > val) this->arg_results.push_back(i);
  }
}

void LessEqualFilter::FilterInt64(BaseColumn *ptr, int64_t val,
                                  size_t row_size) {
  Int64Column *icols = static_cast<Int64Column *>(ptr);
  for (auto i = 0; i < row_size; ++i) {
    if ((*icols)[i] <= val) this->arg_results.push_back(i);
  }
}

void LessEqualFilter::FilterDouble(BaseColumn *ptr, double val,
                                   size_t row_size) {
  DoubleColumn *icols = static_cast<DoubleColumn *>(ptr);
  for (auto i = 0; i < row_size; ++i) {
    if ((*icols)[i] <= val) this->arg_results.push_back(i);
  }
}

void GreaterEqualFilter::FilterInt64(BaseColumn *ptr, int64_t val,
                                     size_t row_size) {
  Int64Column *icols = static_cast<Int64Column *>(ptr);
  for (auto i = 0; i < row_size; ++i) {
    if ((*icols)[i] >= val) this->arg_results.push_back(i);
  }
}

void GreaterEqualFilter::FilterDouble(BaseColumn *ptr, double val,
                                      size_t row_size) {
  DoubleColumn *icols = static_cast<DoubleColumn *>(ptr);
  for (auto i = 0; i < row_size; ++i) {
    if ((*icols)[i] >= val) this->arg_results.push_back(i);
  }
}
}  // namespace tdb
