#pragma once

#include <algorithm>
#include <future>
#include <iostream>
#include <iterator>

#include "operator.h"

namespace tdb {
class UnionOperator : public BinaryOperator {
 public:
  UnionOperator(std::unique_ptr<BinaryOperator> &&lhs,
                std::unique_ptr<BinaryOperator> &&rhs)
      : lhs(std::move(lhs)), rhs(std::move(rhs)) {}

  void Execute() { DoUnion(tables[0].get()); }

  std::vector<size_t> GetArgResults(const Table *ptr) {
    DoUnion(ptr);
    return std::move(arg_results);
  }

 private:
  std::unique_ptr<BinaryOperator> lhs;
  std::unique_ptr<BinaryOperator> rhs;

  void DoUnion(const Table *ptr) {
    auto ltask = std::async(std::launch::async, &BinaryOperator::GetArgResults,
                            lhs.get(), ptr);

    auto rtask = std::async(std::launch::async, &BinaryOperator::GetArgResults,
                            rhs.get(), ptr);

    ltask.wait();
    rtask.wait();
    auto left = ltask.get();
    auto right = rtask.get();

    std::set_union(left.begin(), left.end(), right.begin(), right.end(),
                   std::back_inserter(arg_results));
  }
};

class IntersectionOperator : public BinaryOperator {
 public:
  IntersectionOperator(std::unique_ptr<BinaryOperator> &&lhs,
                       std::unique_ptr<BinaryOperator> &&rhs)
      : lhs(std::move(lhs)), rhs(std::move(rhs)) {}

  void Execute() { DoIntersection(tables[0].get()); }

  std::vector<size_t> GetArgResults(const Table *ptr) {
    DoIntersection(ptr);
    return std::move(arg_results);
  }

 private:
  std::unique_ptr<BinaryOperator> lhs;
  std::unique_ptr<BinaryOperator> rhs;

  void DoIntersection(const Table *ptr) {
    auto ltask = std::async(std::launch::async, &BinaryOperator::GetArgResults,
                            lhs.get(), ptr);

    auto rtask = std::async(std::launch::async, &BinaryOperator::GetArgResults,
                            rhs.get(), ptr);

    ltask.wait();
    rtask.wait();
    auto left = ltask.get();
    auto right = rtask.get();

    std::set_intersection(left.begin(), left.end(), right.begin(), right.end(),
                          std::back_inserter(arg_results));
  }
};

}  // namespace tdb
