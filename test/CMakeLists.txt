enable_testing()

find_package(GTest REQUIRED)

add_executable(test parser_test.cpp operator_test.cpp)

target_link_libraries(test PRIVATE tdb GTest::gtest_main)

include(GoogleTest)

gtest_discover_tests(test)
