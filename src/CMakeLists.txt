file(GLOB_RECURSE SOURCE_FILES ${CMAKE_SOURCE_DIR}/src/*.cpp)

find_package(jsoncpp REQUIRED)

add_library(sdb ${SOURCE_FILES})

target_include_directories(sdb PUBLIC ${CMAKE_SOURCE_DIR}/includes)

target_link_libraries(sdb jsoncpp_lib sjp)
