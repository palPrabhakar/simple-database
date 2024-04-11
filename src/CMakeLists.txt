file(GLOB_RECURSE SOURCE_FILES ${CMAKE_SOURCE_DIR}/src/*.cpp)

find_package(jsoncpp REQUIRED)

add_library(tdb ${SOURCE_FILES})

target_include_directories(tdb PUBLIC ${CMAKE_SOURCE_DIR}/includes)

target_link_libraries(tdb jsoncpp_lib)
