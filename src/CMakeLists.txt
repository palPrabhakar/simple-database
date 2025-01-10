include(FetchContent)

FetchContent_Declare(
    sjp
    GIT_REPOSITORY git@github.com:palPrabhakar/simple-json-parser.git
    GIT_TAG main
)

FetchContent_MakeAvailable(sjp)

file(GLOB_RECURSE SOURCE_FILES ${CMAKE_SOURCE_DIR}/src/*.cpp)

add_library(sdb ${SOURCE_FILES})

target_include_directories(sdb PUBLIC ${CMAKE_SOURCE_DIR}/includes)

target_link_libraries(sdb sjp)
