project(vpack)

cmake_minimum_required(VERSION 3.2)

set(CMAKE_AR "gcc-ar")
set(CMAKE_RANLIB "gcc-ranlib")

# set(CMAKE_C_COMPILER "/usr/bin/clang" CACHE string "clang compiler" FORCE)
# set(CMAKE_CXX_COMPILER "/usr/bin/clang++" CACHE string "clang compiler" FORCE)
# set(CMAKE_AR      "/usr/bin/llvm-ar")
# set(CMAKE_LINKER  "/usr/bin/llvm-ld")
# set(CMAKE_NM      "/usr/bin/llvm-nm")
# set(CMAKE_OBJDUMP "/usr/bin/llvm-objdump")
# set(CMAKE_RANLIB  "/usr/bin/llvm-ranlib")

add_subdirectory("velocypack" EXCLUDE_FROM_ALL)

set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR} ${CMAKE_MODULE_PATH})

# add_definitions(-DDEBUG_MEMORY)
add_definitions(-DENABLE_FREE)

set(CMAKE_CXX_FLAGS -fPIC)
set(CMAKE_CXX_FLAGS -Wall)
set(CMAKE_CXX_FLAGS -Wextra)
set(CMAKE_CXX_FLAGS -Werror)
set(CMAKE_CXX_FLAGS -Wno-unused-parameter)
set(CMAKE_CXX_FLAGS -std=c++11)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -ggdb -Og -g3 -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -march=native -flto")

find_package(BISON)
find_package(FLEX)

BISON_TARGET(vpack src/vpackpath/vpackpath_gram.y ${CMAKE_CURRENT_BINARY_DIR}/vpackpath_gram.c)
FLEX_TARGET(vpack src/vpackpath/vpackpath_scan.l ${CMAKE_CURRENT_BINARY_DIR}/vpackpath_scan.c)
add_flex_bison_dependency(vpack vpack)

add_custom_target(flex ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/vpackpath_scan.c)

add_library(vpack SHARED
	include/memory
	include/vpack
	include/vpackpath/vpackpath
	src/memory
	src/vpack
	src/vpack_ops
	src/vpack_gin
	src/vpack_bson
	src/vpackpath/vpackpath_exec
	src/vpackpath/vpackpath
	${BISON_vpack_OUTPUTS}
)

target_include_directories(vpack PRIVATE src)
target_include_directories(vpack PUBLIC include)

set_target_properties(vpack PROPERTIES PREFIX "")

target_link_libraries(vpack ${CMAKE_SOURCE_DIR}/build-mongo/src/libbson/libbson-static-1.0.a)
include_directories(mongo-c-driver/src/libbson/src)
include_directories(build-mongo/src/libbson/src)
include_directories(build-mongo/src/libbson/src/bson)

target_link_libraries(vpack "velocypack")
include_directories(velocypack/include)
target_compile_definitions(velocypack PUBLIC -Dvelocypack_malloc=_palloc -Dvelocypack_realloc=_repalloc -Dvelocypack_free=_pfree -DVELOCYPACK_NO_THREADLOCALS)


find_package(Postgres)
include_directories(${Postgres_INCLUDEDIR})

install(TARGETS vpack DESTINATION ${Postgres_LIBDIR}
	PERMISSIONS WORLD_EXECUTE GROUP_EXECUTE OWNER_EXECUTE WORLD_READ GROUP_READ OWNER_READ OWNER_WRITE)

install(FILES vpack.control vpack--0.1.sql DESTINATION ${Postgres_EXTENSIONDIR})


find_program(PG_REGRESS pg_regress ${Postgres_LIBDIR}/pgxs/src/test/regress/ REQUIRED)

add_custom_target(installcheck COMMAND ${PG_REGRESS} --inputdir=${PROJECT_SOURCE_DIR}/test --outputdir=${PROJECT_SOURCE_DIR}/test --load-extension=vpack vpackpath vpackpath_encoding vpack_vpackpath)
