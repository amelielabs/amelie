#
# amelie cmake.
#
cmake_minimum_required(VERSION 3.24)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
set(CMAKE_SOURCE_DIR "${CMAKE_SOURCE_DIR}/..")

if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
	set(CMAKE_BUILD_TYPE "Debug")
endif()

file(READ "${CMAKE_SOURCE_DIR}/VERSION" AMELIE_VERSION)
string(STRIP "${AMELIE_VERSION}" AMELIE_VERSION)

message(STATUS "")
message(STATUS "amelie.")
message(STATUS "")
message(STATUS "Real-Time SQL OLTP Database.")
message(STATUS "")
message(STATUS "VERSION:          ${AMELIE_VERSION}")
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
message(STATUS "")

project(amelie C)

# compiler options
set(CMAKE_C_COMPILER "clang")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
	set(CMAKE_C_FLAGS "-Werror -Wall -Wextra -fPIC -g -O2")
elseif("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
	set(CMAKE_C_FLAGS "-Werror -Wall -Wextra -fPIC -g -O0")
endif()

set_property(GLOBAL PROPERTY TARGET_MESSAGES OFF)
set_property(GLOBAL PROPERTY RULE_MESSAGES OFF)
set(CMAKE_C_COMPILER_LAUNCHER "${CMAKE_SOURCE_DIR}/build/launcher.sh")
set(CMAKE_C_LINKER_LAUNCHER "${CMAKE_SOURCE_DIR}/build/launcher.sh")

# libraries
set(CMAKE_THREAD_PREFER_PTHREAD true)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(PCRE REQUIRED)
find_package(CURL REQUIRED)

include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(${PCRE_INCLUDE_DIR})
include_directories(${CURL_INCLUDE_DIR})

set(AMELIE_LIBRARIES m
    ${CMAKE_THREAD_LIBS_INIT}
    ${OPENSSL_LIBRARIES}
    ${PCRE_LIBRARIES})

# project includes
set(AMELIE_ROOT "${CMAKE_SOURCE_DIR}/amelie")

# runtime
include_directories("${AMELIE_ROOT}/runtime")
include_directories("${AMELIE_ROOT}/runtime/runtime")
include_directories("${AMELIE_ROOT}/runtime/io")
include_directories("${AMELIE_ROOT}/runtime/lib")
include_directories("${AMELIE_ROOT}/runtime/data")
include_directories("${AMELIE_ROOT}/runtime/config")

# server
include_directories("${AMELIE_ROOT}/server")
include_directories("${AMELIE_ROOT}/server/user")
include_directories("${AMELIE_ROOT}/server/auth")
include_directories("${AMELIE_ROOT}/server/http")
include_directories("${AMELIE_ROOT}/server/client")
include_directories("${AMELIE_ROOT}/server/server")

# db
include_directories("${AMELIE_ROOT}/db")
include_directories("${AMELIE_ROOT}/db/row")
include_directories("${AMELIE_ROOT}/db/transaction")
include_directories("${AMELIE_ROOT}/db/index")
include_directories("${AMELIE_ROOT}/db/partition")
include_directories("${AMELIE_ROOT}/db/checkpoint")
include_directories("${AMELIE_ROOT}/db/wal")
include_directories("${AMELIE_ROOT}/db/db")

# vm
include_directories("${AMELIE_ROOT}/vm")
include_directories("${AMELIE_ROOT}/vm/value")
include_directories("${AMELIE_ROOT}/vm/aggr")
include_directories("${AMELIE_ROOT}/vm/executor")
include_directories("${AMELIE_ROOT}/vm/vm")
include_directories("${AMELIE_ROOT}/vm/func")

# compiler
include_directories("${AMELIE_ROOT}/compiler")
include_directories("${AMELIE_ROOT}/compiler/parser")
include_directories("${AMELIE_ROOT}/compiler/planner")
include_directories("${AMELIE_ROOT}/compiler/compiler")

# cluster
include_directories("${AMELIE_ROOT}/cluster")
include_directories("${AMELIE_ROOT}/cluster/backup")
include_directories("${AMELIE_ROOT}/cluster/repl")
include_directories("${AMELIE_ROOT}/cluster/cluster")

# main
include_directories("${AMELIE_ROOT}/main")
include_directories("${AMELIE_ROOT}/main/frontend")
include_directories("${AMELIE_ROOT}/main/import")
include_directories("${AMELIE_ROOT}/main/session")
include_directories("${AMELIE_ROOT}/main/system")
include_directories("${AMELIE_ROOT}/main/main")

# test suite
include_directories("${CMAKE_SOURCE_DIR}/test")
include_directories("${CMAKE_SOURCE_DIR}/test/suite")

# sources
set(AMELIE_SOURCES_RUNTIME
    ${AMELIE_ROOT}/runtime/runtime/report.c
    ${AMELIE_ROOT}/runtime/runtime/timer_mgr.c
    ${AMELIE_ROOT}/runtime/runtime/poller.c
    ${AMELIE_ROOT}/runtime/runtime/notify.c
    ${AMELIE_ROOT}/runtime/runtime/context_stack.c
    ${AMELIE_ROOT}/runtime/runtime/context.c
    ${AMELIE_ROOT}/runtime/runtime/bus.c
    ${AMELIE_ROOT}/runtime/runtime/channel.c
    ${AMELIE_ROOT}/runtime/runtime/task.c
    ${AMELIE_ROOT}/runtime/io/vfs.c
    ${AMELIE_ROOT}/runtime/io/tls_lib.c
    ${AMELIE_ROOT}/runtime/io/tls_context.c
    ${AMELIE_ROOT}/runtime/io/tls.c
    ${AMELIE_ROOT}/runtime/io/socket.c
    ${AMELIE_ROOT}/runtime/io/tcp.c
    ${AMELIE_ROOT}/runtime/io/listen.c
    ${AMELIE_ROOT}/runtime/io/resolver.c
    ${AMELIE_ROOT}/runtime/lib/rbtree.c
    ${AMELIE_ROOT}/runtime/lib/random.c
    ${AMELIE_ROOT}/runtime/lib/uuid.c
    ${AMELIE_ROOT}/runtime/lib/crc.c
    ${AMELIE_ROOT}/runtime/lib/base64.c
    ${AMELIE_ROOT}/runtime/lib/base64url.c
    ${AMELIE_ROOT}/runtime/lib/timezone.c
    ${AMELIE_ROOT}/runtime/lib/timezone_mgr.c
    ${AMELIE_ROOT}/runtime/lib/interval.c
    ${AMELIE_ROOT}/runtime/lib/timestamp.c
    ${AMELIE_ROOT}/runtime/lib/vector.c
    ${AMELIE_ROOT}/runtime/lib/logger.c
    ${AMELIE_ROOT}/runtime/lib/linenoise.c
    ${AMELIE_ROOT}/runtime/lib/cli.c
    ${AMELIE_ROOT}/runtime/data/json.c
    ${AMELIE_ROOT}/runtime/data/json_export.c
    ${AMELIE_ROOT}/runtime/config/vars.c
    ${AMELIE_ROOT}/runtime/config/config_local.c
    ${AMELIE_ROOT}/runtime/config/config.c)

set(AMELIE_SOURCES_SERVER
    ${AMELIE_ROOT}/server/user/user_mgr.c
    ${AMELIE_ROOT}/server/auth/jwt_decode.c
    ${AMELIE_ROOT}/server/auth/jwt_encode.c
    ${AMELIE_ROOT}/server/auth/jwt.c
    ${AMELIE_ROOT}/server/auth/auth_cache.c
    ${AMELIE_ROOT}/server/auth/auth.c
    ${AMELIE_ROOT}/server/http/http.c
    ${AMELIE_ROOT}/server/http/uri.c
    ${AMELIE_ROOT}/server/client/client.c
    ${AMELIE_ROOT}/server/server/server.c
    ${AMELIE_ROOT}/server/server/server_bootstrap.c)

set(AMELIE_SOURCES_DB
    ${AMELIE_ROOT}/db/row/row.c
    ${AMELIE_ROOT}/db/transaction/handle_mgr.c
    ${AMELIE_ROOT}/db/transaction/commit.c
    ${AMELIE_ROOT}/db/index/tree.c
    ${AMELIE_ROOT}/db/index/index_tree.c
    ${AMELIE_ROOT}/db/index/index_hash.c
    ${AMELIE_ROOT}/db/partition/part.c
    ${AMELIE_ROOT}/db/partition/part_build.c
    ${AMELIE_ROOT}/db/partition/part_op.c
    ${AMELIE_ROOT}/db/partition/part_list.c
    ${AMELIE_ROOT}/db/checkpoint/snapshot.c
    ${AMELIE_ROOT}/db/checkpoint/snapshot_cursor.c
    ${AMELIE_ROOT}/db/checkpoint/checkpoint_mgr.c
    ${AMELIE_ROOT}/db/checkpoint/checkpoint.c
    ${AMELIE_ROOT}/db/checkpoint/checkpointer.c
    ${AMELIE_ROOT}/db/wal/wal.c
    ${AMELIE_ROOT}/db/wal/wal_cursor.c
    ${AMELIE_ROOT}/db/db/schema_mgr.c
    ${AMELIE_ROOT}/db/db/table_mgr.c
    ${AMELIE_ROOT}/db/db/table_index.c
    ${AMELIE_ROOT}/db/db/view_mgr.c
    ${AMELIE_ROOT}/db/db/node_mgr.c
    ${AMELIE_ROOT}/db/db/db.c
    ${AMELIE_ROOT}/db/db/db_checkpoint.c
    ${AMELIE_ROOT}/db/db/recover_checkpoint.c
    ${AMELIE_ROOT}/db/db/recover.c
    ${AMELIE_ROOT}/db/db/cascade.c)

set(AMELIE_SOURCES_VM
    ${AMELIE_ROOT}/vm/value/update.c
    ${AMELIE_ROOT}/vm/value/like.c
    ${AMELIE_ROOT}/vm/aggr/set.c
    ${AMELIE_ROOT}/vm/aggr/any.c
    ${AMELIE_ROOT}/vm/aggr/all.c
    ${AMELIE_ROOT}/vm/aggr/merge.c
    ${AMELIE_ROOT}/vm/aggr/count.c
    ${AMELIE_ROOT}/vm/aggr/sum.c
    ${AMELIE_ROOT}/vm/aggr/avg.c
    ${AMELIE_ROOT}/vm/aggr/min.c
    ${AMELIE_ROOT}/vm/aggr/max.c
    ${AMELIE_ROOT}/vm/aggr/lambda.c
    ${AMELIE_ROOT}/vm/aggr/aggrs.c
    ${AMELIE_ROOT}/vm/aggr/group.c
    ${AMELIE_ROOT}/vm/aggr/group_merge.c
    ${AMELIE_ROOT}/vm/executor/executor.c
    ${AMELIE_ROOT}/vm/vm/op.c
    ${AMELIE_ROOT}/vm/vm/vm.c
    ${AMELIE_ROOT}/vm/vm/command.c
    ${AMELIE_ROOT}/vm/func/fn_system.c
    ${AMELIE_ROOT}/vm/func/fn_cast.c
    ${AMELIE_ROOT}/vm/func/fn_null.c
    ${AMELIE_ROOT}/vm/func/fn_object.c
    ${AMELIE_ROOT}/vm/func/fn_string.c
    ${AMELIE_ROOT}/vm/func/fn_regexp.c
    ${AMELIE_ROOT}/vm/func/fn_math.c
    ${AMELIE_ROOT}/vm/func/fn_misc.c
    ${AMELIE_ROOT}/vm/func/fn_time.c
    ${AMELIE_ROOT}/vm/func/fn_vector.c
    ${AMELIE_ROOT}/vm/func/fn.c)

set(AMELIE_SOURCES_COMPILER
    ${AMELIE_ROOT}/compiler/parser/keyword.c
    ${AMELIE_ROOT}/compiler/parser/lex.c
    ${AMELIE_ROOT}/compiler/parser/parse_system.c
    ${AMELIE_ROOT}/compiler/parser/parse_user.c
    ${AMELIE_ROOT}/compiler/parser/parse_token.c
    ${AMELIE_ROOT}/compiler/parser/parse_replica.c
    ${AMELIE_ROOT}/compiler/parser/parse_repl.c
    ${AMELIE_ROOT}/compiler/parser/parse_node.c
    ${AMELIE_ROOT}/compiler/parser/parse_schema.c
    ${AMELIE_ROOT}/compiler/parser/parse_table.c
    ${AMELIE_ROOT}/compiler/parser/parse_index.c
    ${AMELIE_ROOT}/compiler/parser/parse_view.c
    ${AMELIE_ROOT}/compiler/parser/parse_cte.c
    ${AMELIE_ROOT}/compiler/parser/parse_expr.c
    ${AMELIE_ROOT}/compiler/parser/parse_insert.c
    ${AMELIE_ROOT}/compiler/parser/parse_update.c
    ${AMELIE_ROOT}/compiler/parser/parse_delete.c
    ${AMELIE_ROOT}/compiler/parser/parse_from.c
    ${AMELIE_ROOT}/compiler/parser/parse_select.c
    ${AMELIE_ROOT}/compiler/parser/parse_watch.c
    ${AMELIE_ROOT}/compiler/parser/parse.c
    ${AMELIE_ROOT}/compiler/parser/parser.c
    ${AMELIE_ROOT}/compiler/planner/path.c
    ${AMELIE_ROOT}/compiler/planner/planner.c
    ${AMELIE_ROOT}/compiler/compiler/compiler.c
    ${AMELIE_ROOT}/compiler/compiler/scan.c
    ${AMELIE_ROOT}/compiler/compiler/emit_expr.c
    ${AMELIE_ROOT}/compiler/compiler/emit_insert.c
    ${AMELIE_ROOT}/compiler/compiler/emit_update.c
    ${AMELIE_ROOT}/compiler/compiler/emit_delete.c
    ${AMELIE_ROOT}/compiler/compiler/emit_upsert.c
    ${AMELIE_ROOT}/compiler/compiler/emit_select.c
    ${AMELIE_ROOT}/compiler/compiler/emit_watch.c
    ${AMELIE_ROOT}/compiler/compiler/pushdown.c)

set(AMELIE_SOURCES_CLUSTER
    ${AMELIE_ROOT}/cluster/backup/backup.c
    ${AMELIE_ROOT}/cluster/backup/restore.c
    ${AMELIE_ROOT}/cluster/repl/streamer.c
    ${AMELIE_ROOT}/cluster/repl/replica_mgr.c
    ${AMELIE_ROOT}/cluster/repl/primary.c
    ${AMELIE_ROOT}/cluster/repl/repl.c
    ${AMELIE_ROOT}/cluster/cluster/compute.c
    ${AMELIE_ROOT}/cluster/cluster/cluster.c
    ${AMELIE_ROOT}/cluster/cluster/cluster_bootstrap.c
    ${AMELIE_ROOT}/cluster/cluster/build.c)

set(AMELIE_SOURCES_MAIN
    ${AMELIE_ROOT}/main/frontend/frontend.c
    ${AMELIE_ROOT}/main/import/import.c
    ${AMELIE_ROOT}/main/import/import_json.c
    ${AMELIE_ROOT}/main/import/import_csv.c
    ${AMELIE_ROOT}/main/session/explain.c
    ${AMELIE_ROOT}/main/session/session.c
    ${AMELIE_ROOT}/main/session/utility.c
    ${AMELIE_ROOT}/main/session/ddl.c
    ${AMELIE_ROOT}/main/session/primary.c
    ${AMELIE_ROOT}/main/system/system.c
    ${AMELIE_ROOT}/main/main/login_mgr.c
    ${AMELIE_ROOT}/main/main/main.c
    ${AMELIE_ROOT}/main/main/amelie.c
    ${AMELIE_ROOT}/main/main/bench.c
    ${AMELIE_ROOT}/main/main/bench_insert.c
    ${AMELIE_ROOT}/main/main/bench_upsert.c
    ${AMELIE_ROOT}/main/main/bench_tpcb.c
    ${AMELIE_ROOT}/main/main/bench_import.c
    ${AMELIE_ROOT}/main/main/ctl.c)

set(AMELIE_SOURCES
    ${AMELIE_SOURCES_RUNTIME}
    ${AMELIE_SOURCES_SERVER}
    ${AMELIE_SOURCES_DB}
    ${AMELIE_SOURCES_VM}
    ${AMELIE_SOURCES_COMPILER}
    ${AMELIE_SOURCES_CLUSTER}
    ${AMELIE_SOURCES_MAIN})

set(AMELIE_SOURCES_CLI
    ${CMAKE_SOURCE_DIR}/cli/amelie.c)

set(AMELIE_SOURCES_TEST_SUITE
    ${CMAKE_SOURCE_DIR}/test/runtime/test_task.c
    ${CMAKE_SOURCE_DIR}/test/runtime/test_exception.c
    ${CMAKE_SOURCE_DIR}/test/runtime/test_palloc.c
    ${CMAKE_SOURCE_DIR}/test/runtime/test_buf.c
    ${CMAKE_SOURCE_DIR}/test/runtime/test_msg.c
    ${CMAKE_SOURCE_DIR}/test/runtime/test_coroutine.c
    ${CMAKE_SOURCE_DIR}/test/runtime/test_event.c
    ${CMAKE_SOURCE_DIR}/test/runtime/test_condition.c
    ${CMAKE_SOURCE_DIR}/test/runtime/test_channel.c
    ${CMAKE_SOURCE_DIR}/test/runtime/test_rpc.c
    ${CMAKE_SOURCE_DIR}/test/runtime/test_cancel.c
    ${CMAKE_SOURCE_DIR}/test/runtime/test_lock.c
    ${CMAKE_SOURCE_DIR}/test/runtime/test_sleep.c
    ${CMAKE_SOURCE_DIR}/test/runtime/test_benchmark.c
    ${CMAKE_SOURCE_DIR}/test/suite/suite.c
    ${CMAKE_SOURCE_DIR}/test/suite/session.c
    ${CMAKE_SOURCE_DIR}/test/amelie-test.c)

# libamelie.a
add_library(amelie_static STATIC ${AMELIE_SOURCES})
set_target_properties(amelie_static PROPERTIES OUTPUT_NAME amelie)
set(AMELIE_STATIC_LINK "$<LINK_LIBRARY:WHOLE_ARCHIVE,amelie_static>")

# amelie
add_executable(amelie_cli ${AMELIE_SOURCES_CLI})
target_link_libraries(amelie_cli ${AMELIE_STATIC_LINK})
target_link_libraries(amelie_cli ${AMELIE_LIBRARIES})
set_target_properties(amelie_cli PROPERTIES ENABLE_EXPORTS ON OUTPUT_NAME amelie)

# amelie-test
add_executable(amelie_test ${AMELIE_SOURCES_TEST_SUITE})
target_link_libraries(amelie_test ${AMELIE_STATIC_LINK})
target_link_libraries(amelie_test ${AMELIE_LIBRARIES} ${CURL_LIBRARIES})
set_target_properties(amelie_test PROPERTIES ENABLE_EXPORTS ON OUTPUT_NAME amelie-test)
