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

# compiler options
set(CMAKE_C_COMPILER "clang")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
	set(CMAKE_C_FLAGS "-Werror -Wall -Wextra -Wno-free-nonheap-object -fPIC -g -O3 -march=native -DNDEBUG")
elseif("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
	set(CMAKE_C_FLAGS "-Werror -Wall -Wextra -fPIC -g -O0")
endif()

set_property(GLOBAL PROPERTY TARGET_MESSAGES OFF)
set_property(GLOBAL PROPERTY RULE_MESSAGES OFF)
set(CMAKE_C_COMPILER_LAUNCHER "${CMAKE_SOURCE_DIR}/build/launcher.sh")
set(CMAKE_C_LINKER_LAUNCHER "${CMAKE_SOURCE_DIR}/build/launcher.sh")

project(amelie C)

# libraries
set(CMAKE_THREAD_PREFER_PTHREAD true)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(PCRE REQUIRED)
find_package(ZSTD REQUIRED)

include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(${PCRE_INCLUDE_DIR})
include_directories(${ZSTD_INCLUDE_DIR})

set(AMELIE_LIBRARIES m
    ${CMAKE_THREAD_LIBS_INIT}
    ${OPENSSL_LIBRARIES}
    ${PCRE_LIBRARIES}
    ${ZSTD_LIBRARIES})

# project includes
set(AMELIE_ROOT "${CMAKE_SOURCE_DIR}/amelie")

# runtime
include_directories("${AMELIE_ROOT}/runtime")
include_directories("${AMELIE_ROOT}/runtime/base")
include_directories("${AMELIE_ROOT}/runtime/io")
include_directories("${AMELIE_ROOT}/runtime/lib")
include_directories("${AMELIE_ROOT}/runtime/data")
include_directories("${AMELIE_ROOT}/runtime/rel")
include_directories("${AMELIE_ROOT}/runtime/runtime")

# server
include_directories("${AMELIE_ROOT}/server")
include_directories("${AMELIE_ROOT}/server/jwt")
include_directories("${AMELIE_ROOT}/server/jsonrpc")
include_directories("${AMELIE_ROOT}/server/http")
include_directories("${AMELIE_ROOT}/server/client")
include_directories("${AMELIE_ROOT}/server/websocket")
include_directories("${AMELIE_ROOT}/server/server")

# db
include_directories("${AMELIE_ROOT}/db")
include_directories("${AMELIE_ROOT}/db/type")
include_directories("${AMELIE_ROOT}/db/storage")
include_directories("${AMELIE_ROOT}/db/flat")
include_directories("${AMELIE_ROOT}/db/heap")
include_directories("${AMELIE_ROOT}/db/cdc")
include_directories("${AMELIE_ROOT}/db/transaction")
include_directories("${AMELIE_ROOT}/db/index")
include_directories("${AMELIE_ROOT}/db/part")
include_directories("${AMELIE_ROOT}/db/catalog")
include_directories("${AMELIE_ROOT}/db/wal")
include_directories("${AMELIE_ROOT}/db/checkpoint")
include_directories("${AMELIE_ROOT}/db/db")

# repl
include_directories("${AMELIE_ROOT}/repl")
include_directories("${AMELIE_ROOT}/repl/backup")
include_directories("${AMELIE_ROOT}/repl/repl")

# vm
include_directories("${AMELIE_ROOT}/vm")
include_directories("${AMELIE_ROOT}/vm/value")
include_directories("${AMELIE_ROOT}/vm/set")
include_directories("${AMELIE_ROOT}/vm/output")
include_directories("${AMELIE_ROOT}/vm/commit")
include_directories("${AMELIE_ROOT}/vm/func")
include_directories("${AMELIE_ROOT}/vm/vm")

# compiler
include_directories("${AMELIE_ROOT}/compiler")
include_directories("${AMELIE_ROOT}/compiler/parser")
include_directories("${AMELIE_ROOT}/compiler/plan")
include_directories("${AMELIE_ROOT}/compiler/compiler")

# system
include_directories("${AMELIE_ROOT}/system")
include_directories("${AMELIE_ROOT}/system/frontend")
include_directories("${AMELIE_ROOT}/system/backend")
include_directories("${AMELIE_ROOT}/system/session")
include_directories("${AMELIE_ROOT}/system/system")
include_directories("${AMELIE_ROOT}/system/api")

# main
include_directories("${AMELIE_ROOT}/main")
include_directories("${AMELIE_ROOT}/main/main")
include_directories("${AMELIE_ROOT}/main/import")
include_directories("${AMELIE_ROOT}/main/bench")
include_directories("${AMELIE_ROOT}/main/test")
include_directories("${AMELIE_ROOT}/main/dst")

# sources
set(AMELIE_SOURCES_RUNTIME
    ${AMELIE_ROOT}/runtime/base/format.c
    ${AMELIE_ROOT}/runtime/base/report.c
    ${AMELIE_ROOT}/runtime/base/clock.c
    ${AMELIE_ROOT}/runtime/base/poller.c
    ${AMELIE_ROOT}/runtime/base/notify.c
    ${AMELIE_ROOT}/runtime/base/context_stack.c
    ${AMELIE_ROOT}/runtime/base/context.c
    ${AMELIE_ROOT}/runtime/base/random.c
    ${AMELIE_ROOT}/runtime/base/task.c
    ${AMELIE_ROOT}/runtime/io/os.c
    ${AMELIE_ROOT}/runtime/io/vfs.c
    ${AMELIE_ROOT}/runtime/io/tls_lib.c
    ${AMELIE_ROOT}/runtime/io/tls_context.c
    ${AMELIE_ROOT}/runtime/io/tls.c
    ${AMELIE_ROOT}/runtime/io/socket.c
    ${AMELIE_ROOT}/runtime/io/tcp.c
    ${AMELIE_ROOT}/runtime/io/listen.c
    ${AMELIE_ROOT}/runtime/lib/rbtree.c
    ${AMELIE_ROOT}/runtime/lib/uuid.c
    ${AMELIE_ROOT}/runtime/lib/crc.c
    ${AMELIE_ROOT}/runtime/lib/base64.c
    ${AMELIE_ROOT}/runtime/lib/base64url.c
    ${AMELIE_ROOT}/runtime/lib/timezone.c
    ${AMELIE_ROOT}/runtime/lib/timezones.c
    ${AMELIE_ROOT}/runtime/lib/date.c
    ${AMELIE_ROOT}/runtime/lib/interval.c
    ${AMELIE_ROOT}/runtime/lib/timestamp.c
    ${AMELIE_ROOT}/runtime/lib/vector.c
    ${AMELIE_ROOT}/runtime/lib/codec_compression.c
    ${AMELIE_ROOT}/runtime/lib/job_worker.c
    ${AMELIE_ROOT}/runtime/lib/jobs.c
    ${AMELIE_ROOT}/runtime/lib/logger.c
    ${AMELIE_ROOT}/runtime/lib/load.c
    ${AMELIE_ROOT}/runtime/lib/console.c
    ${AMELIE_ROOT}/runtime/lib/histogram.c
    ${AMELIE_ROOT}/runtime/data/json.c
    ${AMELIE_ROOT}/runtime/data/json_export.c
    ${AMELIE_ROOT}/runtime/rel/permissions.c
    ${AMELIE_ROOT}/runtime/runtime/opts.c
    ${AMELIE_ROOT}/runtime/runtime/config.c
    ${AMELIE_ROOT}/runtime/runtime/state.c
    ${AMELIE_ROOT}/runtime/runtime/runtime.c)

set(AMELIE_SOURCES_SERVER
    ${AMELIE_ROOT}/server/jwt/jwt_decode.c
    ${AMELIE_ROOT}/server/jwt/jwt_encode.c
    ${AMELIE_ROOT}/server/jwt/jwt.c
    ${AMELIE_ROOT}/server/jsonrpc/jsonrpc.c
    ${AMELIE_ROOT}/server/http/endpoint.c
    ${AMELIE_ROOT}/server/http/uri.c
    ${AMELIE_ROOT}/server/http/http.c
    ${AMELIE_ROOT}/server/client/client.c
    ${AMELIE_ROOT}/server/websocket/ws.c
    ${AMELIE_ROOT}/server/websocket/websocket.c
    ${AMELIE_ROOT}/server/server/server.c
    ${AMELIE_ROOT}/server/server/servers.c)

set(AMELIE_SOURCES_DB
    ${AMELIE_ROOT}/db/storage/storage_file.c
    ${AMELIE_ROOT}/db/flat/flat.c
    ${AMELIE_ROOT}/db/heap/heap.c
    ${AMELIE_ROOT}/db/heap/row_heap.c
    ${AMELIE_ROOT}/db/heap/row_encode.c
    ${AMELIE_ROOT}/db/cdc/cdc.c
    ${AMELIE_ROOT}/db/cdc/cdc_export.c
    ${AMELIE_ROOT}/db/transaction/rels.c
    ${AMELIE_ROOT}/db/index/tree.c
    ${AMELIE_ROOT}/db/index/index_tree.c
    ${AMELIE_ROOT}/db/index/index_hash.c
    ${AMELIE_ROOT}/db/part/part.c
    ${AMELIE_ROOT}/db/part/part_cleanup.c
    ${AMELIE_ROOT}/db/part/part_op.c
    ${AMELIE_ROOT}/db/part/parts.c
    ${AMELIE_ROOT}/db/part/cursor.c
    ${AMELIE_ROOT}/db/catalog/user.c
    ${AMELIE_ROOT}/db/catalog/table.c
    ${AMELIE_ROOT}/db/catalog/table_alter.c
    ${AMELIE_ROOT}/db/catalog/table_index.c
    ${AMELIE_ROOT}/db/catalog/clone.c
    ${AMELIE_ROOT}/db/catalog/udf.c
    ${AMELIE_ROOT}/db/catalog/topic.c
    ${AMELIE_ROOT}/db/catalog/publish.c
    ${AMELIE_ROOT}/db/catalog/sub.c
    ${AMELIE_ROOT}/db/catalog/acknowledge.c
    ${AMELIE_ROOT}/db/catalog/rel.c
    ${AMELIE_ROOT}/db/catalog/catalog.c
    ${AMELIE_ROOT}/db/catalog/catalog_file.c
    ${AMELIE_ROOT}/db/catalog/catalog_mcp.c
    ${AMELIE_ROOT}/db/catalog/cascade.c
    ${AMELIE_ROOT}/db/wal/wal.c
    ${AMELIE_ROOT}/db/wal/wal_recovery.c
    ${AMELIE_ROOT}/db/wal/wal_write.c
    ${AMELIE_ROOT}/db/wal/wal_cursor.c
    ${AMELIE_ROOT}/db/wal/wal_slot.c
    ${AMELIE_ROOT}/db/wal/wal_subscribe.c
    ${AMELIE_ROOT}/db/checkpoint/checkpoint.c
    ${AMELIE_ROOT}/db/checkpoint/checkpoints.c
    ${AMELIE_ROOT}/db/db/syncer.c
    ${AMELIE_ROOT}/db/db/db.c
    ${AMELIE_ROOT}/db/db/db_op.c
    ${AMELIE_ROOT}/db/db/db_create_index.c
    ${AMELIE_ROOT}/db/db/db_snapshot.c
    ${AMELIE_ROOT}/db/db/recover.c)

set(AMELIE_SOURCES_REPL
    ${AMELIE_ROOT}/repl/backup/backup.c
    ${AMELIE_ROOT}/repl/backup/restore.c
    ${AMELIE_ROOT}/repl/repl/streamer.c
    ${AMELIE_ROOT}/repl/repl/receiver.c
    ${AMELIE_ROOT}/repl/repl/node.c
    ${AMELIE_ROOT}/repl/repl/replicas.c
    ${AMELIE_ROOT}/repl/repl/repl.c)

set(AMELIE_SOURCES_VM
    ${AMELIE_ROOT}/vm/value/text.c
    ${AMELIE_ROOT}/vm/value/update.c
    ${AMELIE_ROOT}/vm/value/row.c
    ${AMELIE_ROOT}/vm/value/like.c
    ${AMELIE_ROOT}/vm/set/set.c
    ${AMELIE_ROOT}/vm/set/agg.c
    ${AMELIE_ROOT}/vm/set/union.c
    ${AMELIE_ROOT}/vm/set/any.c
    ${AMELIE_ROOT}/vm/set/all.c
    ${AMELIE_ROOT}/vm/set/in.c
    ${AMELIE_ROOT}/vm/set/matching.c
    ${AMELIE_ROOT}/vm/set/sub.c
    ${AMELIE_ROOT}/vm/output/print.c
    ${AMELIE_ROOT}/vm/output/output.c
    ${AMELIE_ROOT}/vm/output/output_json.c
    ${AMELIE_ROOT}/vm/output/output_jsonrpc.c
    ${AMELIE_ROOT}/vm/output/output_text.c
    ${AMELIE_ROOT}/vm/commit/commit.c
    ${AMELIE_ROOT}/vm/func/functions.c
    ${AMELIE_ROOT}/vm/func/call.c
    ${AMELIE_ROOT}/vm/func/calls.c
    ${AMELIE_ROOT}/vm/func/fn_system.c
    ${AMELIE_ROOT}/vm/func/fn_cast.c
    ${AMELIE_ROOT}/vm/func/fn_null.c
    ${AMELIE_ROOT}/vm/func/fn_json.c
    ${AMELIE_ROOT}/vm/func/fn_string.c
    ${AMELIE_ROOT}/vm/func/fn_regexp.c
    ${AMELIE_ROOT}/vm/func/fn_math.c
    ${AMELIE_ROOT}/vm/func/fn_misc.c
    ${AMELIE_ROOT}/vm/func/fn_time.c
    ${AMELIE_ROOT}/vm/func/fn_vector.c
    ${AMELIE_ROOT}/vm/func/fn_register.c
    ${AMELIE_ROOT}/vm/vm/op.c
    ${AMELIE_ROOT}/vm/vm/vm.c
    ${AMELIE_ROOT}/vm/vm/command.c
    ${AMELIE_ROOT}/vm/vm/command_utility.c)

set(AMELIE_SOURCES_COMPILER
    ${AMELIE_ROOT}/compiler/parser/keyword.c
    ${AMELIE_ROOT}/compiler/parser/lex.c
    ${AMELIE_ROOT}/compiler/parser/parse_expr.c
    ${AMELIE_ROOT}/compiler/parser/parse_if.c
    ${AMELIE_ROOT}/compiler/parser/parse_for.c
    ${AMELIE_ROOT}/compiler/parser/parse_while.c
    ${AMELIE_ROOT}/compiler/parser/parse_break.c
    ${AMELIE_ROOT}/compiler/parser/parse_show.c
    ${AMELIE_ROOT}/compiler/parser/parse_system.c
    ${AMELIE_ROOT}/compiler/parser/parse_checkpoint.c
    ${AMELIE_ROOT}/compiler/parser/parse_token.c
    ${AMELIE_ROOT}/compiler/parser/parse_replica.c
    ${AMELIE_ROOT}/compiler/parser/parse_repl.c
    ${AMELIE_ROOT}/compiler/parser/parse_watch.c
    ${AMELIE_ROOT}/compiler/parser/parse_lock.c
    ${AMELIE_ROOT}/compiler/parser/parse_subscription.c
    ${AMELIE_ROOT}/compiler/parser/parse_grant.c
    ${AMELIE_ROOT}/compiler/parser/parse_user.c
    ${AMELIE_ROOT}/compiler/parser/parse_table.c
    ${AMELIE_ROOT}/compiler/parser/parse_index.c
    ${AMELIE_ROOT}/compiler/parser/parse_clone.c
    ${AMELIE_ROOT}/compiler/parser/parse_function.c
    ${AMELIE_ROOT}/compiler/parser/parse_topic.c
    ${AMELIE_ROOT}/compiler/parser/parse_from.c
    ${AMELIE_ROOT}/compiler/parser/parse_returning.c
    ${AMELIE_ROOT}/compiler/parser/parse_value.c
    ${AMELIE_ROOT}/compiler/parser/parse_row.c
    ${AMELIE_ROOT}/compiler/parser/parse_insert.c
    ${AMELIE_ROOT}/compiler/parser/parse_update.c
    ${AMELIE_ROOT}/compiler/parser/parse_delete.c
    ${AMELIE_ROOT}/compiler/parser/parse_select.c
    ${AMELIE_ROOT}/compiler/parser/parse_matching.c
    ${AMELIE_ROOT}/compiler/parser/parse_declare.c
    ${AMELIE_ROOT}/compiler/parser/parse_with.c
    ${AMELIE_ROOT}/compiler/parser/parse_execute.c
    ${AMELIE_ROOT}/compiler/parser/parse_publish.c
    ${AMELIE_ROOT}/compiler/parser/parse.c
    ${AMELIE_ROOT}/compiler/parser/parse_import.c
    ${AMELIE_ROOT}/compiler/parser/parser.c
    ${AMELIE_ROOT}/compiler/plan/path.c
    ${AMELIE_ROOT}/compiler/plan/path_prepare.c
    ${AMELIE_ROOT}/compiler/plan/plan.c
    ${AMELIE_ROOT}/compiler/plan/plan_pushdown.c
    ${AMELIE_ROOT}/compiler/compiler/compiler.c
    ${AMELIE_ROOT}/compiler/compiler/explain.c
    ${AMELIE_ROOT}/compiler/compiler/cast.c
    ${AMELIE_ROOT}/compiler/compiler/scan.c
    ${AMELIE_ROOT}/compiler/compiler/emit_expr.c
    ${AMELIE_ROOT}/compiler/compiler/emit_insert.c
    ${AMELIE_ROOT}/compiler/compiler/emit_upsert.c
    ${AMELIE_ROOT}/compiler/compiler/emit_update.c
    ${AMELIE_ROOT}/compiler/compiler/emit_delete.c
    ${AMELIE_ROOT}/compiler/compiler/emit_select.c
    ${AMELIE_ROOT}/compiler/compiler/emit_matching.c
    ${AMELIE_ROOT}/compiler/compiler/emit_publish.c
    ${AMELIE_ROOT}/compiler/compiler/emit_watch.c
    ${AMELIE_ROOT}/compiler/compiler/emit_utility.c)

set(AMELIE_SOURCES_SYSTEM
    ${AMELIE_ROOT}/system/frontend/auth_cache.c
    ${AMELIE_ROOT}/system/frontend/auth.c
    ${AMELIE_ROOT}/system/frontend/api.c
    ${AMELIE_ROOT}/system/frontend/mcp.c
    ${AMELIE_ROOT}/system/frontend/mcp_execute.c
    ${AMELIE_ROOT}/system/frontend/frontend.c
    ${AMELIE_ROOT}/system/frontend/client.c
    ${AMELIE_ROOT}/system/frontend/subscriber.c
    ${AMELIE_ROOT}/system/frontend/player.c
    ${AMELIE_ROOT}/system/frontend/players.c
    ${AMELIE_ROOT}/system/backend/pod.c
    ${AMELIE_ROOT}/system/backend/backend.c
    ${AMELIE_ROOT}/system/session/session.c
    ${AMELIE_ROOT}/system/system/repo.c
    ${AMELIE_ROOT}/system/system/system.c
    ${AMELIE_ROOT}/system/system/system_metrics.c
    ${AMELIE_ROOT}/system/system/main.c
    ${AMELIE_ROOT}/system/api/amelie.c)

set(AMELIE_SOURCES
    ${AMELIE_SOURCES_RUNTIME}
    ${AMELIE_SOURCES_SERVER}
    ${AMELIE_SOURCES_DB}
    ${AMELIE_SOURCES_REPL}
    ${AMELIE_SOURCES_VM}
    ${AMELIE_SOURCES_COMPILER}
    ${AMELIE_SOURCES_SYSTEM})

set(AMELIE_SOURCES_MAIN
    ${AMELIE_ROOT}/main/main/bookmarks.c
    ${AMELIE_ROOT}/main/main/main.c
    ${AMELIE_ROOT}/main/main/main_cmd.c
    ${AMELIE_ROOT}/main/main/main_cli.c
    ${AMELIE_ROOT}/main/import/import.c
    ${AMELIE_ROOT}/main/import/import_main.c
    ${AMELIE_ROOT}/main/bench/bench.c
    ${AMELIE_ROOT}/main/bench/bench_insert.c
    ${AMELIE_ROOT}/main/bench/bench_upsert.c
    ${AMELIE_ROOT}/main/bench/bench_tpcb.c
    ${AMELIE_ROOT}/main/bench/bench_import.c
    ${AMELIE_ROOT}/main/bench/bench_decre.c
    ${AMELIE_ROOT}/main/bench/bench_vector.c
    ${AMELIE_ROOT}/main/bench/bench_pubsub.c
    ${AMELIE_ROOT}/main/bench/bench_main.c
    ${AMELIE_ROOT}/main/test/test_plan.c
    ${AMELIE_ROOT}/main/test/test_suite.c
    ${AMELIE_ROOT}/main/test/test_command.c
    ${AMELIE_ROOT}/main/test/test_main.c
    ${AMELIE_ROOT}/main/dst/dst_user.c
    ${AMELIE_ROOT}/main/dst/dst_stat.c
    ${AMELIE_ROOT}/main/dst/dst.c
    ${AMELIE_ROOT}/main/dst/dst_step.c
    ${AMELIE_ROOT}/main/dst/dst_validate.c
    ${AMELIE_ROOT}/main/dst/dst_main.c)

set(AMELIE_SOURCES_UNIT
    ${CMAKE_SOURCE_DIR}/tests/unit/test_task.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_exception.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_palloc.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_buf.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_format.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_coroutine.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_event.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_condition.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_ipc.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_rpc.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_cancel.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_sleep.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_benchmark.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_heap.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_lock.c
    ${CMAKE_SOURCE_DIR}/tests/unit/test_api.c)

# use fast math only for the vectorized processing
set_source_files_properties(
	${AMELIE_ROOT}/runtime/lib/vector.c
	${AMELIE_ROOT}/vm/set/matching.c
	PROPERTIES COMPILE_OPTIONS "-ffast-math"
)

# libamelie.a
add_library(amelie_static STATIC ${AMELIE_SOURCES})
set_target_properties(amelie_static PROPERTIES OUTPUT_NAME amelie)
set(AMELIE_STATIC_LINK "$<LINK_LIBRARY:WHOLE_ARCHIVE,amelie_static>")

# libamelie-unit.a
add_library(amelie_unit_static STATIC ${AMELIE_SOURCES_UNIT})
set_target_properties(amelie_unit_static PROPERTIES OUTPUT_NAME amelie_unit)
set(AMELIE_UNIT_STATIC_LINK "$<LINK_LIBRARY:WHOLE_ARCHIVE,amelie_unit_static>")

# libamelie.so
file(TOUCH libamelie_so.c)
add_library(amelie_so SHARED libamelie_so.c)
set_target_properties(amelie_so PROPERTIES OUTPUT_NAME amelie)
target_link_libraries(amelie_so ${AMELIE_STATIC_LINK})
target_link_libraries(amelie_so ${AMELIE_LIBRARIES})

# amelie
add_executable(amelie ${AMELIE_SOURCES_MAIN})
target_link_libraries(amelie ${AMELIE_STATIC_LINK})
target_link_libraries(amelie ${AMELIE_UNIT_STATIC_LINK})
target_link_libraries(amelie ${AMELIE_LIBRARIES})
set_target_properties(amelie PROPERTIES ENABLE_EXPORTS ON OUTPUT_NAME amelie)
