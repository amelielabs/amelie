
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>

hot static void
func_has(Vm*       vm,
         Function* func,
         Value*    result,
         int       argc,
         Value**   argv)
{
	unused(vm);
	function_validate_argc(func, argc);
	value_idx_has(result, argv[0], argv[1]);
}

hot static void
func_set(Vm*       vm,
         Function* func,
         Value*    result,
         int       argc,
         Value**   argv)
{
	unused(vm);
	function_validate_argc(func, argc);
	value_idx_set(result, argv[0], argv[1], argv[2]);
}

hot static void
func_unset(Vm*       vm,
           Function* func,
           Value*    result,
           int       argc,
           Value**   argv)
{
	unused(vm);
	function_validate_argc(func, argc);
	value_idx_unset(result, argv[0], argv[1]);
}

hot static void
func_append(Vm*       vm,
            Function* func,
            Value*    result,
            int       argc,
            Value**   argv)
{
	unused(vm);
	function_validate_argc(func, argc);
	value_append(result, argv[0], argv[1]);
}

hot static void
func_sizeof(Vm*       vm,
            Function* func,
            Value*    result,
            int       argc,
            Value**   argv)
{
	unused(vm);
	function_validate_argc(func, argc);
	auto arg = argv[0];
	if (arg->type == VALUE_SET)
		value_set_int(result, ((Set*)arg->obj)->list_count);
	else
		value_sizeof(result, arg);
}

hot static void
func_string(Vm*       vm,
            Function* func,
            Value*    result,
            int       argc,
            Value**   argv)
{
	unused(vm);
	function_validate_argc(func, argc);
	value_to_string(result, argv[0], vm->local->timezone);
}

hot static void
func_json(Vm*       vm,
          Function* func,
          Value*    result,
          int       argc,
          Value**   argv)
{
	unused(vm);
	function_validate_argc(func, argc);
	value_to_json(result, argv[0], vm->local->timezone);
}

hot static void
func_interval(Vm*       vm,
              Function* func,
              Value*    result,
              int       argc,
              Value**   argv)
{
	unused(vm);
	function_validate_argc(func, argc);
	function_validate_arg(func, argv, 0, VALUE_STRING);
	Interval iv;
	interval_init(&iv);
	interval_read(&iv, &argv[0]->string);
	value_set_interval(result, &iv);
}

hot static void
func_timestamp(Vm*       vm,
               Function* func,
               Value*    result,
               int       argc,
               Value**   argv)
{
	unused(vm);
	function_validate_argc(func, argc);
	function_validate_arg(func, argv, 0, VALUE_STRING);
	Timestamp ts;
	timestamp_init(&ts);
	timestamp_read(&ts, &argv[0]->string);
	auto time = timestamp_of(&ts, vm->local->timezone);
	value_set_timestamp(result, time);
}

hot static void
func_generate_series(Vm*       vm,
                     Function* func,
                     Value*    result,
                     int       argc,
                     Value**   argv)
{
	unused(vm);
	function_validate_argc(func, argc);
	function_validate_arg(func, argv, 0, VALUE_TIMESTAMP);
	function_validate_arg(func, argv, 1, VALUE_TIMESTAMP);
	function_validate_arg(func, argv, 2, VALUE_INTERVAL);

	Timestamp ts;
	timestamp_init(&ts);

	// end
	timestamp_read_value(&ts, argv[1]->integer);
	uint64_t end = timestamp_of(&ts, NULL);

	// pos
	timestamp_init(&ts);
	timestamp_read_value(&ts, argv[0]->integer);
	uint64_t pos = timestamp_of(&ts, NULL);

	auto buf = buf_begin();
	encode_array(buf);
	while (pos <= end)
	{
		encode_timestamp(buf, pos);
		timestamp_add(&ts, &argv[2]->interval);
		pos = timestamp_of(&ts, NULL);
	}
	encode_array_end(buf);
	buf_end(buf);
	value_set_buf(result, buf);
}

hot static void
func_time_bucket(Vm*       vm,
                 Function* func,
                 Value*    result,
                 int       argc,
                 Value**   argv)
{
	unused(vm);
	function_validate_argc(func, argc);
	function_validate_arg(func, argv, 0, VALUE_INTERVAL);
	function_validate_arg(func, argv, 1, VALUE_TIMESTAMP);
	auto iv = &argv[0]->interval;
	if (iv->m != 0)
		error("time_bucket(): month intervals are not supported");
	uint64_t span = iv->us + iv->d * 86400000000ULL;
	uint64_t ts = argv[1]->integer / span * span;
	value_set_timestamp(result, ts);
}

hot static void
func_now(Vm*       vm,
         Function* func,
         Value*    result,
         int       argc,
         Value**   argv)
{
	unused(vm);
	unused(argv);
	function_validate_argc(func, argc);
	value_set_timestamp(result, time_us());
}

hot static void
func_error(Vm*       vm,
           Function* func,
           Value*    result,
           int       argc,
           Value**   argv)
{
	unused(vm);
	unused(result);
	function_validate_argc(func, argc);
	function_validate_arg(func, argv, 0, VALUE_STRING);
	auto arg = argv[0];
	error("%.*s", str_size(&arg->string), str_of(&arg->string));
}

static void
func_config(Vm*       vm,
            Function* func,
            Value*    result,
            int       argc,
            Value**   argv)
{
	unused(vm);
	unused(argv);
	function_validate_argc(func, argc);
	auto buf = config_list(config(), &vm->local->config);
	value_set_buf(result, buf);
}

static void
func_users(Vm*       vm,
           Function* func,
           Value*    result,
           int       argc,
           Value**   argv)
{
	unused(vm);
	unused(argv);
	function_validate_argc(func, argc);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_USERS, 1, &buf);
	value_set_buf(result, buf);
}

static void
func_replicas(Vm*       vm,
              Function* func,
              Value*    result,
              int       argc,
              Value**   argv)
{
	unused(vm);
	unused(argv);
	function_validate_argc(func, argc);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_REPLICAS, 1, &buf);
	value_set_buf(result, buf);
}

static void
func_repl(Vm*       vm,
          Function* func,
          Value*    result,
          int       argc,
          Value**   argv)
{
	unused(vm);
	unused(argv);
	function_validate_argc(func, argc);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_REPL, 1, &buf);
	value_set_buf(result, buf);
}

static void
func_nodes(Vm*       vm,
           Function* func,
           Value*    result,
           int       argc,
           Value**   argv)
{
	unused(vm);
	unused(argv);
	function_validate_argc(func, argc);
	Buf* buf;
	rpc(global()->control->system, RPC_SHOW_NODES, 1, &buf);
	value_set_buf(result, buf);
}

static void
func_schemas(Vm*       vm,
             Function* func,
             Value*    result,
             int       argc,
             Value**   argv)
{
	unused(argv);
	function_validate_argc(func, argc);
	auto buf = schema_mgr_list(&vm->db->schema_mgr);
	value_set_buf(result, buf);
}

static void
func_functions(Vm*       vm,
               Function* func,
               Value*    result,
               int       argc,
               Value**   argv)
{
	unused(argv);
	function_validate_argc(func, argc);
	auto buf = function_mgr_list(vm->function_mgr);
	value_set_buf(result, buf);
}

static void
func_tables(Vm*       vm,
            Function* func,
            Value*    result,
            int       argc,
            Value**   argv)
{
	unused(argv);
	function_validate_argc(func, argc);
	auto buf = table_mgr_list(&vm->db->table_mgr);
	value_set_buf(result, buf);
}

static void
func_views(Vm*       vm,
           Function* func,
           Value*    result,
           int       argc,
           Value**   argv)
{
	unused(argv);
	function_validate_argc(func, argc);
	auto buf = view_mgr_list(&vm->db->view_mgr);
	value_set_buf(result, buf);
}

static void
func_wal(Vm*       vm,
         Function* func,
         Value*    result,
         int       argc,
         Value**   argv)
{
	unused(argv);
	function_validate_argc(func, argc);
	// todo
	(void)result;
	(void)vm;
	error("todo: ");
}

void
func_setup(FunctionMgr* mgr)
{
	struct
	{
		const char*  schema;
		const char*  name;
		FunctionMain function;
		int          argc;
	} def[] =
	{
		// public
		{ "public", "has",             (FunctionMain)func_has,             2 },
		{ "public", "set",             (FunctionMain)func_set,             3 },
		{ "public", "unset",           (FunctionMain)func_unset,           2 },
		{ "public", "append",          (FunctionMain)func_append,          2 },
		{ "public", "sizeof",          (FunctionMain)func_sizeof,          1 },
		{ "public", "string",          (FunctionMain)func_string,          1 },
		{ "public", "json",            (FunctionMain)func_json,            1 },
		{ "public", "interval",        (FunctionMain)func_interval,        1 },
		{ "public", "timestamp",       (FunctionMain)func_timestamp,       1 },
		{ "public", "generate_series", (FunctionMain)func_generate_series, 3 },
		{ "public", "time_bucket",     (FunctionMain)func_time_bucket,     2 },
		{ "public", "now",             (FunctionMain)func_now,             0 },
		{ "public", "error",           (FunctionMain)func_error,           1 },
		// system
		{ "system", "config",          (FunctionMain)func_config,          0 },
		{ "system", "users",           (FunctionMain)func_users,           0 },
		{ "system", "replicas",        (FunctionMain)func_replicas,        0 },
		{ "system", "repl",            (FunctionMain)func_repl,            0 },
		{ "system", "replication",     (FunctionMain)func_repl,            0 },
		{ "system", "nodes",           (FunctionMain)func_nodes,           0 },
		{ "system", "schemas",         (FunctionMain)func_schemas,         0 },
		{ "system", "functions",       (FunctionMain)func_functions,       0 },
		{ "system", "tables",          (FunctionMain)func_tables,          0 },
		{ "system", "views",           (FunctionMain)func_views,           0 },
		{ "system", "wal",             (FunctionMain)func_wal,             0 },
		{  NULL,     NULL,              NULL,                              0 }
	};
	for (int i = 0; def[i].name; i++)
	{
		auto func = function_allocate(def[i].schema, def[i].name,
		                              def[i].argc,
		                              def[i].function);
		function_mgr_add(mgr, func);
	}
}
