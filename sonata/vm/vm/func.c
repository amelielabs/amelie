
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>

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
	value_to_string(result, argv[0]);
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
	value_to_json(result, argv[0]);
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
	auto buf = config_list(config());
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
		{ "public", "has",         (FunctionMain)func_has,        2 },
		{ "public", "set",         (FunctionMain)func_set,        3 },
		{ "public", "unset",       (FunctionMain)func_unset,      2 },
		{ "public", "sizeof",      (FunctionMain)func_sizeof,     1 },
		{ "public", "string",      (FunctionMain)func_string,     1 },
		{ "public", "json",        (FunctionMain)func_json,       1 },
		{ "public", "error",       (FunctionMain)func_error,      1 },
		// system
		{ "system", "config",      (FunctionMain)func_config,     0 },
		{ "system", "users",       (FunctionMain)func_users,      0 },
		{ "system", "replicas",    (FunctionMain)func_replicas,   0 },
		{ "system", "repl",        (FunctionMain)func_repl,       0 },
		{ "system", "replication", (FunctionMain)func_repl,       0 },
		{ "system", "nodes",       (FunctionMain)func_nodes,      0 },
		{ "system", "schemas",     (FunctionMain)func_schemas,    0 },
		{ "system", "functions",   (FunctionMain)func_functions,  0 },
		{ "system", "tables",      (FunctionMain)func_tables,     0 },
		{ "system", "views",       (FunctionMain)func_views,      0 },
		{ "system", "wal",         (FunctionMain)func_wal,        0 },
		{  NULL,     NULL,          NULL,                         0 }
	};
	for (int i = 0; def[i].name; i++)
	{
		auto func = function_allocate(def[i].schema, def[i].name,
		                              def[i].argc,
		                              def[i].function);
		function_mgr_add(mgr, func);
	}
}
