
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_client.h>
#include <monotone_server.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_snapshot.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>

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
	value_set_data_from(result, buf);
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
	rpc(global()->control->core, RPC_USER_SHOW, 1, &buf);
	value_set_data_from(result, buf);
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
	value_set_data_from(result, buf);
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
	value_set_data_from(result, buf);
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
	value_set_data_from(result, buf);
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
	value_set_data_from(result, buf);
}

static void
func_storages(Vm*       vm,
              Function* func,
              Value*    result,
              int       argc,
              Value**   argv)
{
	unused(argv);
	function_validate_argc(func, argc);
	auto buf = storage_mgr_list(&vm->db->storage_mgr);
	value_set_data_from(result, buf);
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
	auto buf = wal_status(&vm->db->wal);
	value_set_data_from(result, buf);
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
		{ "public", "has",       (FunctionMain)func_has,       2 },
		{ "public", "set",       (FunctionMain)func_set,       3 },
		{ "public", "unset",     (FunctionMain)func_unset,     2 },
		{ "public", "sizeof",    (FunctionMain)func_sizeof,    1 },
		{ "public", "string",    (FunctionMain)func_string,    1 },
		{ "public", "json",      (FunctionMain)func_json,      1 },
		// system
		{ "system", "config",    (FunctionMain)func_config,    0 },
		{ "system", "users",     (FunctionMain)func_users,     0 },
		{ "system", "schemas",   (FunctionMain)func_schemas,   0 },
		{ "system", "functions", (FunctionMain)func_functions, 0 },
		{ "system", "tables",    (FunctionMain)func_tables,    0 },
		{ "system", "views",     (FunctionMain)func_views,     0 },
		{ "system", "storages",  (FunctionMain)func_storages,  0 },
		{ "system", "wal",       (FunctionMain)func_wal,       0 },
		{ "system", "debug",     (FunctionMain)func_debug,     0 },
		{  NULL,    NULL,        NULL,                         0 }
	};
	for (int i = 0; def[i].name; i++)
	{
		auto func = function_allocate(def[i].schema, def[i].name,
		                              def[i].argc,
		                              def[i].function);
		function_mgr_add(mgr, func);
	}
}
