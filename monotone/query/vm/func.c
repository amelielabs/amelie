
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
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_vm.h>

static void
func_mn_config(Vm*       vm,
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
func_mn_tables(Vm*       vm,
               Function* func,
               Value*    result,
               int       argc,
               Value**   argv)
{
	unused(argv);
	function_validate_argc(func, argc);
	if (unlikely(! transaction_active(vm->trx)))
		error("tables(): not in transaction");
	auto buf = table_mgr_list(&vm->db->table_mgr);
	value_set_data_from(result, buf);
}

static void
func_mn_views(Vm*       vm,
              Function* func,
              Value*    result,
              int       argc,
              Value**   argv)
{
	unused(argv);
	function_validate_argc(func, argc);
	if (unlikely(! transaction_active(vm->trx)))
		error("views(): not in transaction");
	auto buf = meta_mgr_list(&vm->db->meta_mgr);
	value_set_data_from(result, buf);
}

static void
func_mn_wal(Vm*       vm,
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
func_mn_setup(FunctionMgr* mgr)
{
	// config
	auto func = function_allocate("mn_config", 0, (FunctionMain)func_mn_config);
	function_mgr_add(mgr, func);

	// tables
	func = function_allocate("mn_tables", 0, (FunctionMain)func_mn_tables);
	function_mgr_add(mgr, func);

	// views
	func = function_allocate("mn_views", 0, (FunctionMain)func_mn_views);
	function_mgr_add(mgr, func);

	// wal
	func = function_allocate("mn_wal", 0, (FunctionMain)func_mn_wal);
	function_mgr_add(mgr, func);

	// debug
	func = function_allocate("mn_debug", 0, (FunctionMain)func_mn_debug);
	function_mgr_add(mgr, func);
}
