
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

void
fn_register(FunctionMgr* mgr)
{
	function_mgr_register(mgr, fn_system_def);
	function_mgr_register(mgr, fn_cast_def);
	function_mgr_register(mgr, fn_null_def);
	function_mgr_register(mgr, fn_object_def);
	function_mgr_register(mgr, fn_string_def);
	function_mgr_register(mgr, fn_regexp_def);
	/*
	function_mgr_register(mgr, fn_math_def);
	function_mgr_register(mgr, fn_misc_def);
	function_mgr_register(mgr, fn_time_def);
	function_mgr_register(mgr, fn_vector_def);
	*/
}
