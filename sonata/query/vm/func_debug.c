
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_client.h>
#include <indigo_server.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>
#include <indigo_request.h>
#include <indigo_vm.h>

void
func_debug(Vm*       vm,
           Function* func,
           Value*    result,
           int       argc,
           Value**   argv)
{
	if (argc < 1)
		error("debug(): incorrect call");

	// name
	function_validate_arg(func, argv, 0, VALUE_STRING);
	argc--;
	argv++;

	// todo
	unused(vm);
	unused(result);

	error("debug(): unknown command");
}
