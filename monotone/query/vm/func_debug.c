
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
