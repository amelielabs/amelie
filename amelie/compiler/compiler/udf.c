
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
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>

static void
udf_if_prepare(Udf* self)
{
	UdfContext* context = self->iface_arg;
	assert(! self->context);

	// prepare local context
	Local local;
	local_init(&local, global());
	local_update_time(&local);
	guard(&local_free, &local);

	// parse statements
	Compiler compiler;
	compiler_init(&compiler, context->db, context->function_mgr);
	guard(compiler_free, &compiler);
	compiler_parse(&compiler, &local, &self->config->columns,
	               &self->config->text);

	// ensure function has no utility/ddl commands
	auto stmt = compiler_stmt(&compiler);
	if (stmt && stmt_is_utility(stmt))
		error("functions cannot contain utility commands");

	// generate bytecode
	compiler_emit(&compiler);

	// prepare program
	Program  program;
	compiler_program(&compiler, &program);

	// create executable context and copy program
	auto ea = executable_allocate();
	executable_set(ea, &program);
	self->context = ea;
}

static void
udf_if_free(Udf* self)
{
	if (self->context)
	{
		executable_free(self->context);
		self->context = NULL;
	}
}

UdfIf udf_if =
{
	.prepare = udf_if_prepare,
	.free    = udf_if_free
};
