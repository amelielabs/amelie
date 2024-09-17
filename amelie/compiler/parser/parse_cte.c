
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

static void
parse_cte_args(Stmt* self, const char* clause)
{
	// )
	if (stmt_if(self, ')'))
		return;

	for (;;)
	{
		// name
		auto name = stmt_if(self, KNAME);
		if (! name)
			error("%s name (<name> expected", clause);

		// ensure arg does not exists
		auto arg = columns_find(&self->columns, &name->string);
		if (arg)
			error("<%.*s> cte argument redefined", str_size(&name->string),
			      str_of(&name->string));

		// add argument
		arg = column_allocate();
		columns_add(&self->columns, arg);
		column_set_name(arg, &name->string);

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// )
	if (! stmt_if(self, ')'))
		error("%s name (<)> expected", clause);
}

void
parse_cte(Stmt* self, bool with)
{
	const char* clause = with? "WITH": "INTO";
	if (self->name)
		error("%s CTE redefined", clause);

	// name [(args)]
	self->name = stmt_if(self, KNAME);
	if (! self->name)
		error("%s <name> expected", clause);

	// (args)
	if (stmt_if(self, '('))
		parse_cte_args(self, clause);
}
