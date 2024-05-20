
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
#include <indigo_parser.h>
#include <indigo_semantic.h>
#include <indigo_compiler.h>

hot void
emit_insert(Compiler* self, Ast* ast)
{
	auto stmt   = self->current;
	auto insert = ast_insert_of(ast);
	auto table  = insert->target->table;

	// foreach row generate insert
	auto i = insert->rows;
	for (; i; i = i->next)
	{
		auto row = ast_row_of(i);

		// write row to the code data
		int data_size;
		int data = emit_row(&self->code_data, row, &data_size);

		Route* route;
		if (table->config->reference)
		{
			// get route by order
			route = router_at(self->router, 0);

		} else
		{
			// validate row and hash keys
			auto hash = row_hash(table_def(table),
			                     code_data_at(&self->code_data, data),
			                     data_size);

			// get route by hash
			route = router_get(self->router, hash);
		}

		// get the request
		auto req = dispatch_add(self->dispatch, stmt->order, route);

		// CINSERT
		code_add(&req->code, CINSERT, (intptr_t)insert->target->table,
		         data,
		         data_size, insert->unique);
	}

	// CREADY
	for (int order = 0; order < self->dispatch->set_size; order++)
	{
		auto req = dispatch_at_stmt(self->dispatch, stmt->order, order);
		if (! req)
			continue;
		code_add(&req->code, CREADY, stmt->order, -1, 0, 0);
	}

	// coordinator
	compiler_switch(self, &self->code_coordinator);

	// CRECV
	op0(self, CRECV);
}
