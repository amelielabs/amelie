
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
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_compiler.h>

hot void
emit_upsert(Compiler* self, Ast* ast)
{
	auto insert = ast_insert_of(ast);
	(void)self;
	(void)insert;

#if 0
	// foreach row
	auto def = table_def(insert->target->table);
	auto i = insert->rows;
	for (; i; i = i->next)
	{
		auto row = ast_row_of(i);

		// write row to the code data
		int data_size;
		int data = emit_row(&self->code_data, row, &data_size);

		// validate row and hash keys
		auto hash = row_hash(def, code_data_at(&self->code_data, data), data_size);

		// get the request code
		auto code = &dispatch_map(self->dispatch, hash, self->current->order)->code;

		// CINSERT
		code_add(code, CINSERT, (intptr_t)insert->target->table,
		         data,
		         data_size, insert->unique);
	}
#endif
}
