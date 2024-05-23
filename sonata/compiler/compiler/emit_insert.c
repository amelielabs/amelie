
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
#include <sonata_auth.h>
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
#include <sonata_parser.h>
#include <sonata_semantic.h>
#include <sonata_compiler.h>

hot void
emit_insert(Compiler* self, Ast* ast)
{
	auto insert = ast_insert_of(ast);
	auto table = insert->target->table;

	// emit rows
	insert->rows_offset = code_data_offset(&self->code_data);

	// []
	encode_array(&self->code_data.data, insert->rows_count);
	auto i = insert->rows;
	for (; i; i = i->next)
	{
		auto row = ast_row_of(i);
		int data_size;
		emit_row(&self->code_data, row, &data_size);
	}

	// CINSERT
	op2(self, CINSERT, (intptr_t)table, insert->unique);
}
