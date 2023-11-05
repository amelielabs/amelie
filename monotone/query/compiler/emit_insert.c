
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

static atomic_u32 seq = 0;

hot void
emit_insert_generate(Compiler* self, AstInsert* insert)
{
	auto stmt = self->current;
	int  count = insert->generate_count->integer;
	for (int i = 0; i < count; i++)
	{
		int key = atomic_u32_inc(&seq);

		// (batch)
		int  data = code_data_pos(&self->code_data);
		auto buf = &self->code_data.data;
		int  count_batch = insert->generate_batch->integer;
		encode_array(buf, count_batch);
		for (int j = 0; j < count_batch; j++)
			encode_integer(buf, key);
		int data_size = buf_size(buf) - data;

		// get the request code
		uint32_t hash;
		hash = hash_murmur3_32((uint8_t*)&key, sizeof(key), 0);

		// get route by hash
		auto route = router_get(self->router, hash);

		// get the request code
		auto code = &dispatch_add(self->dispatch, stmt->order, route)->code;

		// CINSERT
		code_add(code, CINSERT, (intptr_t)insert->target->table, data, data_size,
		         insert->unique);
	}
}

hot void
emit_insert(Compiler* self, Ast* ast)
{
	auto stmt = self->current;
	auto insert = ast_insert_of(ast);

	// GENERATE
	if (insert->generate)
	{
		emit_insert_generate(self, insert);
		return;
	}

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

		// get route by hash
		auto route = router_get(self->router, hash);

		// get the request
		auto req = dispatch_add(self->dispatch, stmt->order, route);

		// CINSERT
		code_add(&req->code, CINSERT, (intptr_t)insert->target->table,
		         data,
		         data_size, insert->unique);
	}
}
