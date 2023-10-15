
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
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_compiler.h>

static inline int
emit_row(CodeData* data, AstRow* row, int* data_size)
{
	int start = code_data_pos(data);
	auto buf = &data->data;

	// []
	encode_array(buf, row->list_count);

	auto value = row->list;
	while (value)
	{
		auto expr = ast_value_of(value)->expr;
		while (expr)
		{
			switch (expr->id) {
			case '[':
				encode_array(buf, expr->integer);
				break;
			case '{':
				encode_map(buf, expr->integer);
				break;
			case KSTRING:
				encode_string(buf, &expr->string);
				break;
			case KNULL:
				encode_null(buf);
				break;
			case KTRUE:
			case KFALSE:
				encode_bool(buf, expr->id == KTRUE);
				break;
			case KINT:
				encode_integer(buf, expr->integer);
				break;
			case KFLOAT:
				encode_float(buf, expr->fp);
				break;
			default:
				abort();
				break;
			}

			expr = expr->next;
		}

		value = value->next;
	}

	*data_size = buf_size(buf) - start;
	return start;
}

static atomic_u32 seq = 0;

hot void
emit_insert(Compiler* self, Ast* ast)
{
	auto insert = ast_insert_of(ast);

	if (insert->generate)
	{
		int count = insert->generate_count->integer;
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

			// get the destination code
			uint32_t hash;
			hash = hash_murmur3_32((uint8_t*)&key, sizeof(key), 0);

			auto code = self->code_get(hash, self->code_get_arg);

			// CINSERT
			code_add(code, CINSERT, (intptr_t)insert->table, data, data_size,
			         insert->unique);
		}

		return;
	}

	// foreach row
	auto i = insert->rows;
	for (; i; i = i->next)
	{
		auto row = ast_row_of(i);

		// write row to the code data
		int data_size;
		int data = emit_row(&self->code_data, row, &data_size);

		// get the destination code
		auto code = self->code_get(row->hash, self->code_get_arg);

		// CINSERT
		code_add(code, CINSERT, (intptr_t)insert->table, data, data_size,
		         insert->unique);
	}
}
