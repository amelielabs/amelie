
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
emit_row(Code* code, AstRow* row, int* data_size)
{
	int start = code_data_prepare(code);

	// []
	encode_array(&code->data, row->list_count);

	auto value = row->list;
	while (value)
	{
		auto expr = ast_value_of(value)->expr;
		while (expr)
		{
			switch (expr->id) {
			case '[':
				encode_array(&code->data, expr->integer);
				break;
			case '{':
				encode_map(&code->data, expr->integer);
				break;
			case KSTRING:
				encode_string(&code->data, &expr->string);
				break;
			case KNULL:
				encode_null(&code->data);
				break;
			case KTRUE:
			case KFALSE:
				encode_bool(&code->data, expr->id == KTRUE);
				break;
			case KINT:
				encode_integer(&code->data, expr->integer);
				break;
			case KFLOAT:
				encode_float(&code->data, expr->fp);
				break;
			default:
				abort();
				break;
			}

			expr = expr->next;
		}

		value = value->next;
	}

	code_data_update(code);
	*data_size = buf_size(&code->data) - start;
	return start;
}

static atomic_u32 seq = 0;

hot static void
compiler_insert(Compiler* self, Ast* ast)
{
	auto insert = ast_insert_of(ast);

	if (insert->generate)
	{
		int count = insert->generate_count->integer;
		for (int i = 0; i < count; i++)
		{
			int key = atomic_u32_inc(&seq);
			uint32_t hash;
			hash = hash_murmur3_32((uint8_t*)&key, sizeof(key), 0);

			// (batch)
			int count_batch = insert->generate_batch->integer;

			auto code = self->code_get(hash, self->code_get_arg);
			int data = code_data_prepare(code);

			encode_array(&code->data, count_batch);
			for (int j = 0; j < count_batch; j++)
				encode_integer(&code->data, key);

			code_data_update(code);
			int data_size = buf_size(&code->data) - data;

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

		// get code based on the hash
		auto code = self->code_get(row->hash, self->code_get_arg);
		int data_size;
		int data = emit_row(code, row, &data_size);

		// CINSERT
		code_add(code, CINSERT, (intptr_t)insert->table, data, data_size,
		         insert->unique);
	}
}

bool
compiler_is_utility(Compiler* self)
{
	auto query = &self->parser.query;
	if (query->stmts.count != 1)
		return false;
	auto node = query->stmts.list;
	switch (node->ast->id) {
	case KSHOW:
	case KSET:
	case KCREATE_USER:
	case KCREATE_TABLE:
	case KDROP_USER:
	case KDROP_TABLE:
		return true;
	}
	return false;
}

void
compiler_parse(Compiler* self, Str* text)
{
	parser_run(&self->parser, text);
}

void
compiler_generate(Compiler* self, CompilerCode code_get, void* code_get_arg)
{
	self->code_get     = code_get;
	self->code_get_arg = code_get_arg;

	// process statements
	auto node = self->parser.query.stmts.list;
	for (; node; node = node->next)
	{
		switch (node->ast->id) {
		case KINSERT:
			compiler_insert(self, node->ast);
			break;
		case KSELECT:
			break;
		default:
			assert(0);
			break;
		}
	}
}
