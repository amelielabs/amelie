
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

hot static void
compiler_select(Compiler* self, Ast* ast)
{
	auto select = ast_insert_of(ast);

	(void)select;
	(void)self;
}

bool
compiler_is_utility(Compiler* self)
{
	if (self->parser.stmts_count != 1)
		return false;

	auto stmt = container_of(self->parser.stmts.next, Stmt, link);
	switch (stmt->id) {
	case STMT_SHOW:
	case STMT_SET:
	case STMT_CREATE_USER:
	case STMT_CREATE_TABLE:
	case STMT_DROP_USER:
	case STMT_DROP_TABLE:
	case STMT_ALTER_USER:
	case STMT_ALTER_TABLE:
		return true;
	default:
		break;
	}
	return false;
}

void
compiler_parse(Compiler* self, Str* text)
{
	parse(&self->parser, text);
}

void
compiler_generate(Compiler* self, CompilerCode code_get, void* code_get_arg)
{
	self->code_get     = code_get;
	self->code_get_arg = code_get_arg;

	// process statements
	list_foreach(&self->parser.stmts)
	{
		auto stmt = list_at(Stmt, link);
		switch (stmt->id) {
		case STMT_INSERT:
			compiler_insert(self, stmt->ast);
			break;
		case STMT_SELECT:
			compiler_select(self, stmt->ast);
			break;
		default:
			assert(0);
			break;
		}

	}
}
