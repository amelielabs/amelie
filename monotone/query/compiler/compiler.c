
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

#if 0
hot static inline uint32_t
compiler_hash_key(Table* table, AstRow* row)
{
	auto schema = &table->config->schema;

	// validate column count
	if (row->list_count < schema->column_count)
		error("table <%.*s>: number of columns does not mismatch",
			  str_size(&table->config->name),
			  str_of(&table->config->name));

	// calculate key hash
	uint32_t hash = 0;

	auto arg    = row->list;
	auto column = schema->column;
	int  count  = 0;
	while (column)
	{
		if (column_is_key(column))
		{
			uint8_t* key;
			int      key_size;

			auto     expr = ast_expr_of(arg);
			if (column->type == TYPE_STRING)
			{
				if (expr->expr->id != KSTRING)
					error("key column <%.*s>: expected string type", str_size(&column->name),
					      str_of(&column->name));

				key = str_u8(&expr->expr->string);
				key_size = str_size(&expr->expr->string);
			} else
			{
				if (expr->expr->id != KINT)
					error("key column <%.*s>: expected int type", str_size(&column->name),
					      str_of(&column->name));

				key = (uint8_t*)&expr->expr->integer;
				key_size = sizeof(expr->expr->integer);
			}

			hash = hash_murmur3_32(key, key_size, hash);
			count++;
			if (count == schema->key_count)
				break;
		}

		column = column->next;
		arg    = arg->next;
	}

	return hash;
}

static void
compiler_insert(Compiler* self, Ast* ast)
{
	auto insert = ast_insert_of(ast);

	// find table, get schema
	auto table = table_mgr_find(&self->db->table_mgr, &insert->table->string, true);

	// foreach row
	auto i = insert->rows;
	for (; i; i = i->next)
	{
		auto row = ast_row_of(i);

		// get hash of the primary key
		uint32_t hash = compiler_hash_key(table, row);

		// get code based on the hash
		auto code = self->code_get(hash, self->code_get_arg);
		compiler_set_code(self, code);

		for (auto j = row->list; j; j = j->next)
		{
			// expr
			auto expr = ast_expr_of(j);
			emit_expr(self, NULL, expr->expr);

			// PUSH
			int r = rmap_pop(&self->map);
			op1(self, CPUSH, r);
			runpin(self, r);
		}

		// array
		int r = op2(self, CARRAY, rpin(self), row->list_count);
		op1(self, CPUSH, r);
		runpin(self, r);
	
		// CINSERT
		int name_offset;
		name_offset = code_add_string(code, &table->config->name);
		op3(self, CINSERT, name_offset, 1, insert->unique);
	}
}
#endif

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
#if 0
		switch (node->ast->id) {
		case KINSERT:
			compiler_insert(self, node->ast);
			break;
		case KSELECT:
		{
			auto select = ast_select_of(node->ast);
			emit_expr(self, NULL, select->expr);
			break;
		}
		default:
			assert(0);
			break;
		}
#endif
	}
}
