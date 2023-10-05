
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

static void
compiler_insert(Compiler* self, Ast* ast)
{
	auto insert = ast_insert_of(ast);

	// find table, get schema

	// foreach row
	auto i = insert->rows;
	for (; i; i = i->next)
	{
		auto row = ast_row_of(i);

		// code = shard(key_list))
		//

		for (auto j = row->list; j; j = j->next)
		{
			// expr
			auto expr = ast_expr_of(j);

			// emit
			// push
		}
	
		// CINSERT
	}

	// insert
	/*
	bool unique = (cmd->type->id == IN_CMD_INSERT);
	int table_name_offset;
	table_name_offset = in_code_add_string(cmd->code, name.string, name.string_size);
	in_op3(cmd, IN_CINSERT, table_name_offset, count, unique);
	*/
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
compiler_generate(Compiler* self)
{
	// process statements
	auto node = self->parser.query.stmts.list;
	for (; node; node = node->next)
	{
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
	}
}
