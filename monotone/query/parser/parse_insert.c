
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

static inline void
parse_value_add(AstStack* stack)
{
	if (ast_head(stack))
		ast_head(stack)->integer++;
}

hot static inline AstValue*
parse_value(Stmt* self)
{
	AstStack stack;
	ast_stack_init(&stack);

	auto value = ast_value_allocate();
	Ast* last  = NULL;
	bool done  = false;
	while (! done)
	{
		auto ast = stmt_next(self);
		switch (ast->id) {
		case '[':
			parse_value_add(&stack);
			ast_push(&stack, ast);
			break;
		case ']':
		{
			auto head = ast_head(&stack);
			if (!head || head->id != '[')
				error("expected ']'");
			ast_pop(&stack);
			continue;
		}
		case '{':
			parse_value_add(&stack);
			ast_push(&stack, ast);
			break;
		case '}':
		{
			auto head = ast_head(&stack);
			if (!head || head->id != '{')
				error("expected '}'");
			ast_pop(&stack);
			continue;
		}
		case ':':
		case ',':
			// todo
			continue;
		case KSTRING:
			parse_value_add(&stack);
			break;
		case KNULL:
			parse_value_add(&stack);
			break;
		case KTRUE:
		case KFALSE:
			parse_value_add(&stack);
			break;
		case KINT:
			parse_value_add(&stack);
			break;
		case KREAL:
			parse_value_add(&stack);
			break;
		default:
			done = true;
			stmt_push(self, ast);
			continue;
		}

		if (value->expr == NULL)
			value->expr = ast;
		else
			last->next = ast;
		last = ast;
	}

	if (unlikely(ast_head(&stack)))
		error("bad row value");

	return value;
}

hot static inline AstRow*
parse_row(Stmt* self)
{
	// (value, ...)

	// (
	if (! stmt_if(self, '('))
		error("<(> expected");

	auto row  = ast_row_allocate();
	Ast* last = NULL;
	for (;;)
	{
		// value
		auto value = parse_value(self);
		if (unlikely(value->expr == NULL))
			error("bad row value");

		if (row->list == NULL)
			row->list = &value->ast;
		else
			last->next = &value->ast;
		last = &value->ast;
		row->list_count++;

		// ,
		if (stmt_if(self, ','))
			continue;

		// )
		if (! stmt_if(self, ')'))
			error("expected ')'");
		break;
	}

	return row;
}

hot void
parse_insert(Stmt* self, bool unique)
{
	// [INSERT|REPLACE] INTO name VALUES (value, ..), ...
	auto stmt = ast_insert_allocate();
	self->ast = &stmt->ast;

	// insert
	stmt->unique = unique;

	// INTO
	if (! stmt_if(self, KINTO))
		error("INSERT <INTO> expected");

	// name
	// schema.name
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		error("INSERT INTO <name> expected");
	stmt->table = table_mgr_find(&self->db->table_mgr, &schema, &name, true);

	// GENERATE
	if (stmt_if(self, KGENERATE))
	{
		stmt->generate = true;

		// count
		stmt->generate_count = stmt_if(self, KINT);
		if (! stmt->generate_count)
			error("GENERATE <count> expected");

		// batch
		stmt->generate_batch = stmt_if(self, KINT);
		if (! stmt->generate_batch)
			error("GENERATE count <batch count> expected");

		return;
	}

	// VALUES
	if (! stmt_if(self, KVALUES))
		error("INSERT INTO name <VALUES> expected");

	// row, ...
	Ast* last = NULL;
	for (;;)
	{
		auto row = parse_row(self);
		if (row == NULL)
			break;

		if (stmt->rows == NULL)
			stmt->rows = &row->ast;
		else
			last->next = &row->ast;
		last = &row->ast;
		stmt->rows_count++;

		if (stmt_if(self, ','))
			continue;
		break;
	}
}
