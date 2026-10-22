
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_copy.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

void
emit_close(Compiler* self, Stmt* stmt)
{
	// early release control for cross tenant transactions
	if (! access_multi_user(&self->program->access))
		return;

	// mark last sending operation in the main block
	auto block = compiler_main(self);
	if (stmt->block == block && block->stmts.last_send == stmt)
		op0(self, CCLOSE);
}

hot static void
emit_into(Compiler* self, Stmt* stmt)
{
	// INTO var, ... and := operator
	auto ret = stmt->ret;
	if (! ret)
		stmt_error(stmt, NULL, "statement cannot be assigned");
	if (! ret->count_into)
		return;

	assert(stmt->r != -1);

	auto var = stmt->ret->list_into->var;
	if (var->type == TYPE_STORE)
	{
		// INTO table_var

		// ensure only one table var used
		if (ret->count_into > 1)
			stmt_error(self->current, stmt->ast, "INTO accept only one table variable");

		// compare columns
		auto type = rtype(self, stmt->r);
		if (type != TYPE_NULL && !columns_compare(&var->columns, &ret->columns))
			stmt_error(self->current, stmt->ast, "variable table columns mismatch");

		// CVAR_MOV (var = store)
		op3(self, CVAR_MOV, var->order, var->is_arg, stmt->r);

		runpin(self, stmt->r);
		stmt->r = -1;
		return;
	}

	// INTO var, ...
	auto ref = stmt->ret->list_into;
	list_foreach(&ret->columns.list)
	{
		auto column = list_at(Column, link);

		// all variables must be resolved at this point
		auto var = ref->var;
		if (var->type == TYPE_STORE)
			stmt_error(self->current, stmt->ast, "INTO accept only one table variable");

		auto type = column->type;
		if (type != TYPE_NULL && var->type != type)
			stmt_error(self->current, stmt->ast, "variable expected {s}",
					   type_of(var->type));

		// CVAR_SET
		op4(self, CVAR_SET, var->order, var->is_arg, stmt->r, column->order);

		// variables count can be less than expressions
		ref = ref->next;
		if (! ref)
			break;
	}

	// CFREE
	stmt->r = emit_free(self, stmt->r);
}

hot static void
emit_end(Compiler* self, Block* block)
{
	// ensure all statement freed
	auto stmt = block->stmts.list;
	for (; stmt; stmt = stmt->next)
	{
		if (stmt->r != -1 && !stmt->is_return)
			stmt->r = emit_free(self, stmt->r);
	}
}

hot void
emit_block(Compiler* self, Block* block)
{
	auto stmt_prev = self->current;
	self->current = NULL;

	// emit statements in the block
	auto stmt = block->stmts.list;
	for (; stmt; stmt = stmt->next)
	{
		self->current = stmt;

		// free all statement in the block before executing
		// block with break/continue statement
		if (stmt->is_break)
			emit_end(self, block);

		switch (stmt->id) {
		case STMT_IF:
			emit_if(self, stmt);
			break;
		case STMT_FOR:
			emit_for(self, stmt);
			break;
		case STMT_WHILE:
			emit_while(self, stmt);
			break;
		case STMT_BREAK:
		case STMT_CONTINUE:
			emit_break(self, stmt);
			break;
		case STMT_RETURN:
			break;
		case STMT_WATCH:
			emit_watch(self, stmt->ast);
			break;
		case STMT_INSERT:
			stmt->r = emit_insert(self, stmt->ast);
			break;
		case STMT_UPDATE:
			stmt->r = emit_update(self, stmt->ast);
			break;
		case STMT_DELETE:
			stmt->r = emit_delete(self, stmt->ast);
			break;
		case STMT_MATCHING:
			stmt->r = emit_matching(self, stmt->ast);
			break;
		case STMT_SELECT:
			stmt->r = emit_select(self, stmt->ast, false);
			break;
		default:
			abort();
			break;
		}

		// set cte columns types
		if (stmt->cte_name)
			columns_copy_types(&stmt->cte_columns, &stmt->ret->columns);

		// INTO and := (only for expressions)
		if (stmt->ret && stmt->ret->count_into > 0)
			emit_into(self, stmt);

		emit_close(self, stmt);

		// RETURN var | stmt
		if (stmt->is_return)
			emit_return(self, stmt);
	}

	// cleanup block (not main)
	if (block != compiler_main(self))
	{
		auto last = block->stmts.list_tail;
		if (last && !last->is_return && !last->is_break)
			emit_end(self, block);
	}

	// set previous stmt
	self->current = stmt_prev;
}

hot void
emit(Compiler* self)
{
	auto main = compiler_namespace(self);
	assert(main);

	// initialize declared variables (skipping arguments)
	auto vars = 0;
	for (auto var = main->vars.list; var; var = var->next)
		if (! var->is_arg)
			vars++;
	if (vars > 0)
		op1(self, CPUSH_NULLS, vars);

	// emit main block
	emit_block(self, main->blocks.list);

#if 0
	// mark last sending operation in the main block
	auto block = compiler_main(self);
	if (stmt->block == block && block->stmts.last_send == stmt)
		program->send_last = op_pos(self) - 1;
#endif
}
