
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

static int
emit_insert_store_rows(Compiler* self)
{
	auto stmt    = self->current;
	auto insert  = ast_insert_of(stmt->ast);
	auto table   = from_first(&insert->from)->from_table;
	auto columns = table_columns(table);

	// emit rows
	auto rset = op3pin(self, CSET, TYPE_STORE, columns->count, 0);
	for (auto row = insert->rows.list; row; row = row->next)
	{
		auto col = row->ast;
		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);
			auto type = emit_push(self, &insert->from, col);
			if (unlikely(type != TYPE_NULL && column->type != type))
				stmt_error(stmt, row->ast, "'{s}' expected for column '{str}",
				           type_of(column->type),
				           &column->name);
			col = col->next;
		}
		op1(self, CSET_ADD, rset);
	}
	return op2pin(self, CDUP, TYPE_STORE, rset);
}

static int
emit_insert_store(Compiler* self)
{
	auto stmt    = self->current;
	auto insert  = ast_insert_of(stmt->ast);
	auto target  = from_first(&insert->from);
	auto table   = target->from_table;
	auto columns = table_columns(table);

	// create or match a set to use with the insert operation
	int r;
	if (insert->select)
	{
		// use rows set returned from select
		auto columns_select = &ast_select_of(insert->select->ast)->ret.columns;
		if (! columns_compare(columns, columns_select))
			stmt_error(stmt, insert->select->ast, "SELECT columns must match the INSERT table");

		r = op2pin(self, CDUP, TYPE_STORE, insert->select->r);
	} else
	{
		if (insert->rows.count > 0)
			// use rows as expressions provided during parsing
			r = emit_insert_store_rows(self);
		else
			// use rows set created during parsing
			r = op2pin(self, CSET_PTR, TYPE_STORE, (intptr_t)insert->values);
	}

	return r;
}

hot static int
emit_upsert(Compiler* self, AstInsert* insert, int rstore, int refs_count)
{
	auto target = from_first(&insert->from);

	// create returning set
	int rset = -1;
	if (returning_has(&insert->ret))
		rset = op3pin(self, CSET, TYPE_STORE, insert->ret.count, 0);

	// CTABLE_PREPARE
	target->rcursor = op2pin(self, CTABLE_PREPARE, TYPE_CURSOR,
	                         (intptr_t)target->from_table);

	// jmp _start
	int jmp_start = op_pos(self);
	op1(self, CJMP, 0 /* _start */);

	// _where:
	int jmp_where = op_pos(self);

	// ON CONFLICT UPDATE or do nothing
	switch (insert->on_conflict) {
	case ON_CONFLICT_UPDATE:
	{
		// WHERE expression
		int jmp_where_jntr = 0;
		if (insert->update_where)
		{
			// expr
			int rexpr;
			rexpr = emit_expr(self, &insert->from, insert->update_where);

			// jntr _start
			jmp_where_jntr = op_pos(self);

			op2(self, CJNTR, 0 /* _start */, rexpr);
			runpin(self, rexpr);
		}

		// update
		emit_update_target(self, &insert->from, insert->update_expr);

		// set jntr _start to _start
		if (insert->update_where)
			op_at(self, jmp_where_jntr)->a = jmp_start;
		break;
	}
	case ON_CONFLICT_ERROR:
	{
		// find error()
		Str name;
		str_set(&name, "error", 5);
		auto func = functions_find(share()->functions, &name);
		assert(func);

		// call error()
		Str str;
		str_set(&str, "unique key constraint violation", 31);
		int r = emit_string(self, &str, false);
		op1(self, CPUSH, r);
		runpin(self, r);

		// CALL
		r = op4pin(self, CCALL, func->type, (intptr_t)func, 1, -1);
		runpin(self, r);
		break;
	}
	case ON_CONFLICT_NOTHING:
		// skip returning
		op1(self, CJMP, jmp_start);
		break;
	default:
		break;
	}

	// _returning:
	int jmp_returning = -1;

	// [RETURNING]
	if (returning_has(&insert->ret))
	{
		jmp_returning = op_pos(self);

		// push expr and set column type
		for (auto as = insert->ret.list; as; as = as->next)
		{
			auto column = as->r->column;
			// expr
			auto type = emit_push(self, &insert->from, as->l);
			column_set_type(column, type, type_sizeof(type));
		}

		// add to the returning set
		op1(self, CSET_ADD, rset);
	}

	// _start:

	// set jmp_start to _start
	op_at(self, jmp_start)->a = op_pos(self);

	// CUPSERT
	op5(self, CUPSERT, target->rcursor, jmp_where, jmp_returning,
	    rstore, refs_count);

	// close and free cursor
	target->rcursor = emit_free(self, target->rcursor);

	// returning set
	return rset;
}

hot int
emit_insert(Compiler* self, Ast* ast)
{
	auto insert = ast_insert_of(ast);

	// emit store
	auto rstore = emit_insert_store(self);

	// push references
	auto stmt = self->current;
	auto refs_count = stmt->refs.count;
	auto ref = stmt->refs.list;
	while (ref)
	{
		if (ref->ast) {
			int type;
			if (ref->ast->id == KVAR)
			{
				auto var = ref->ast->var;
				op3(self, CPUSH_VAR, var->order, var->is_arg, ref->not_null);
				type = var->type;
			} else
			{
				auto r = emit_expr(self, ref->from, ref->ast);
				op3(self, CPUSH_REF, r, 0, ref->not_null);
				type = rtype(self, r);
				runpin(self, r);
			}
			if (ref->column)
			{
				if (ref->column->type != type)
					stmt_error(stmt, ref->ast, "expected '{s}'", type_of(ref->column->type));
			}

		} else {
			op3(self, CPUSH_REF, ref->r, 1, ref->not_null);
		}
		ref = ref->next;
	}

	// upsert or insert
	int r = -1;
	if (insert->on_conflict != ON_CONFLICT_NONE)
	{
		r = emit_upsert(self, insert, rstore, refs_count);
	} else
	{
		// CINSERT
		auto target = from_first(&insert->from);
		op3(self, CINSERT, (intptr_t)target->from_table, rstore, refs_count);
	}
	runpin(self, rstore);
	return r;
}
