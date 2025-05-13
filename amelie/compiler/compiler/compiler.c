
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_compiler.h>

void
compiler_init(Compiler*    self,
              Db*          db,
              Local*       local,
              FunctionMgr* function_mgr,
              Reg*         regs)
{
	self->program   = program_allocate();
	self->code      = &self->program->code;
	self->code_data = &self->program->code_data;
	self->args      = NULL;
	self->regs      = regs;
	self->current   = NULL;
	self->last      = NULL;
	self->db        = db;
	set_cache_init(&self->values_cache);
	parser_init(&self->parser, db, local, function_mgr, &self->values_cache,
	             self->program, regs);
	rmap_init(&self->map);
}

void
compiler_free(Compiler* self)
{
	if (self->program)
		program_free(self->program);
	parser_free(&self->parser);
	set_cache_free(&self->values_cache);
	rmap_free(&self->map);
}

void
compiler_reset(Compiler* self)
{
	self->code    = &self->program->code;
	self->args    = NULL;
	self->current = NULL;
	self->last    = NULL;
	program_reset(self->program);
	parser_reset(&self->parser);
	rmap_reset(&self->map);
}

void
compiler_parse(Compiler* self, Str* text)
{
	parse(&self->parser, text);
}

void
compiler_parse_import(Compiler*    self, Str* text, Str* uri,
                      EndpointType type)
{
	parse_import(&self->parser, text, uri, type);
}

static void
emit_stmt(Compiler* self)
{
	// generate backend code (pushdown)
	auto stmt = self->current;
	switch (stmt->id) {
	case STMT_INSERT:
	{
		auto insert = ast_insert_of(stmt->ast);
		if (insert->on_conflict == ON_CONFLICT_NONE)
			emit_insert(self, stmt->ast);
		else
			emit_upsert(self, stmt->ast);
		break;
	}
	case STMT_UPDATE:
	{
		emit_update(self, stmt->ast);
		break;
	}

	case STMT_DELETE:
	{
		emit_delete(self, stmt->ast);
		break;
	}

	case STMT_SELECT:
	{
		// select from table, ...
		auto select = ast_select_of(stmt->ast);
		if (select->pushdown)
		{
			// table direct scan or join
			//
			// execute on one or more backends, process the result on frontend
			//
			pushdown(self, stmt->ast);
			break;
		}

		// select (select from table)
		// select expr
		//
		// do nothing (frontend only)
		return;
	}

	case STMT_CALL:
		// do nothing
		return;

	case STMT_WATCH:
		// do nothing (frontend only)
		return;

	default:
		abort();
		break;
	}

	// set last statement which uses a table,
	// set snapshot if two or more stmts are using tables
	if (self->last)
		self->program->snapshot = true;
	self->last = self->current;

	// CRET
	op0(self, CRET);
}

static inline void
emit_send_generated_on_match(Compiler* self, Targets* targets, void* arg)
{
	AstInsert* insert = arg;
	auto target = targets_outer(&insert->targets_generated);

	// generate and push to the stack each generated column expression
	auto count = 0;
	auto op = insert->generated_columns;
	for (; op; op = op->next)
	{
		auto column = op->l->column;

		// push column order
		int rexpr = op2(self, CINT, rpin(self, TYPE_INT), column->order);
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);

		// push expr
		rexpr = emit_expr(self, targets, op->r);
		int type = rtype(self, rexpr);

		// ensure that the expression type is compatible
		// with the column
		if (unlikely(type != TYPE_NULL && column->type != type))
			stmt_error(self->current, &insert->ast,
			           "column '%.*s.%.*s' generated expression type '%s' does not match column type '%s'",
			           str_size(&target->name), str_of(&target->name),
			           str_size(&column->name), str_of(&column->name),
			           type_of(type),
			           type_of(column->type));

		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
		count++;
	}

	// CUPDATE_STORE
	op2(self, CUPDATE_STORE, target->id, count);
}

static inline int
emit_send_insert_rows(Compiler* self)
{
	auto stmt    = self->current;
	auto insert  = ast_insert_of(stmt->ast);
	auto table   = targets_outer(&insert->targets)->from_table;
	auto columns = table_columns(table);

	// emit rows
	auto rset = op3(self, CSET, rpin(self, TYPE_STORE), columns->count, 0);
	for (auto row = insert->rows.list; row; row = row->next)
	{
		auto col = row->ast;
		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);
			int rexpr   = emit_expr(self, &insert->targets, col);
			int type    = rtype(self, rexpr);
			if (unlikely(type != TYPE_NULL && column->type != type))
				stmt_error(stmt, row->ast, "'%s' expected for column '%.*s'",
						   type_of(column->type),
						   str_size(&column->name), str_of(&column->name));
			op1(self, CPUSH, rexpr);
			runpin(self, rexpr);
			col = col->next;
		}
		op1(self, CSET_ADD, rset);
	}
	return op2(self, CDUP, rpin(self, TYPE_STORE), rset);
}

static inline void
emit_send_insert(Compiler* self, int start)
{
	auto stmt    = self->current;
	auto insert  = ast_insert_of(stmt->ast);
	auto table   = targets_outer(&insert->targets)->from_table;
	auto columns = table_columns(table);

	// get values
	int r;
	if (insert->select)
	{
		// use rows set returned from select
		auto columns_select = &ast_select_of(insert->select->ast)->ret.columns;
		if (! columns_compare(columns, columns_select))
			stmt_error(stmt, insert->select->ast, "SELECT columns must match the INSERT table");
		r = op2(self, CDUP, rpin(self, TYPE_STORE), insert->select->r);
	} else
	{
		if (insert->rows.count > 0)
			// use rows as expressions provided during parsing
			r = emit_send_insert_rows(self);
		else
			// use rows set created during parsing
			r = op2(self, CSET_PTR, rpin(self, TYPE_STORE), (intptr_t)insert->values);
	}

	// scan over insert values to generate and apply stored columns
	if (columns->count_stored > 0)
	{
		// store_open( rvalues )
		auto values_dup = op2(self, CDUP, rpin(self, TYPE_STORE), r);
		targets_outer(&insert->targets_generated)->r = values_dup;
		scan(self, &insert->targets_generated,
		     NULL,
		     NULL,
		     NULL,
		     emit_send_generated_on_match,
		     insert);
		runpin(self, values_dup);
	}

	// CSEND_SHARD
	op3(self, CSEND_SHARD, start, (intptr_t)table, r);
	runpin(self, r);
}

static inline void
emit_send(Compiler* self, int start)
{
	// generate frontend send code
	Target* target = NULL;
	auto stmt = self->current;
	switch (stmt->id) {
	case STMT_INSERT:
	{
		emit_send_insert(self, start);
		self->program->sends++;
		return;
	}

	case STMT_UPDATE:
	{
		target = targets_outer(&ast_update_of(stmt->ast)->targets);
		break;
	}

	case STMT_DELETE:
	{
		target = targets_outer(&ast_delete_of(stmt->ast)->targets);
		break;
	}

	case STMT_SELECT:
	{
		// select from table, ...
		auto select = ast_select_of(stmt->ast);
		if (select->pushdown)
		{
			target = select->pushdown;
			break;
		}

		// select (select from table)
		// select expr
		//
		// do nothing (frontend only)
		//
		return;
	}

	case STMT_CALL:
		// do nothing
		return;

	case STMT_WATCH:
		// no targets
		return;

	default:
		abort();
		break;
	}

	// table target pushdown
	auto table = target->from_table;

	// point-lookup or range scan
	auto path = target->path_primary;
	if (path->type == PATH_LOOKUP && !path->match_start_columns)
	{
		if (! path->match_start_vars)
		{
			// match exact partition using the point lookup const key hash
			uint32_t hash = path_create_hash(path);

			// CSEND_LOOKUP
			op3(self, CSEND_LOOKUP, start, (intptr_t)table, hash);
		} else
		{
			// match exact partition using the point lookup exprs
			for (auto i = 0; i < path->match_start; i++)
			{
				auto value = path->keys[i].start;
				auto rexpr = emit_expr(self, target->targets, value);
				op1(self, CPUSH, rexpr);
				runpin(self, rexpr);
			}

			// CSEND_LOOKUP_BY
			op2(self, CSEND_LOOKUP_BY, start, (intptr_t)table);
		}
	} else
	{
		// send to all table partitions (one or more)

		// CSEND_ALL
		op2(self, CSEND_ALL, start, (intptr_t)table);
	}
	self->program->sends++;
}

static inline void
emit_call(Compiler* self)
{
	auto stmt = self->current;
	auto call = ast_call_of(stmt->ast);
	auto args = call->ast.r;
	auto arg  = args->l;
	// emit variables into registers, registers kept pinned until the end
	// of vm execution to avoid problem with async/recv on backends
	Targets targets;
	targets_init(&targets, stmt->scope);
	for (auto var = call->scope->vars.list; arg && var; var = var->next)
	{
		var->r = emit_expr(self, &targets, arg);
		Type type = rtype(self, var->r);
		if (type != var->type)
			stmt_error(self->current, arg, "expected %s", type_of(var->type));
		arg = arg->next;
	}
}

static inline void
emit_recv(Compiler* self)
{
	Returning* ret = NULL;
	int r = -1;
	auto stmt = self->current;
	switch (stmt->id) {
	case STMT_INSERT:
	{
		auto insert = ast_insert_of(stmt->ast);
		ret = &insert->ret;
		r = pushdown_recv_returning(self, returning_has(ret));
		break;
	}

	case STMT_UPDATE:
	{
		auto update = ast_update_of(stmt->ast);
		ret = &update->ret;
		r = pushdown_recv_returning(self, returning_has(ret));
		break;
	}

	case STMT_DELETE:
	{
		auto delete = ast_delete_of(stmt->ast);
		ret = &delete->ret;
		r = pushdown_recv_returning(self, returning_has(ret));
		break;
	}

	case STMT_SELECT:
	{
		auto select = ast_select_of(stmt->ast);
		ret = &select->ret;

		if (select->pushdown)
		{
			// process the result from one or more backends
			r = pushdown_recv(self, stmt->ast);
			break;
		}

		// select (select from table)
		// select expr
		r = emit_select(self, stmt->ast);
		break;
	}

	case STMT_CALL:
		// emit arguments and assign as variables
		emit_call(self);
		break;

	case STMT_WATCH:
		// no targets (frontend only)
		r = emit_watch(self, stmt->ast);
		break;

	default:
		abort();
		break;
	}
	stmt->r = r;

	// statement has assign :=
	auto var = stmt->assign;
	if (var)
	{
		if (! ret)
			stmt_error(stmt, NULL, "statement cannot be assigned");
		if (ret->count > 1)
			stmt_error(stmt, NULL, "statement must return only one column to be assigned");
		// argument assignment
		auto type = columns_first(&ret->columns)->type;
		if (var->type != TYPE_NULL && var->type != type)
			stmt_error(self->current, stmt->ast, "variable expected %s", type_of(var->type));
		var->type = type;
		var->r = op2(self, CASSIGN, rpin(self, var->type), r);
	}

	// last statement
	if (! stmt->ret)
		return;

	// create content out of result on result
	if (!stmt->scope->call && r != -1)
		op3(self, CCONTENT, r, (intptr_t)&ret->columns, (intptr_t)&ret->format);

	op0(self, CRET);
}

static inline bool
stmt_is_expr(Stmt* self)
{
	if (self->id != STMT_SELECT)
		return false;
	return !ast_select_of(self->ast)->pushdown;
}

static inline void
emit_recv_upto(Compiler* self, int last, int order)
{
	auto current = self->current;
	auto stmt = self->parser.stmts.list;
	for (; stmt; stmt = stmt->next)
	{
		if (stmt->order <= last)
			continue;
		if (stmt->id == STMT_CALL)
			continue;
		if (stmt->order > order)
			break;
		// <= recv
		self->current = stmt;
		emit_recv(self);
	}
	self->current = current;
}

static inline  int
stmt_maxcte(Stmt* self)
{
	int order = -1;
	if (self->id == STMT_INSERT)
	{
		auto insert = ast_insert_of(self->ast);
		if (insert->select)
			order = insert->select->order;
	} else
	{
		for (auto ref = self->select_list.list; ref; ref = ref->next)
		{
			auto select = ast_select_of(ref->ast);
			for (auto target = select->targets.list; target; target = target->next)
			{
				if (target->type != TARGET_CTE)
					continue;
				if (target->from_cte->order > order)
					order = target->from_cte->order;
			}
		}
	}
	return order;
}

hot void
compiler_emit(Compiler* self)
{
	auto recv_last = -1;
	auto stmt = self->parser.stmts.list;
	for (; stmt; stmt = stmt->next)
	{
		// generate frontend code (recv)
		compiler_switch_frontend(self);
		self->current = stmt;

		auto is_expr = false;
		if (stmt->id == STMT_CALL)
		{
			emit_recv(self);
		} else
		{
			// generate recv up to the max dependable statement order, including
			// frontend only expressions
			is_expr = stmt_is_expr(stmt);
			auto recv = -1;
			if (is_expr)
				recv = stmt->order;
			else
				recv = stmt_maxcte(stmt);
			if (recv >= 0)
			{
				emit_recv_upto(self, recv_last, recv);
				recv_last = recv;
			}
		}

		// generate backend code (pushdown)
		compiler_switch_backend(self);
		auto stmt_start = code_count(self->code);
		emit_stmt(self);

		// generate frontend code
		compiler_switch_frontend(self);

		// RETURN stmt
		if (stmt->ret)
		{
			if (! is_expr)
				emit_send(self, stmt_start);
			emit_recv_upto(self, recv_last, stmt->order);
			recv_last = stmt->order;
			continue;
		}

		// skip expression only statements (generated by recv above)
		if (is_expr)
			continue;

		// generate SEND command
		emit_send(self, stmt_start);
	}

	// recv rest of commands (if any left)
	emit_recv_upto(self, recv_last, self->parser.stmts.count);

	// no statements (last statement always returns)
	if (! self->parser.stmt)
		op0(self, CRET);

	// set the max number of registers used
	code_set_regs(&self->program->code, self->map.count);
	code_set_regs(&self->program->code_backend, self->map.count);
}
