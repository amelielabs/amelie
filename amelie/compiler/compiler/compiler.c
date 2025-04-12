
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
              FunctionMgr* function_mgr)
{
	self->snapshot = false;
	self->current  = NULL;
	self->last     = NULL;
	self->db       = db;
	self->code     = &self->code_backend;
	self->args     = NULL;
	code_init(&self->code_frontend);
	code_init(&self->code_backend);
	code_data_init(&self->code_data);
	access_init(&self->access);
	set_cache_init(&self->values_cache);
	parser_init(&self->parser, db, local, function_mgr, &self->code_data,
	            &self->access, &self->values_cache);
	rmap_init(&self->map);
}

void
compiler_free(Compiler* self)
{
	parser_free(&self->parser);
	code_free(&self->code_frontend);
	code_free(&self->code_backend);
	code_data_free(&self->code_data);
	access_free(&self->access);
	set_cache_free(&self->values_cache);
	rmap_free(&self->map);
}

void
compiler_reset(Compiler* self)
{
	self->code     = &self->code_backend;
	self->args     = NULL;
	self->snapshot = false;
	self->current  = NULL;
	self->last     = NULL;
	code_reset(&self->code_frontend);
	code_reset(&self->code_backend);
	code_data_reset(&self->code_data);
	access_reset(&self->access);
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
	auto     stmt = self->current;
	Targets* dml = NULL;
	auto     table_in_use = false;
	switch (stmt->id) {
	case STMT_INSERT:
	{
		auto insert = ast_insert_of(stmt->ast);
		if (insert->on_conflict == ON_CONFLICT_NONE)
			emit_insert(self, stmt->ast);
		else
			emit_upsert(self, stmt->ast);
		dml = &insert->targets;
		table_in_use = true;
		break;
	}
	case STMT_UPDATE:
	{
		emit_update(self, stmt->ast);
		dml = &ast_update_of(stmt->ast)->targets;
		table_in_use = true;
		break;
	}

	case STMT_DELETE:
	{
		emit_delete(self, stmt->ast);
		dml = &ast_delete_of(stmt->ast)->targets;
		table_in_use = true;
		break;
	}

	case STMT_SELECT:
	{
		// select from table, ...
		auto select = ast_select_of(stmt->ast);
		if (select->pushdown == PUSHDOWN_TARGET)
		{
			// partitioned or shared table direct scan or join
			//
			// execute on one or more backends, process the result on frontend
			//
			pushdown(self, stmt->ast);
			table_in_use = true;
			break;
		}

		// select (select from table)
		if (select->pushdown == PUSHDOWN_FULL)
		{
			// shared table being involved in a subquery
			//
			// pushdown whole query, frontend will receive the result
			//
			pushdown_full(self, stmt->ast);
			table_in_use = true;
			break;
		}

		// no targets or all targets are expressions
		//
		// do nothing (frontend only)
		return;
	}

	case STMT_WATCH:
		// do nothing (frontend only)
		return;

	default:
		abort();
		break;
	}

	if (table_in_use)
	{
		// set last statement which uses a table,
		// set snapshot if two or more stmts are using tables
		if (self->last)
			self->snapshot = true;
		self->last = self->current;

		// set snapshot if at least one dml uses a shared table
		if (dml && targets_count(dml, TARGET_TABLE_SHARED))
			self->snapshot = true;
	}

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

static inline void
emit_send_insert(Compiler* self, int start)
{
	auto stmt    = self->current;
	auto insert  = ast_insert_of(stmt->ast);
	auto table   = targets_outer(&insert->targets)->from_table;
	auto columns = table_columns(table);

	// get insert values
	int r;
	if (insert->select)
	{
		auto columns_select = &ast_select_of(insert->select->ast)->ret.columns;
		if (! columns_compare(columns, columns_select))
			stmt_error(stmt, insert->select->ast, "SELECT columns must match the INSERT table");
		r = op2(self, CCTE_GET, rpin(self, TYPE_STORE), insert->select->order);
	} else
	{
		r = op2(self, CSET_PTR, rpin(self, TYPE_STORE),
		        (intptr_t)insert->values);
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
	op4(self, CSEND_SHARD, stmt->order, start, (intptr_t)table, r);
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
		break;
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
		if (select->pushdown == PUSHDOWN_TARGET)
		{
			target = select->pushdown_target;
			break;
		}

		// select (select from table)
		if (select->pushdown == PUSHDOWN_FULL)
		{
			// execute on the first partition of a shared table
			// using in the subquery
			target = select->pushdown_target;
			break;
		}

		// no targets or all targets are expressions
		break;
	}

	case STMT_WATCH:
		// no targets
		break;

	default:
		abort();
		break;
	}

	// table target
	if (! target)
		return;

	auto table = target->from_table;

	// point-lookup or range scan
	auto path = target->path_primary;
	if (path->type == PATH_LOOKUP && !path->match_start_columns)
	{
		// match exact partition using the point lookup key hash
		uint32_t hash = path_create_hash(path);

		// CSEND_LOOKUP
		op4(self, CSEND_LOOKUP, stmt->order, start, (intptr_t)table, hash);
	} else
	{
		// send to all table partitions (one or more)

		// CSEND_ALL
		op3(self, CSEND_ALL, stmt->order, start, (intptr_t)table);
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

		if (select->pushdown == PUSHDOWN_TARGET)
		{
			// process the result from one or more backends
			r = pushdown_recv(self, stmt->ast);
			break;
		}

		// select (select from table)
		if (select->pushdown == PUSHDOWN_FULL)
		{
			// recv whole query result

			// CRECV_TO (note: set or union received)
			r = op2(self, CRECV_TO, rpin(self, TYPE_STORE), stmt->order);
			break;
		}

		// no targets or all targets are expressions
		r = emit_select(self, stmt->ast);
		break;
	}

	case STMT_WATCH:
		// no targets (frontend only)
		r = emit_watch(self, stmt->ast);
		break;

	default:
		abort();
		break;
	}

	auto has_result = r != -1;
	if (! has_result)
		r = op1(self, CNULL, rpin(self, TYPE_NULL));

	// CCTE_SET
	op2(self, CCTE_SET, stmt->order, r);
	runpin(self, r);

	// statement returns
	if (stmt->ret)
	{
		// create content using cte result
		if (has_result)
			op3(self, CCONTENT, stmt->order,
			    (intptr_t)&ret->columns,
			    (intptr_t)&ret->format);
		op0(self, CRET);
	}
}

static inline void
emit_recv_upto(Compiler* self, int last, int order)
{
	auto current = self->current;
	auto stmt = self->parser.stmt_list.list;
	for (; stmt; stmt = stmt->next)
	{
		if (stmt->order <= last)
			continue;
		if (stmt->order > order)
			break;
		// <= recv
		self->current = stmt;	
		emit_recv(self);
	}
	self->current = current;
}

static inline bool
stmt_is_expr(Stmt* self)
{
	if (self->id != STMT_SELECT)
		return false;
	return ast_select_of(self->ast)->pushdown == PUSHDOWN_NONE;
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
	rmap_prepare(&self->map);
	auto recv_last = -1;

	auto stmt = self->parser.stmt_list.list;
	for (; stmt; stmt = stmt->next)
	{
		// generate backend code (pushdown)
		auto stmt_start = code_count(&self->code_backend);
		compiler_switch_backend(self);
		self->current = stmt;
		emit_stmt(self);

		// generate frontend code
		compiler_switch_frontend(self);

		// generate recv up to the max dependable statement order, including
		// frontend only expressions
		auto is_expr = stmt_is_expr(stmt);
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
	emit_recv_upto(self, recv_last, self->parser.stmt_list.count);

	// no statements (last statement always returns)
	if (! self->parser.stmt)
		op0(self, CRET);
}

void
compiler_program(Compiler* self, Program* program)
{
	program->code         = &self->code_frontend;
	program->code_backend = &self->code_backend;
	program->code_data    = &self->code_data;
	program->access       = &self->access;
	program->stmts        = self->parser.stmt_list.count;
	program->stmts_last   = -1;
	program->snapshot     = self->snapshot;
	program->repl         = false;
	if (self->last)
		program->stmts_last = self->last->order;
}
