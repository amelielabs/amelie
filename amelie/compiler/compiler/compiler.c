
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
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
	self->code     = &self->code_node;
	self->args     = NULL;
	code_init(&self->code_coordinator);
	code_init(&self->code_node);
	code_data_init(&self->code_data);
	set_cache_init(&self->values_cache);
	parser_init(&self->parser, db, local, function_mgr, &self->code_data,
	            &self->values_cache);
	rmap_init(&self->map);
}

void
compiler_free(Compiler* self)
{
	parser_free(&self->parser);
	code_free(&self->code_coordinator);
	code_free(&self->code_node);
	code_data_free(&self->code_data);
	set_cache_free(&self->values_cache);
	rmap_free(&self->map);
}

void
compiler_reset(Compiler* self)
{
	self->code     = &self->code_node;
	self->args     = NULL;
	self->snapshot = false;
	self->current  = NULL;
	self->last     = NULL;
	code_reset(&self->code_coordinator);
	code_reset(&self->code_node);
	code_data_reset(&self->code_data);
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
	// generate node code (pushdown)
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
			// distributed or shared table direct scan or join
			//
			// execute on one or more nodes, process the result on coordinator
			//
			pushdown(self, stmt->ast);
			table_in_use = true;
			break;
		}

		// select (select from table)
		if (select->pushdown == PUSHDOWN_FIRST)
		{
			// shared table being involved in a subquery
			//
			// execute the whole query on the first node, coordinator will
			// receive the result
			//
			pushdown_first(self, stmt->ast);
			table_in_use = true;
			break;
		}

		// no targets or all targets are expressions
		//
		// do nothing (coordinator only)
		return;
	}

	case STMT_WATCH:
		// do nothing (coordinator only)
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
			error("<%.*s.%.*s> column generated expression type '%s' does not match column type '%s'",
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
emit_send_generated(Compiler* self)
{
	auto stmt   = self->current;
	auto insert = ast_insert_of(stmt->ast);

	// store_open( insert->values )
	auto target = targets_outer(&insert->targets_generated);
	target->r = op2(self, CSET_PTR, rpin(self, TYPE_STORE),
	                (intptr_t)insert->values);

	// scan over insert values to generate and apply stored columns
	scan(self, &insert->targets_generated,
	     NULL,
	     NULL,
	     NULL,
	     emit_send_generated_on_match,
	     insert);
}

static inline void
emit_send(Compiler* self, int start)
{
	// generate coordinator send code
	Target* target = NULL;
	auto stmt = self->current;
	switch (stmt->id) {
	case STMT_INSERT:
	{
		auto insert = ast_insert_of(stmt->ast);
		auto table = targets_outer(&insert->targets)->from_table;
		if (table_columns(table)->count_stored > 0)
			emit_send_generated(self);
		// CSEND
		op4(self, CSEND, stmt->order, start,
		    (intptr_t)table,
		    (intptr_t)insert->values);
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
		if (select->pushdown == PUSHDOWN_FIRST)
		{
			// CSEND_FIRST
			op2(self, CSEND_FIRST, stmt->order, start);
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
	if (table->config->shared)
	{
		// send to first node

		// CSEND_FIRST
		op2(self, CSEND_FIRST, stmt->order, start);
	} else
	{
		// point-lookup or range scan
		if (ast_path_of(target->path)->type == PATH_LOOKUP)
		{
			// send to one node (shard by lookup key hash)
			uint32_t hash = target_lookup_hash(target);

			// CSEND_HASH
			op4(self, CSEND_HASH, stmt->order, start, (intptr_t)table, hash);
		} else
		{
			// send to all table nodes

			// CSEND_ALL
			op3(self, CSEND_ALL, stmt->order, start, (intptr_t)table);
		}
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
			// process the result from one or more nodes
			r = pushdown_recv(self, stmt->ast);
			break;
		}

		// select (select from table)
		if (select->pushdown == PUSHDOWN_FIRST)
		{
			// recv whole query result from the first node

			// CRECV_TO (note: set or merge received)
			r = op2(self, CRECV_TO, rpin(self, TYPE_STORE), stmt->order);
			break;
		}

		// no targets or all targets are expressions
		r = emit_select(self, stmt->ast);
		break;
	}

	case STMT_WATCH:
		// no targets (coordinator only)
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
		// generate node code (pushdown)
		auto stmt_start = code_count(&self->code_node);
		compiler_switch_node(self);
		self->current = stmt;
		emit_stmt(self);

		// generate coordinator code
		compiler_switch_coordinator(self);

		// generate recv up to the max dependable statement order, including
		// coordinator only expressions
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
	program->code       = &self->code_coordinator;
	program->code_node  = &self->code_node;
	program->code_data  = &self->code_data;
	program->stmts      = self->parser.stmt_list.count;
	program->stmts_last = -1;
	program->snapshot   = self->snapshot;
	program->repl       = false;
	if (self->last)
		program->stmts_last = self->last->order;
}
