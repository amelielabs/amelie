
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
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

#if 0
typedef struct QueryCompiler QueryCompiler;

struct QueryCompiler
{
	Query    query;
	Compiler compiler;
};

static inline Query*
query_compiler_create(QueryIf*  iface,
                      Local*    local,
                      SetCache* set_cache,
                      Str*      content_type)
{
	auto self  = (QueryCompiler*)am_malloc(sizeof(QueryCompiler));
	auto query = &self->query;
	query->iface = iface;
	str_copy(&query->content_type, content_type);
	list_init(&query->link);
	compiler_init(&self->compiler, local, set_cache);
	return &self->query;
}

static inline void
query_compiler_free(Query* query)
{
	auto self = (QueryCompiler*)query;
	compiler_free(&self->compiler);
	str_free(&self->query.content_type);
	am_free(self);
}

static inline void
query_compiler_reset(Query* query)
{
	auto self = (QueryCompiler*)query;
	compiler_reset(&self->compiler);
}

static inline void
query_compiler_parse(Query* query, QueryContext* ctx)
{
	auto self = (QueryCompiler*)query;
	auto compiler = &self->compiler;
	compiler_set(compiler, ctx->program);

	// parse SQL statements
	compiler_parse(compiler, ctx->text);

	// propagate explain/profile request
	ctx->explain = compiler->parser.explain;
	ctx->profile = compiler->parser.profile;

	auto stmt = compiler_stmt(compiler);
	if (! stmt)
		return;

	// set returning columns
	auto last = compiler_main(compiler)->stmts.list_tail;
	if (last->ret && last->ret->columns.count > 0)
		ctx->returning = &last->ret->columns;

	// update context to pass EXECUTE arguments
	if (stmt->id == STMT_EXECUTE)
	{
		auto execute = ast_execute_of(stmt->ast);
		auto udf = execute->udf;
		ctx->program = (Program*)udf->data;
		ctx->execute = udf;
		ctx->args    = set_value(execute->args, 0);

		// set udf returning columns, for table results
		if (udf->config->type == TYPE_STORE)
			ctx->returning = &udf->config->returning;
		return;
	}

	// generate bytecode
	compiler_emit(compiler);

	// generate explain
	if (ctx->explain || ctx->profile)
		explain(compiler, NULL, NULL);
}

static inline void
query_compiler_parse_endpoint(Query* query, QueryContext* ctx, EndpointType type)
{
	auto self = (QueryCompiler*)query;
	auto compiler = &self->compiler;
	compiler_set(compiler, ctx->program);

	// parse SQL statements
	compiler_parse_import(compiler, ctx->text, ctx->uri, type);

	auto stmt = compiler_stmt(compiler);
	if (! stmt)
		return;

	// set returning columns
	auto last = compiler_main(compiler)->stmts.list_tail;
	if (last->ret && last->ret->columns.count > 0)
		ctx->returning = &last->ret->columns;

	// update context to pass EXECUTE arguments
	if (stmt->id == STMT_EXECUTE)
	{
		auto execute = ast_execute_of(stmt->ast);
		auto udf = execute->udf;
		ctx->program = (Program*)udf->data;
		ctx->execute = udf;
		ctx->args     = set_value(execute->args, 0);

		// set udf returning columns, for table results
		if (udf->config->type == TYPE_STORE)
			ctx->returning = &udf->config->returning;
		return;
	}

	// generate bytecode
	compiler_emit(compiler);

	// generate explain
	if (ctx->explain || ctx->profile)
		explain(compiler, NULL, NULL);
}

// sql
static Query*
query_sql_create(Local* local, SetCache* set_cache, Str* content_type)
{
	return query_compiler_create(&query_sql_if, local, set_cache, content_type);
}

static void
query_sql_parse(Query* query, QueryContext* ctx)
{
	query_compiler_parse(query, ctx);
}

QueryIf query_sql_if =
{
	.create = query_sql_create,
	.free   = query_compiler_free,
	.reset  = query_compiler_reset,
	.parse  = query_sql_parse
};

// csv
static Query*
query_csv_create(Local* local, SetCache* set_cache, Str* content_type)
{
	return query_compiler_create(&query_csv_if, local, set_cache, content_type);
}

static void
query_csv_parse(Query* query, QueryContext* ctx)
{
	query_compiler_parse_endpoint(query, ctx, ENDPOINT_CSV);
}

QueryIf query_csv_if =
{
	.create = query_csv_create,
	.free   = query_compiler_free,
	.reset  = query_compiler_reset,
	.parse  = query_csv_parse
};

// json
static Query*
query_json_create(Local* local, SetCache* set_cache, Str* content_type)
{
	return query_compiler_create(&query_json_if, local, set_cache, content_type);
}

static void
query_json_parse(Query* query, QueryContext* ctx)
{
	query_compiler_parse_endpoint(query, ctx, ENDPOINT_JSON);
}

QueryIf query_json_if =
{
	.create = query_json_create,
	.free   = query_compiler_free,
	.reset  = query_compiler_reset,
	.parse  = query_json_parse
};

// jsonl
static Query*
query_jsonl_create(Local* local, SetCache* set_cache, Str* content_type)
{
	return query_compiler_create(&query_jsonl_if, local, set_cache, content_type);
}

static void
query_jsonl_parse(Query* query, QueryContext* ctx)
{
	query_compiler_parse_endpoint(query, ctx, ENDPOINT_JSONL);
}

QueryIf query_jsonl_if =
{
	.create = query_jsonl_create,
	.free   = query_compiler_free,
	.reset  = query_compiler_reset,
	.parse  = query_jsonl_parse
};
#endif
