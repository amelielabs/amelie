
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
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
#include <amelie_catalog.h>
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

typedef struct QueryCompiler QueryCompiler;

struct QueryCompiler
{
	Query    query;
	Compiler compiler;
};

static inline Query*
query_compiler_create(Local* local, QueryIf* iface, Str* content_type)
{
	auto self  = (QueryCompiler*)am_malloc(sizeof(QueryCompiler));
	auto query = &self->query;
	query->iface = iface;
	str_copy(&query->content_type, content_type);
	list_init(&query->link);
	compiler_init(&self->compiler, local);
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

	// set returning columns and format
	auto last = compiler_main(compiler)->stmts.list_tail;
	if (last->ret && last->ret->columns.count > 0)
	{
		ctx->returning     = &last->ret->columns;
		ctx->returning_fmt = &last->ret->format;
	}

	// generate bytecode
	compiler_emit(compiler);
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

	// set returning columns and format
	if (stmt->ret && stmt->ret->columns.count > 0)
	{
		ctx->returning     = &stmt->ret->columns;
		ctx->returning_fmt = &stmt->ret->format;
	}

	// generate bytecode
	compiler_emit(compiler);
}

// sql
static Query*
query_sql_create(Local* local, Str* content_type)
{
	return query_compiler_create(local, &query_sql_if, content_type);
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
query_csv_create(Local* local, Str* content_type)
{
	return query_compiler_create(local, &query_csv_if, content_type);
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
query_json_create(Local* local, Str* content_type)
{
	return query_compiler_create(local, &query_json_if, content_type);
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
query_jsonl_create(Local* local, Str* content_type)
{
	return query_compiler_create(local, &query_jsonl_if, content_type);
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
