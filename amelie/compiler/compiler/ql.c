
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
#include <amelie_os.h>
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

typedef struct QlCompiler QlCompiler;

struct QlCompiler
{
	Ql       ql;
	Compiler compiler;
};

static inline Ql*
ql_compiler_create(Local* local, QlIf* iface, Str* application_type)
{
	auto self = (QlCompiler*)am_malloc(sizeof(QlCompiler));
	auto ql = &self->ql;
	ql->iface = iface;
	str_copy(&ql->application_type, application_type);
	list_init(&ql->link);
	compiler_init(&self->compiler, local);
	return &self->ql;
}

static inline void
ql_compiler_free(Ql* ql)
{
	auto self = (QlCompiler*)ql;
	compiler_free(&self->compiler);
	str_free(&self->ql.application_type);
	am_free(self);
}

static inline void
ql_compiler_reset(Ql* ql)
{
	auto self = (QlCompiler*)ql;
	compiler_reset(&self->compiler);
}

static inline void
ql_compiler_parse(Ql* ql, Program* program, Str* text)
{
	auto self = (QlCompiler*)ql;
	auto compiler = &self->compiler;
	compiler_set(compiler, program);

	// parse SQL statements
	compiler_parse(compiler, text);
	if (! compiler->parser.stmts.count)
		return;

	// generate bytecode
	compiler_emit(compiler);
}

static inline void
ql_compiler_parse_endpoint(Ql*          ql,
                           Program*     program,
                           Str*         text,
                           Str*         uri,
                           EndpointType type)
{
	auto self = (QlCompiler*)ql;
	auto compiler = &self->compiler;
	compiler_set(compiler, program);

	// parse SQL statements
	compiler_parse_import(compiler, text, uri, type);
	if (! compiler->parser.stmts.count)
		return;

	// generate bytecode
	compiler_emit(compiler);
}

// sql
static Ql*
ql_sql_create(Local* local, Str* application_type)
{
	return ql_compiler_create(local, &ql_sql_if, application_type);
}

static void
ql_sql_parse(Ql* ql, Program* program, Str* text, Str* uri)
{
	unused(uri);
	ql_compiler_parse(ql, program, text);
}

QlIf ql_sql_if =
{
	.create = ql_sql_create,
	.free   = ql_compiler_free,
	.reset  = ql_compiler_reset,
	.parse  = ql_sql_parse
};

// csv
static Ql*
ql_csv_create(Local* local, Str* application_type)
{
	return ql_compiler_create(local, &ql_csv_if, application_type);
}

static void
ql_csv_parse(Ql* ql, Program* program, Str* text, Str* uri)
{
	ql_compiler_parse_endpoint(ql, program, text, uri, ENDPOINT_CSV);
}

QlIf ql_csv_if =
{
	.create = ql_csv_create,
	.free   = ql_compiler_free,
	.reset  = ql_compiler_reset,
	.parse  = ql_csv_parse
};

// json
static Ql*
ql_json_create(Local* local, Str* application_type)
{
	return ql_compiler_create(local, &ql_json_if, application_type);
}

static void
ql_json_parse(Ql* ql, Program* program, Str* text, Str* uri)
{
	ql_compiler_parse_endpoint(ql, program, text, uri, ENDPOINT_JSON);
}

QlIf ql_json_if =
{
	.create = ql_json_create,
	.free   = ql_compiler_free,
	.reset  = ql_compiler_reset,
	.parse  = ql_json_parse
};

// jsonl
static Ql*
ql_jsonl_create(Local* local, Str* application_type)
{
	return ql_compiler_create(local, &ql_jsonl_if, application_type);
}

static void
ql_jsonl_parse(Ql* ql, Program* program, Str* text, Str* uri)
{
	ql_compiler_parse_endpoint(ql, program, text, uri, ENDPOINT_JSONL);
}

QlIf ql_jsonl_if =
{
	.create = ql_jsonl_create,
	.free   = ql_compiler_free,
	.reset  = ql_compiler_reset,
	.parse  = ql_jsonl_parse
};
