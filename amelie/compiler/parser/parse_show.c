
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

void
parse_show(Stmt* self)
{
	// SHOW <SECTION> [name] [IN|FROM schema] [extended] [FORMAT type]
	auto stmt = ast_show_allocate();
	self->ast = &stmt->ast;
	self->ret = &stmt->ret;

	// section | option name
	auto name = stmt_next_shadow(self);
	if (name->id != KNAME && name->id != KSTRING)
		stmt_error(self, name, "name expected");
	stmt->section = name->string;

	// [name]
	name = stmt_next(self);
	if (name->id == KNAME || name->id == KSTRING) {
		stmt->name = name->string;
	} else
	if (name->id == KNAME_COMPOUND)
	{
		stmt->name = name->string;
		str_split(&stmt->name, &stmt->schema, '.');
		str_advance(&stmt->name, str_size(&stmt->schema) + 1);
	} else {
		stmt_push(self, name);
	}

	// [IN | FROM schema]
	if (stmt_if(self, KIN) || stmt_if(self, KFROM))
	{
		auto schema = stmt_expect(self, KNAME);
		stmt->schema = schema->string;
	}

	// [EXTENDED]
	stmt->extended = stmt_if(self, KEXTENDED) != NULL;

	// [FORMAT type]
	Str* spec;
	if (stmt_if(self, KFORMAT))
	{
		auto type = stmt_expect(self, KSTRING);
		spec = &type->string;
	} else {
		spec = self->parser->local->format;
	}

	// force include -unwrap to avoid double [] wrap
	char* fmt = palloc(str_size(spec) + 8);
	memcpy(fmt, spec->pos, str_size(spec));
	memcpy(fmt + str_size(spec), "-unwrap", 8);
	str_set_cstr(&stmt->ret.format, fmt);

	// set returning column
	auto column = column_allocate();
	column_set_name(column, &stmt->section);
	column_set_type(column, TYPE_JSON, -1);
	columns_add(&stmt->ret.columns, column);
}
