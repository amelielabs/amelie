
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

void
parse_user_create(Stmt* self)
{
	// CREATE USER [IF NOT EXISTS] name [SECRET value]
	auto stmt = ast_user_create_allocate();
	self->ast = &stmt->ast;

	stmt->config = user_config_allocate();

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// name
	auto name = stmt_expect(self, KNAME);
	user_config_set_name(stmt->config, &name->string);

	// [SECRET]
	if (stmt_if(self, KSECRET))
	{
		// value
		auto value = stmt_expect(self, KSTRING);
		user_config_set_secret(stmt->config, &value->string);
	}
}

void
parse_user_drop(Stmt* self)
{
	// DROP USER [IF EXISTS] name
	auto stmt = ast_user_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	stmt->name = stmt_expect(self, KNAME);
}

void
parse_user_alter(Stmt* self)
{
	// ALTER USER name SECRET expr
	auto stmt = ast_user_alter_allocate();
	self->ast = &stmt->ast;

	stmt->config = user_config_allocate();

	// name
	auto name = stmt_expect(self, KNAME);
	user_config_set_name(stmt->config, &name->string);

	// SECRET
	stmt_expect(self, KSECRET);

	// value
	auto value = stmt_expect(self, KSTRING);
	user_config_set_secret(stmt->config, &value->string);
}
