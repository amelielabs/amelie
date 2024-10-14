
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
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
#include <amelie_aggr.h>
#include <amelie_executor.h>
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
	auto name = stmt_if(self, KNAME);
	if (! name)
		error("CREATE USER <name> expected");
	user_config_set_name(stmt->config, &name->string);

	// [SECRET]
	if (stmt_if(self, KSECRET))
	{
		// value
		auto value = stmt_if(self, KSTRING);
		if (! value)
			error("CREATE USER SECRET <value> string expected");
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
	stmt->name = stmt_if(self, KNAME);
	if (! stmt->name)
		error("DROP USER <name> expected");
}

void
parse_user_alter(Stmt* self)
{
	// ALTER USER name SECRET expr
	auto stmt = ast_user_alter_allocate();
	self->ast = &stmt->ast;

	stmt->config = user_config_allocate();

	// name
	auto name = stmt_if(self, KNAME);
	if (! name)
		error("ALTER USER <name> expected");
	user_config_set_name(stmt->config, &name->string);

	// SECRET
	if (! stmt_if(self, KSECRET))
		error("ALTER USER <SECRET> expected");

	// value
	auto value = stmt_if(self, KSTRING);
	if (! value)
		error("ALTER USER SECRET <value> string expected");
	user_config_set_secret(stmt->config, &value->string);
}
