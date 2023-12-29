
//
// indigo
//
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>
#include <indigo_request.h>
#include <indigo_vm.h>
#include <indigo_parser.h>

void
parse_user_create(Stmt* self)
{
	// CREATE USER [IF NOT EXISTS] name [PASSWORD value]
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

	// PASSWORD
	if (stmt_if(self, KPASSWORD))
	{
		// value
		auto value = stmt_if(self, KSTRING);
		if (! value)
			error("CREATE USER PASSWORD <value> string expected");
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
	// ALTER USER name PASSWORD expr
	auto stmt = ast_user_alter_allocate();
	self->ast = &stmt->ast;

	stmt->config = user_config_allocate();

	// name
	auto name = stmt_if(self, KNAME);
	if (! name)
		error("ALTER USER <name> expected");
	user_config_set_name(stmt->config, &name->string);

	// PASSWORD
	if (! stmt_if(self, KPASSWORD))
		error("ALTER USER <PASSWORD> expected");

	// value
	auto value = stmt_if(self, KSTRING);
	if (! value)
		error("ALTER USER PASSWORD <value> string expected");
	user_config_set_secret(stmt->config, &value->string);
}
