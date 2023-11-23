
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_snapshot.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

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
