
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
#include <amelie_db>
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_parser.h>

void
parse_grant(Stmt* self, bool grant)
{
	// GRANT  perm, ... ON name TO user
	// REVOKE perm, ... ON name FROM user
	auto stmt = ast_grant_allocate();
	self->ast = &stmt->ast;

	// grant or revoke
	stmt->grant = grant;

	// permissions
	for (;;)
	{
		// name
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
			stmt_error(self, name, "name expected");

		uint32_t id = 0;
		if (permission_of(&name->string, &id) == -1)
			stmt_error(self, name, "unknown permission name");
		stmt->perms |= id;

		// [ON]
		if (stmt_if(self, KON))
			break;

		// ,
		stmt_expect(self, ',');
	}

	// relation
	parse_target(self, &stmt->rel_user, &stmt->rel);

	// TO | FROM
	if (grant)
		stmt_expect(self, KTO);
	else
		stmt_expect(self, KFROM);

	// user
	auto user = stmt_expect(self, KNAME);
	stmt->to = user->string;
}
