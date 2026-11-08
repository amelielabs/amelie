
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
#include <amelie_heap>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>

void
parse_token_create(Stmt* self)
{
	// CREATE TOKEN name [EXPIRE interval]
	auto stmt = ast_token_create_allocate();
	self->ast = &stmt->ast;
	self->ret = &stmt->ret;

	// name
	stmt->db = stmt_expect(self, KNAME);

	// EXPIRE
	if (stmt_if(self, KEXPIRE))
	{
		// value
		stmt->expire = stmt_expect(self, KSTRING);
	}

	// set returning column
	auto column = column_allocate();
	column_set_name(column, &stmt->db->string);
	column_set_type(column, TYPE_JSON, -1);
	columns_add(&stmt->ret.columns, column);
}

#if 0
void
parse_grant(Stmt* self, bool grant)
{
	// GRANT  perm, ... [ON name] TO db
	// REVOKE perm, ... [ON name] FROM db
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

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// [ON]
	if (stmt_if(self, KON))
	{
		// relation
		auto name = stmt_expect(self, KNAME);
		stmt->rel = name->string;
	}

	// TO | FROM
	if (grant)
		stmt_expect(self, KTO);
	else
		stmt_expect(self, KFROM);

	// db
	auto db = stmt_expect(self, KNAME);
	stmt->to = db->string;
}

void
parse_grant_to_inline(Stmt* self, Grants* grants)
{
	// GRANT perm, ... TO db

	// permissions
	uint32_t perms = 0;
	for (;;)
	{
		// name
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
			stmt_error(self, name, "name expected");

		uint32_t id = 0;
		if (permission_of(&name->string, &id) == -1)
			stmt_error(self, name, "unknown permission name");
		perms |= id;

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// TO
	stmt_expect(self, KTO);

	// db
	auto db = stmt_expect(self, KNAME);
	grants_add(grants, &db->string, perms);
}

void
parse_grant_self_inline(Stmt* self, Grants* grants)
{
	// GRANT perm, ...

	// permissions
	uint32_t perms = 0;
	for (;;)
	{
		// name
		auto name = stmt_next_shadow(self);
		if (name->id != KNAME)
			stmt_error(self, name, "name expected");

		uint32_t id = 0;
		if (permission_of(&name->string, &id) == -1)
			stmt_error(self, name, "unknown permission name");
		perms |= id;

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	Str to;
	str_set(&to, "self", 4);
	grants_add(grants, &to, perms);
}
#endif
