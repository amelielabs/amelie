#pragma once

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

typedef struct Database Database;

struct Database
{
	Rel             rel;
	Catalog         catalog;
	Catalog*        databases;
	DatabaseConfig* config;

	Usage           memory;
	uint32_t        inv_seq;
	uint64_t        inv_revoked;
	int             commit;
	Task*           compute;
};

bool database_create(Catalog*, Tr*, DatabaseConfig*, bool);
bool database_drop(Catalog*, Tr*, Str*, bool, bool);
bool database_rename(Catalog*, Tr*, Str*, Str*, bool);
bool database_revoke_token(Catalog*, Tr*, Str*, Str*, bool);
bool database_grant(Catalog*, Tr*, Str*, bool, uint32_t, bool);
bool database_describe(Catalog*, Tr*, Str*, Str*, bool);
bool database_limit_set(Catalog*, Tr*, Str*, Limits*, bool);
bool database_limit_unset(Catalog*, Tr*, Str*, uint64_t, bool);
bool database_api_create(Catalog*, Tr*, Str*, Api*, bool);
bool database_api_drop(Catalog*, Tr*, Str*, Str*, bool);

always_inline static inline Database*
database_of(Rel* self)
{
	return (Database*)self;
}

static inline void
database_invalidate(Database* self)
{
	self->inv_seq++;
}

static inline void
database_check_parent(Tr* self, Database* of)
{
	// system or parent
	auto database = database_of(self->database);
	if (! (database->config->system || str_compare(&database->config->name, &of->config->parent)))
		error("database '{str}': permission denied", &of->config->name);
}
