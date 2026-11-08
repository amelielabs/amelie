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

typedef struct Db Db;

struct Db
{
	Rel       rel;
	Catalog   catalog;
	Catalog*  databases;
	Usage     memory;
	uint32_t  inv_seq;
	uint64_t  inv_revoked;
	int       commit;
	Task*     compute;
	DbConfig* config;
};

bool db_create(Catalog*, Tr*, DbConfig*, bool);
bool db_drop(Catalog*, Tr*, Str*, bool);
bool db_rename(Catalog*, Tr*, Str*, Str*, bool);
bool db_revoke_token(Catalog*, Tr*, Str*, Str*, bool);
bool db_describe(Catalog*, Tr*, Str*, Str*, bool);
bool db_limit_set(Catalog*, Tr*, Str*, Limits*, bool);
bool db_limit_unset(Catalog*, Tr*, Str*, uint64_t, bool);
bool db_api_create(Catalog*, Tr*, Str*, Api*, bool);
bool db_api_drop(Catalog*, Tr*, Str*, Str*, bool);

always_inline static inline Db*
db_of(Rel* self)
{
	return (Db*)self;
}

static inline void
db_invalidate(Db* self)
{
	self->inv_seq++;
}

static inline void
db_check_ownership(Tr* self, Db* on)
{
	// todo: use tr->grants to check if the ddl access
	unused(self);
	unused(on);
}
