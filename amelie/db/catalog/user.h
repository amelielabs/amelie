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

typedef struct User User;

struct User
{
	Rel         rel;
	Catalog     catalog;
	Usage       memory;
	int         commit;
	uint32_t    inv_seq;
	uint64_t    inv_revoked;
	Task*       compute;
	Catalog*    users;
	UserConfig* config;
};

bool user_create(Catalog*, Tr*, UserConfig*, bool);
bool user_drop(Catalog*, Tr*, Str*, bool, bool);
bool user_rename(Catalog*, Tr*, Str*, Str*, bool);
bool user_revoke_token(Catalog*, Tr*, Str*, Str*, bool);
bool user_grant(Catalog*, Tr*, Str*, bool, uint32_t, bool);
bool user_describe(Catalog*, Tr*, Str*, Str*, bool);
bool user_limit_set(Catalog*, Tr*, Str*, Limits*, bool);
bool user_limit_unset(Catalog*, Tr*, Str*, uint64_t, bool);
bool user_api_create(Catalog*, Tr*, Str*, Api*, bool);
bool user_api_drop(Catalog*, Tr*, Str*, Str*, bool);

always_inline static inline User*
user_of(Rel* self)
{
	return (User*)self;
}

static inline void
user_invalidate(User* self)
{
	self->inv_seq++;
}

static inline void
user_check_ownership(Tr* self, User* of)
{
	// superuser or parent
	auto user = user_of(self->user_);
	if (! (user->config->superuser ||
	       str_compare(&user->config->name, &of->config->parent)))
		error("user '{str}': permission denied", &of->config->name);
}
