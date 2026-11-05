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

typedef struct Users Users;
typedef struct User  User;

struct User
{
	Rel         rel;
	Catalog     catalog;
	Usage       memory;
	int         commit;
	uint32_t    inv_seq;
	uint64_t    inv_revoked;
	Task*       compute;
	Users*      users;
	UserConfig* config;
};

bool user_create(Users*, Tr*, UserConfig*, bool);
bool user_drop(Users*, Tr*, Str*, bool, bool);
bool user_rename(Users*, Tr*, Str*, Str*, bool);
bool user_revoke_token(Users*, Tr*, Str*, Str*, bool);
bool user_grant(Users*, Tr*, Str*, bool, uint32_t, bool);
bool user_describe(Users*, Tr*, Str*, Str*, bool);
bool user_limit_set(Users*, Tr*, Str*, Limits*, bool);
bool user_limit_unset(Users*, Tr*, Str*, uint64_t, bool);
bool user_api_create(Users*, Tr*, Str*, Api*, bool);
bool user_api_drop(Users*, Tr*, Str*, Str*, bool);

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
