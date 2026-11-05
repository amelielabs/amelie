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
	Str*     name;
	bool     superuser;
	bool     agent;
	uint32_t inv_seq;
	uint64_t inv_revoked;
	Usage*   memory;
	Grants*  grants;
};

static inline void
user_init(User* self)
{
	self->name        = NULL;
	self->superuser   = false;
	self->agent       = false;
	self->inv_seq     = 0;
	self->inv_revoked = 0;
	self->memory      = NULL;
	self->grants      = NULL;
}

static inline void
user_invalidate(User* self)
{
	self->inv_seq++;
}

static inline void
user_permission_error(User* self)
{
	error("user '{str}': permission denied", self->name);
}

static inline void
user_check(User* self, uint32_t perms)
{
	if (self->superuser)
		return;
	if (! grants_check_first(self->grants, perms))
		user_permission_error(self);
}

static inline void
check_user(Tr* tr, uint32_t perms)
{
	user_check(tr->user, perms);
}

#if 0
static inline void
check_ownership_self(Tr* tr, User* on)
{
	// user must be the owner or self
	auto self = user_of(tr->user);
	if (! (user_owns(user_of(tr->user), &on->rel) || self == on))
		user_permission_error(on);
}
#endif
