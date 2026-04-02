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

static inline void
check_user(Tr* tr, uint32_t perms)
{
	// user itself must have the permissions
	user_check(user_of(tr->user), perms);
}

static inline void
check_ownership_user(Tr* tr, Rel* rel)
{
	// user must be the parent of other user
	if (! user_owns(user_of(tr->user), rel))
		user_permission_error(user_of(rel));
}

static inline void
check_ownership(Tr* tr, Rel* rel)
{
	// user is the owner of rel
	if (! user_owns(user_of(tr->user), rel))
		rel_permission_error(rel);
}

static inline void
check_permission(Tr* tr, Rel* rel, uint32_t perms)
{
	user_check_permission(user_of(tr->user), rel, perms);
}
