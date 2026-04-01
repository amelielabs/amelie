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
	auto user = user_of(tr->user);
	if (user->config->superuser)
		return;
	if (! grants_check_first(tr->user->grants, perms))
		user_permission_error(user);
}

static inline bool
check_ownership_rel(Tr* tr, Rel* rel)
{
	// user must be the owner of rel
	auto user = user_of(tr->user);
	return user->config->superuser ||
	       str_compare(&user->config->name, rel->user);
}

static inline void
check_ownership_user(Tr* tr, Rel* rel)
{
	if (! check_ownership_rel(tr, rel))
		user_permission_error(user_of(rel));
}

static inline void
check_ownership(Tr* tr, Rel* rel)
{
	if (! check_ownership_rel(tr, rel))
		rel_permission_error(rel);
}

static inline void
check_permission_for(User* user, Rel* rel, uint32_t perms)
{
	// user must have the permissions on the relation
	if (user->config->superuser)
		return;

	if (unlikely(! rel->grants))
		return;

	// owner
	if (str_compare(&user->config->name, rel->user))
		return;

	// check permissions
	if (likely(grants_check(rel->grants, &user->config->name, perms)))
		return;

	rel_permission_error(rel);
}

static inline void
check_permission(Tr* tr, Rel* rel, uint32_t perms)
{
	check_permission_for(user_of(tr->user), rel, perms);
}
