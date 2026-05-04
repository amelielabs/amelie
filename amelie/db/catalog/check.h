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
user_permission_error(User* self)
{
	error("user '{str}': permission denied", &self->config->name);
}

static inline bool
user_owns(User* self, Rel* rel)
{
	return self->config->superuser ||
	       str_compare(&self->config->name, rel->user);
}

static inline void
user_check(User* self, uint32_t perms)
{
	if (self->config->superuser)
		return;
	if (! grants_check_first(&self->config->grants, perms))
		user_permission_error(self);
}

hot static inline void
user_check_permission(User* self, Rel* rel, uint32_t perms)
{
	// user must have the permissions on the relation
	if (self->config->superuser)
		return;

	if (unlikely(! rel->grants))
		return;

	// owner
	if (str_compare(&self->config->name, rel->user))
		return;

	// check permissions
	if (likely(grants_check(rel->grants, &self->config->name, perms)))
		return;

	rel_permission_error(rel);
}

hot static inline void
user_check_access(User* self, Access* access)
{
	// check permissions on the relations
	if (self->config->superuser)
		return;
	for (auto i = 0; i < access->list_count; i++)
	{
		auto record = access_at(access, i);
		if (record->perm == PERM_NONE)
			continue;
		user_check_permission(self, record->rel, record->perm);
	}
}

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
