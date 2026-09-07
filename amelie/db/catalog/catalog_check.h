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
catalog_check(Catalog* self, Tr* tr, uint32_t perms, Str* ns)
{
	// check user has permissions
	auto user = user_of(tr->user);
	check_user(tr, perms);

	// check target namespace
	if (likely(str_compare(&user->config->name, ns)))
		return;

	// ensure target user exists
	catalog_find_user(self, ns, true);

	// only superuser can create on behalf of other user
	if (! user->config->superuser)
		user_permission_error(user);
}

static inline void
catalog_check_uniqueness(Catalog* self, Uuid* id)
{
	auto rel = rels_find_by(&self->users, REL_UNDEF, id, false);
	if (rel)
		goto error;
	rel = rels_find_by(&self->rels, REL_UNDEF, id, false);
	if (rel)
		goto error;
	return;
error:
	error("relation '{str}.{str}': already exists with the same id",
	      rel->user,
	      rel->name);
}
