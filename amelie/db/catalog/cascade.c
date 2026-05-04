
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_cdc.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>

void
catalog_validate_udfs(Catalog* self, Str* user, Str* name)
{
	// validate udfs dependencies on the relation
	list_foreach(&self->rels.list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != REL_UDF)
			continue;
		auto udf = udf_of(rel);
		if (self->iface->udf_depends(udf, user, name))
			error("function '{str}' depends on relation '{str}.{str}'",
			      udf->rel.name, user, name);
	}
}

static void
cascade_user_error(Str* user, Rel* dep)
{
	error("user '{str}' still has {s} '{str}'", user,
	      rel_type_of(dep->type), dep->name);
}

static void
cascade_user_drop_execute(Catalog* self, Tr* tr, Str* user, bool cascade)
{
	// validate or drop all objects matching the user
	list_foreach_safe(&self->rels.list)
	{
		auto rel = list_at(Rel, link);
		if (! str_compare(rel->user, user))
			continue;
		if (! cascade)
			cascade_user_error(user, rel);
		catalog_drop_of(self, tr, rel);
	}
}

bool
cascade_user_drop(Catalog* self, Tr* tr, Str* name,
                  bool     cascade,
                  bool     if_exists)
{
	auto user = catalog_find_user(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only user owner can do that or superuser
	check_ownership_user(tr, &user->rel);

	if (user->config->superuser)
		error("user '{str}': system user cannot be dropped", name);

	// invalidate auth caches
	self->iface->user_invalidate(self, user);

	// validate or drop all objects matching the user
	cascade_user_drop_execute(self, tr, name, cascade);

	user_drop(self, tr, name, false);
	return true;
}

static void
cascade_user_rename_execute(Catalog* self, Tr* tr, Str* user, Str* user_new)
{
	/*
	// ensure no udfs depend on the relation
	catalog_validate_udfs(self, user, name);
	*/

	list_foreach_safe(&self->rels.list)
	{
		auto rel = list_at(Rel, link);
		if (! str_compare(rel->user, user))
			continue;

		if (rel->type == REL_UDF)
		{
			auto udf = udf_of(rel);
			error("function '{str}' depends on user '{str}",
			      udf->rel.name, user);
			break;
		}

		catalog_rename_of(self, tr, rel, user_new, rel->name);
	}
}

bool
cascade_user_rename(Catalog* self, Tr* tr,
                    Str*     name,
                    Str*     name_new,
                    bool     if_exists)
{
	auto user = catalog_find_user(self, name, false);
	if (! user)
	{
		if (! if_exists)
			error("user '{str}': not exists", name);
		return false;
	}

	// only user owner can do that or superuser
	check_ownership_user(tr, &user->rel);

	if (user->config->superuser)
		error("user '{str}': system user cannot be renamed", name);

	// rename all user objects
	cascade_user_rename_execute(self, tr, name, name_new);

	// rename user
	user_rename(self, tr, name, name_new, false);
	return true;
}
