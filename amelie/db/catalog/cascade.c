
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

hot bool
catalog_deps_add(Buf* list, Rel* rel)
{
	auto it  = (Rel**)list->start;
	auto end = (Rel**)list->position;
	for (; it < end; it++)
		if (*it == rel)
			return false;
	buf_write(list, &rel, sizeof(Rel**));
	return true;
}

hot int
catalog_deps(Catalog* self, Rel* rel, Buf* list)
{
	auto count = 0;
	list_foreach(&self->rels.list)
	{
		auto at = list_at(Rel, link);
		if (at == rel)
			continue;

		auto add = false;
		switch (at->type) {
		case REL_UDF:
		{
			auto udf = udf_of(at);
			add = self->iface->udf_depends(udf, rel->user, rel->name);
			break;
		}
		case REL_BRANCH:
		{
			auto branch = branch_of(at);
			if (rel->type == REL_TABLE)
			{
				// drop branch if tables matches is
				add = branch->table == table_of(rel);
				break;
			}
			if (rel->type == REL_BRANCH)
			{
				// drop branch if branch is a child
				auto ref = branch_of(rel);
				if (branch->table != ref->table)
					break;
				add = branch->config->snapshot.id_parent == ref->config->snapshot.id;
				break;
			}
			break;
		}
		default:
			break;
		}

		if (! add)
			continue;

		if (catalog_deps_add(list, at))
			count += catalog_deps(self, at, list) + 1;
	}
	return count;
}

void
catalog_deps_drop(Catalog* self, Tr* tr, Buf* list)
{
	auto it  = (Rel**)list->position - 1;
	auto end = (Rel**)list->start;
	for (; it >= end; it--)
		catalog_drop_of(self, tr, *it);
}

bool
catalog_deps_validate(Catalog* self, Rel* rel, bool error_on_match)
{
	// no recursion
	list_foreach(&self->rels.list)
	{
		auto at = list_at(Rel, link);
		if (at == rel)
			continue;

		auto dep = false;
		switch (at->type) {
		case REL_UDF:
		{
			// udf depends on the relation
			auto udf = udf_of(at);
			dep = self->iface->udf_depends(udf, rel->user, rel->name);
			break;
		}
		case REL_BRANCH:
		{
			// branch depends on the relation (which is table or branch)
			auto branch = branch_of(at);
			if (rel->type == REL_TABLE)
			{
				dep = branch->table == table_of(rel);
				break;
			}
			if (rel->type == REL_BRANCH)
			{
				auto ref = branch_of(rel);
				if (branch->table != ref->table)
					break;
				dep = branch->config->snapshot.id_parent == ref->config->snapshot.id;
				break;
			}
			break;
		}
		default:
			break;
		}

		if (dep)
		{
			if (error_on_match)
			{
				error("{s} '{str}.{str}' depends on {s} '{str}'",
				      rel_type_of(at->type), at->user, at->name,
				      rel_type_of(rel->type), rel->name);
			}
			return false;
		}
	}
	return true;
}
