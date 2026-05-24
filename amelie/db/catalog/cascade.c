
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
		case REL_CLONE:
		{
			auto clone = clone_of(at);
			if (rel->type == REL_TABLE)
			{
				// drop clone if tables matches is
				add = clone->table == table_of(rel);
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
		case REL_CLONE:
		{
			// clone depends on the table
			auto clone =clone_of(at);
			if (rel->type == REL_TABLE)
			{
				dep = clone->table == table_of(rel);
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

bool
catalog_deps_validate_user(Catalog* self, Str* user, bool error_on_match)
{
	// no recursion
	list_foreach(&self->rels.list)
	{
		auto at  = list_at(Rel, link);
		auto dep = false;
		switch (at->type) {
		case REL_UDF:
		{
			// udf depends on the user
			auto udf = udf_of(at);
			dep = self->iface->udf_depends(udf, user, NULL);
			break;
		}
		case REL_CLONE:
		{
			// clone depends on the user
			auto clone = clone_of(at);
			dep = str_compare(&clone->config->table_user, user);
			break;
		}
		default:
			break;
		}

		if (dep)
		{
			if (error_on_match)
			{
				error("{s} '{str}.{str}' depends on user '{str}'",
				      rel_type_of(at->type), at->user, at->name,
				      user);
			}
			return false;
		}
	}

	// ensure not child users
	list_foreach(&self->users.list)
	{
		auto at = list_at(Rel, link);
		if (str_compare(at->user, user))
			error("user '{str}' depends on user '{str}'",
			      at->user, user);
	}
	return true;
}
