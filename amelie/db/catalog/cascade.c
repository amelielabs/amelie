
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_transaction.h>
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

hot static inline bool
catalog_depends(Catalog* self, Rel* rel, Rel* at)
{
	auto dep = false;
	switch (rel->type) {
	case REL_TABLE:
	{
		if (at->type == REL_CLONE)
		{
			// clone depends on the table
			auto clone = clone_of(at);
			dep = clone->table == table_of(rel);
		} else
		if (at->type == REL_UDF)
		{
			// udf depends on the table
			auto udf = udf_of(at);
			dep = self->iface->udf_depends(udf, rel->user, rel->name);
		} else
		if (at->type == REL_SUBSCRIPTION)
		{
			// subscription depends on the table
			dep = rel == sub_of(at)->rel_on;
		}
		break;
	}
	case REL_TOPIC:
	case REL_CLONE:
	{
		if (at->type == REL_UDF)
		{
			// udf depends on the rel
			auto udf = udf_of(at);
			dep = self->iface->udf_depends(udf, rel->user, rel->name);
		} else
		if (at->type == REL_SUBSCRIPTION)
		{
			// subscription depends on the rel
			dep = rel == sub_of(at)->rel_on;
		}
		break;
	}
	case REL_SUBSCRIPTION:
	{
		if (at->type == REL_UDF)
		{
			// udf depends on the subscription
			auto udf = udf_of(at);
			dep = self->iface->udf_depends(udf, rel->user, rel->name);
		}
		break;
	}
	case REL_UDF:
	{
		if (at->type == REL_UDF)
		{
			// udf depends on the udf
			auto udf = udf_of(at);
			dep = self->iface->udf_depends(udf, rel->user, rel->name);
		}
		break;
	}
	default:
		break;
	}
	return dep;
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

		if (! catalog_depends(self, rel, at))
			continue;

		if (catalog_deps_add(list, at))
			count += catalog_deps(self, at, list) + 1;
	}
	return count;
}

void
catalog_deps_drop(Catalog* self, Tr* tr, Buf* list)
{
	auto it  = (Rel**)list->start;
	auto end = (Rel**)list->position;
	for (; it < end; it++)
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
		if (catalog_depends(self, rel, at))
		{
			if (error_on_match)
				error("{s} '{str}.{str}' depends on {s} '{str}'",
				      rel_type_of(at->type), at->user, at->name,
				      rel_type_of(rel->type), rel->name);
			return false;
		}
	}
	return true;
}

bool
catalog_deps_validate_udf(Catalog* self, Rel* rel, bool error_on_match)
{
	// no recursion
	list_foreach(&self->rels.list)
	{
		auto at = list_at(Rel, link);
		if (at == rel)
			continue;

		if (at->type != REL_UDF)
			continue;

		// udf depends on the relation
		auto udf = udf_of(at);
		auto dep = self->iface->udf_depends(udf, rel->user, rel->name);
		if (dep)
		{
			if (error_on_match)
				error("{s} '{str}.{str}' depends on {s} '{str}'",
				      rel_type_of(at->type), at->user, at->name,
				      rel_type_of(rel->type), rel->name);
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
		case REL_SUBSCRIPTION:
		{
			// subscription rel depends on the user
			auto sub = sub_of(at);
			dep = str_compare(&sub->config->rel_user, user);
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
