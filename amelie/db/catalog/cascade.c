
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
cascade_user_error(Str* user)
{
	error("user '%.*s' still has relations", str_size(user),
	      str_of(user));
}

static void
cascade_user_drop_execute(Catalog* self, Tr* tr, Str* user, bool drop)
{
	// validate or drop all objects matching the user
	list_foreach_safe(&self->rels.list)
	{
		auto rel = list_at(Rel, link);
		if (! str_compare_case(rel->user, user))
			continue;
		if (! drop)
			cascade_user_error(user);
		switch (rel->type) {
		case REL_TOPIC:
			topic_drop_of(self, tr, topic_of(rel));
			break;
		case REL_SUBSCRIPTION:
			sub_drop_of(self, tr, sub_of(rel));
			break;
		case REL_UDF:
			udf_drop_of(self, tr, udf_of(rel));
			break;
		case REL_BRANCH:
			branch_drop_of(self, tr, branch_of(rel));
			break;
		case REL_TABLE:
			table_drop_of(self, tr, table_of(rel));
			break;
		default:
			abort();
			break;
		}
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
			error("user '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only user owner can do that or superuser
	check_ownership_user(tr, &user->rel);

	if (user->config->superuser)
		error("user '%.*s': system user cannot be dropped", str_size(name),
		      str_of(name));

	// validate or drop all objects matching the user
	cascade_user_drop_execute(self, tr, name, cascade);

	user_drop(self, tr, name, false);
	return true;
}

static void
cascade_user_rename_execute(Catalog* self, Tr* tr, Str* user, Str* user_new)
{
	list_foreach_safe(&self->rels.list)
	{
		auto rel = list_at(Rel, link);
		if (! str_compare_case(rel->user, user))
			continue;
		switch (rel->type) {
		case REL_UDF:
		{
			auto udf = udf_of(rel);
			error("function '%.*s' depends on user '%.*s",
			      str_size(udf->rel.name), str_of(udf->rel.name),
			      str_size(user), str_of(user));
			break;
		}
		case REL_TOPIC:
		{
			auto topic = topic_of(rel);
			topic_rename(self, tr, &topic->config->user,
			             &topic->config->name,
			             user_new,
			             &topic->config->name, false);
			break;
		}
		case REL_SUBSCRIPTION:
		{
			auto sub = sub_of(rel);
			sub_rename(self, tr, &sub->config->user,
			           &sub->config->name,
			           user_new,
			           &sub->config->name, false);
			break;
		}
		case REL_BRANCH:
		{
			auto branch = branch_of(rel);
			branch_rename(self, tr, &branch->config->user,
			              &branch->config->name,
			              user_new,
			              &branch->config->name, false);
			break;
		}
		case REL_TABLE:
		{
			auto table = table_of(rel);
			table_rename(self, tr, &table->config->user,
			             &table->config->name,
			             user_new,
			             &table->config->name, false);
			break;
		}
		default:
			abort();
			break;
		}
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
			error("user '%.*s': not exists", str_size(name),
			      str_of(name));
		return false;
	}

	// only user owner can do that or superuser
	check_ownership_user(tr, &user->rel);

	if (user->config->superuser)
		error("user '%.*s': system user cannot be renamed", str_size(name),
		      str_of(name));

	// rename all user objects
	cascade_user_rename_execute(self, tr, name, name_new);

	// rename user
	user_rename(self, tr, name, name_new, false);
	return true;
}
