
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

	// subs
	list_foreach_safe(&self->sub_mgr.mgr.list)
	{
		auto sub = sub_of(list_at(Rel, link));
		if (! str_compare_case(&sub->config->user, user))
			continue;
		if (drop)
			sub_mgr_drop_of(&self->sub_mgr, tr, sub);
		else
			cascade_user_error(user);
	}

	// udfs
	list_foreach_safe(&self->udf_mgr.mgr.list)
	{
		auto udf = udf_of(list_at(Rel, link));
		if (! str_compare_case(&udf->config->user, user))
			continue;
		if (drop)
			udf_mgr_drop_of(&self->udf_mgr, tr, udf);
		else
			cascade_user_error(user);
	}

	// branches
	list_foreach_safe(&self->branch_mgr.mgr.list)
	{
		auto branch = branch_of(list_at(Rel, link));
		if (! str_compare_case(&branch->config->user, user))
			continue;
		if (drop)
			branch_mgr_drop_of(&self->branch_mgr, tr, branch);
		else
			cascade_user_error(user);
	}

	// tables
	list_foreach_safe(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Rel, link));
		if (! str_compare_case(&table->config->user, user))
			continue;
		if (drop)
			table_mgr_drop_of(&self->table_mgr, tr, table);
		else
			cascade_user_error(user);
	}
}

bool
cascade_user_drop(Catalog* self, Tr* tr, Str* name,
                  bool     cascade,
                  bool     if_exists)
{
	auto user = user_mgr_find(&self->user_mgr, name, false);
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

	user_mgr_drop(&self->user_mgr, tr, name, false);
	return true;
}

static void
cascade_user_rename_execute(Catalog* self, Tr* tr, Str* user, Str* user_new)
{
	// udfs
	list_foreach_safe(&self->udf_mgr.mgr.list)
	{
		auto udf = udf_of(list_at(Rel, link));
		if (str_compare_case(&udf->config->user, user))
			error("function '%.*s' depends on user '%.*s",
			      str_size(udf->rel.name), str_of(udf->rel.name),
			      str_size(user), str_of(user));
	}

	// subs
	list_foreach_safe(&self->sub_mgr.mgr.list)
	{
		auto sub = sub_of(list_at(Rel, link));
		if (str_compare_case(&sub->config->user, user))
			sub_mgr_rename(&self->sub_mgr, tr, &sub->config->user,
			               &sub->config->name,
			               user_new,
			               &sub->config->name, false);
	}

	// branches
	list_foreach_safe(&self->branch_mgr.mgr.list)
	{
		auto branch = branch_of(list_at(Rel, link));
		if (str_compare_case(&branch->config->user, user))
			branch_mgr_rename(&self->branch_mgr, tr, &branch->config->user,
			                  &branch->config->name,
			                  user_new,
			                  &branch->config->name, false);
	}

	// tables
	list_foreach_safe(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Rel, link));
		if (str_compare_case(&table->config->user, user))
			table_mgr_rename(&self->table_mgr, tr, &table->config->user,
			                 &table->config->name,
			                 user_new,
			                 &table->config->name, false);
	}

}

bool
cascade_user_rename(Catalog* self, Tr* tr,
                    Str*     name,
                    Str*     name_new,
                    bool     if_exists)
{
	auto user = user_mgr_find(&self->user_mgr, name, false);
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
	user_mgr_rename(&self->user_mgr, tr, name, name_new, false);
	return true;
}
