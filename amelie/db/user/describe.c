
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_user.h>

#if 0
static void
describe_amelie(Catalog* self, Buf* buf)
{
	Str user;
	str_set(&user, "amelie", 6);
	auto amelie = catalog_find_user(self, &user, true);

	// apis
	list_foreach(&amelie->config->apis.list)
	{
		auto api = list_at(Api, link);
		buf_format(buf, "create api {qstr} ", &api->uri);
		if (api->mcp)
			buf_format(buf, "as mcp\n");
		else
			buf_format(buf, "on {str}.{str}\n",
			           &api->rel_user, &api->rel);
		buf_write(buf, ";", 1);
	}
}
#endif

static void
describe_grants(Grants* self, Buf* buf)
{
	if (grants_empty(self))
		return;

	auto grant = grants_first(self);
	buf_write(buf, "  grant ", 8);

	auto n = 0;
	auto permissions = grant->permissions;
	while (permissions > 0)
	{
		auto id = permission_next(&permissions);
		if (n > 0)
			buf_write(buf, ", ", 2);
		buf_format(buf, "{s}", permission_name_of(id));
		n++;
	}
}

void
describe_user(User* self, Buf* buf, int flags)
{
	auto verbose = !flags_has(flags, FMINIMAL);
	auto config = self->config;

	// create user
	if (config->agent)
		buf_format(buf, "create agent ");
	else
		buf_format(buf, "create user ");

	// name
	buf_format(buf, "{str}\n", &config->name);

	// description
	if (! str_empty(&config->description))
		buf_format(buf, "  description {qstr}\n", &config->description);

	if (! verbose)
		return;

	// created
	if (! str_empty(&config->created_at))
		buf_format(buf, "  created {qstr}\n", &config->created_at);

	// revoked
	if (! str_empty(&config->revoked_at))
		buf_format(buf, "  revoked {qstr}\n", &config->revoked_at);

	// limit name = value, ...
	auto limit_clause = false;	
	auto limits = &config->limits;
	for (auto i = 0; i < LIMIT_MAX; i++)
	{
		if (! limits_is_set(limits, i))
			continue;

		if (! limit_clause)
		{
			buf_format(buf, "  limit ");
			limit_clause = true;
		} else {
			buf_format(buf, ", ");
		}
		buf_format(buf, "{s} = {i64}", limits_of(i),
		           limits->limits[i]);
	}
	if (limit_clause)
		buf_write(buf, "\n", 1);

	// grants
	describe_grants(&self->config->grants, buf);

	if (buf->position[-1] != '\n')
		buf_write(buf, "\n", 1);

	// apis
	list_foreach(&self->config->apis.list)
	{
		auto api = list_at(Api, link);
		buf_format(buf, "  api {qstr} ", &api->uri);
		if (api->mcp)
			buf_format(buf, "as mcp\n");
		else
			buf_format(buf, "on {str}\n", &api->rel);
	}
}
