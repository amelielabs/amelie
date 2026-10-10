
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

static void
describe_type(Column* self, Buf* buf, int flags)
{
	auto cons = &self->constraints;
	unused(flags);

	// type
	switch (self->type) {
	case TYPE_BOOL:
		buf_write(buf, "bool", 4);
		break;
	case TYPE_INT:
		if (self->size == sizeof(int8_t))
			buf_write(buf, "i8", 2);
		else
		if (self->size == sizeof(int16_t))
			buf_write(buf, "i16", 3);
		else
		if (self->size == sizeof(int32_t))
			buf_write(buf, "int", 3);
		else
			buf_write(buf, "i64", 3);
		break;
	case TYPE_DOUBLE:
		if (self->size == sizeof(float))
			buf_write(buf, "float", 5);
		else
			buf_write(buf, "double", 6);
		break;
	case TYPE_DECIMAL:
		buf_format(buf, "decimal({i64}, {i64})",
		           cons->decimal,
		           cons->decimal_scale);
		break;
	case TYPE_DATE:
		buf_write(buf, "date", 4);
		break;
	case TYPE_TIMESTAMP:
		buf_write(buf, "timestamp", 9);
		break;
	case TYPE_INTERVAL:
		buf_write(buf, "interval", 8);
		break;
	case TYPE_UUID:
		buf_write(buf, "uuid", 4);
		break;
	case TYPE_STRING:
		buf_write(buf, "text", 4);
		break;
	case TYPE_JSON:
		buf_write(buf, "json", 4);
		break;
	case TYPE_VECTOR:
		buf_format(buf, "vector({d})", self->size_flat / sizeof(float));
		break;
	default:
		abort();
	}
}

static void
describe_column(Column* self, Buf* buf, int flags)
{
	// name
	buf_format(buf, "{str} ", &self->name);

	// type
	describe_type(self, buf, flags);

	// constraints
	auto cons = &self->constraints;

	// not null
	if (cons->not_null)
		buf_format(buf, " not null");

	// identity
	if (cons->identity)
		buf_format(buf, " identity");

	// [(mod)]
	if (cons->identity_modulo != INT64_MAX)
		buf_format(buf, "({i64})", cons->identity_modulo);

	// default
	if (! buf_empty(&cons->value))
	{
		buf_format(buf, " default ");

		auto value = buf_create();
		defer_buf(value);
		row_encode_column(cons->value.start, NULL, self, runtime()->timezone, value);

		auto pos = value->start;
		json_export(buf, runtime()->timezone, &pos);
	}

	// drop (support dropped columns)
	if (self->dropped)
		buf_format(buf, " drop");
}

static void
describe_grants(Grants* self, Buf* buf)
{
	if (grants_empty(self))
		return;

	auto grant = grants_first(self);
	for (; grant; grant = grants_next(self, grant))
	{
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
		buf_format(buf, " to {.*s}", grant->name_size, grant->name);
	}
}

static void
describe_grants_self(Grants* self, Buf* buf)
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

static void
describe_table(Table* self, Buf* buf, Str* user, int flags)
{
	auto verbose = !flags_has(flags, FMINIMAL);
	auto config  = self->config;

	// create table
	if (!verbose && str_compare_case(self->rel.user, user))
		buf_format(buf, "create table {str} (\n", &config->name);
	else
		buf_format(buf, "create table {str}.{str} (\n", &config->user,
		           &config->name);

	// (columns)
	list_foreach(&config->columns.list)
	{
		auto column = list_at(Column, link);
		buf_write(buf, "  ", 2);
		describe_column(column, buf, flags);
		buf_write(buf, ",\n", 2);
	}

	// primary key
	auto primary = table_primary(self);
	if (primary)
	{
		auto keys = &primary->keys;
		buf_format(buf, "  primary key (");
		for (auto at = 0; at < keys->count; at++)
		{
			auto key = keys_at(keys, at);
			if (at > 0)
				buf_write(buf, ", ", 2);
			buf_format(buf, "{str}", &key->column->name);
		}
		buf_write(buf, ")", 1);

		// using type
		if (primary->type == INDEX_HASH)
			buf_write(buf, " using hash,\n", 13);
		else
			buf_write(buf, " using tree,\n", 13);
	}

	// partition key
	auto keys = &self->config->partitioning;
	buf_format(buf, "  partition key (");
	for (auto at = 0; at < keys->count; at++)
	{
		auto key = keys_at(keys, at);
		if (at > 0)
			buf_write(buf, ", ", 2);
		buf_format(buf, "{str}", &key->column->name);
	}
	buf_write(buf, ")", 1);
	buf_write(buf, "\n)\n", 3);

	// id
	if (verbose)
	{
		char id[UUID_SZ];
		uuid_get(&config->id, id, sizeof(id));
		buf_format(buf, "  id {qs}\n", id);
	}

	// description
	if (! str_empty(&config->description))
		buf_format(buf, "  description {qstr}\n", &config->description);

	// partitions
	buf_format(buf, "  partitions {d}\n", config->parts_count);

	if (! verbose)
		return;

	// secondary indexes
	list_foreach(&config->indexes)
	{
		auto index = list_at(IndexConfig, link);
		if (index == primary)
			continue;

		// [unique] index
		if (index->unique)
			buf_write(buf, "  unique index", 14);
		else
			buf_write(buf, "  index ", 7);
		buf_format(buf, " {str} (", &index->name);

		// (keys)
		keys = &index->keys;
		for (auto at = 0; at < keys->count; at++)
		{
			auto key = keys_at(keys, at);
			if (at > 0)
				buf_write(buf, ", ", 2);
			buf_format(buf, "{str}", &key->column->name);
		}
		buf_write(buf, ")", 1);

		// using hash
		if (index->type == INDEX_HASH)
			buf_write(buf, " using hash", 11);
		buf_write(buf, "\n", 1);
	}

	// grants
	describe_grants(&self->config->grants, buf);
}

static void
describe_sidetable(Sidetable* self, Buf* buf, Str* user, int flags)
{
	auto verbose = !flags_has(flags, FMINIMAL);
	auto config = self->config;

	// create table
	if (!verbose && str_compare_case(self->rel.user, user))
		buf_format(buf, "create table {str}",  &config->name);
	else
		buf_format(buf, "create table {str}.{str}",
		           &config->user, &config->name);

	// on
	if (!verbose && str_compare_case(&config->table_user, user))
		buf_format(buf, " on {str}\n", &config->table);
	else
		buf_format(buf, " on {str}.{str}\n",
		           &config->table_user, &config->table);

	// description
	if (! str_empty(&config->description))
		buf_format(buf, "  description {qstr}\n", &config->description);

	if (! verbose)
		return;

	// timeline
	buf_format(buf, "  timeline {i64}\n", config->timeline.timeline);

	// grants
	describe_grants(&self->config->grants, buf);
}

static void
describe_udf(Udf* self, Buf* buf, Str* user, int flags)
{
	auto verbose = !flags_has(flags, FMINIMAL);
	auto config = self->config;

	// create function
	if (!verbose && str_compare_case(self->rel.user, user))
		buf_format(buf, "create function {str} ", &config->name);
	else
		buf_format(buf, "create function {str}.{str} ", &config->user,
		           &config->name);

	// (args)
	buf_write(buf, "(", 1);
	list_foreach(&config->args.list)
	{
		auto column = list_at(Column, link);
		describe_column(column, buf, flags);
		if (! list_is_last(&config->args.list, &column->link))
			buf_write(buf, ", ", 2);
	}
	buf_write(buf, ")", 1);

	// return
	if (config->type == TYPE_STORE)
	{
		buf_format(buf, " return table");

		// (args)
		buf_write(buf, "(", 1);
		list_foreach(&config->returning.list)
		{
			auto column = list_at(Column, link);
			describe_column(column, buf, flags);
			if (! list_is_last(&config->returning.list, &column->link))
				buf_write(buf, ", ", 2);
		}
		buf_write(buf, ")", 1);

	} else 
	if (config->type != TYPE_NULL)
	{
		buf_format(buf, " return {s}", type_of(config->type));
	}
	buf_write(buf, "\n", 1);

	// description
	if (! str_empty(&config->description))
		buf_format(buf, "  description {qstr}\n", &config->description);

	// begin text end
	buf_write_str(buf, &config->text);

	if (! verbose)
		return;

	// grants
	describe_grants(&self->config->grants, buf);
}

static void
describe_user(User* self, Buf* buf, Str* user, int flags)
{
	auto verbose = !flags_has(flags, FMINIMAL);
	auto config = self->config;

	// create user
	if (config->agent)
		buf_format(buf, "create agent ");
	else
		buf_format(buf, "create user ");

	// [parent.]name
	if (!verbose && str_compare_case(self->rel.user, user))
		buf_format(buf, "{str}\n", &config->name);
	else
		buf_format(buf, "{str}.{str}\n", &config->parent,
		           &config->name);

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
	describe_grants_self(&self->config->grants, buf);

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
			buf_format(buf, "on {str}.{str}\n",
			           &api->rel_user, &api->rel);
	}
}

void
describe_text(Rel* self, Buf* buf, Str* user, int flags)
{
	unused(user);

	switch (self->type) {
	case REL_TABLE:
		describe_table(table_of(self), buf, user, flags);
		break;
	case REL_SIDETABLE:
		describe_sidetable(sidetable_of(self), buf, user, flags);
		break;
	case REL_UDF:
		describe_udf(udf_of(self), buf, user, flags);
		break;
	case REL_USER:
		describe_user(user_of(self), buf, user, flags);
		break;
	default:
		abort();
		break;
	}
	if (!buf_empty(buf) && buf->position[-1] == '\n')
		buf_truncate(buf, 1);

	buf_write(buf, ";", 1);
}

void
describe(Rel* self, Buf* buf, Str* user, int flags)
{
	auto offset = buf_size(buf);
	encode_str32(buf, 0);

	// generate schema
	describe_text(self, buf, user, flags);

	// update generated string size
	auto start = buf->start + offset;
	pack_str32(&start, buf_size(buf) - (offset + data_size_str32()));
}

static void
describe_rels(Rels* rels, RelType type, Buf* buf)
{
	Str user;
	str_set(&user, "amelie", 6);
	list_foreach(&rels->list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != type)
			continue;
		if (rel->type == REL_USER && user_of(rel)->config->superuser)
			continue;
		describe_text(rel, buf, &user, 0);
		buf_write(buf, "\n\n", 2);
	}
}

void
describe_catalog(Catalog* self, Buf* buf)
{
	// users, tables, sidetables, udfs
	describe_rels(&self->users, REL_USER, buf);
	describe_rels(&self->rels, REL_TABLE, buf);
	describe_rels(&self->rels, REL_SIDETABLE, buf);
	describe_rels(&self->rels, REL_UDF, buf);
}
