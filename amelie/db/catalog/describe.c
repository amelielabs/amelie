
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

static void
describe_type(Column* self, Buf* buf, int flags)
{
	auto cons = &self->constraints;
	unused(flags);

	// type
	switch (self->type) {
	case TYPE_BOOL:
		buf_format(buf, "bool");
		break;
	case TYPE_INT:
		if (self->size == sizeof(int8_t))
			buf_format(buf, "i8");
		else
		if (self->size == sizeof(int16_t))
			buf_format(buf, "i16");
		else
		if (self->size == sizeof(int32_t))
			buf_format(buf, "int");
		else
			buf_format(buf, "i64");
		break;
	case TYPE_DOUBLE:
		if (self->size == sizeof(float))
			buf_format(buf, "float");
		else
			buf_format(buf, "double");
		break;
	case TYPE_DECIMAL:
		buf_format(buf, "decimal({i64}, {i64})",
		           cons->decimal,
		           cons->decimal_scale);
		break;
	case TYPE_DATE:
		buf_format(buf, "date");
		break;
	case TYPE_TIMESTAMP:
		buf_format(buf, "timestamp");
		break;
	case TYPE_INTERVAL:
		buf_format(buf, "interval");
		break;
	case TYPE_UUID:
		buf_format(buf, "uuid");
		break;
	case TYPE_STRING:
		buf_format(buf, "text");
		break;
	case TYPE_JSON:
		buf_format(buf, "json");
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
		// todo:
	}

	// drop (support dropped columns)
	if (self->dropped)
		buf_format(buf, " drop");
}

static void
describe_table(Table* self, Buf* buf, int flags)
{
	auto config = self->config;

	// create table
	buf_format(buf, "create table {str}.{str}", &config->user,
	           &config->name);

	// (columns)
	buf_write(buf, "(", 1);
	list_foreach(&config->columns.list)
	{
		auto column = list_at(Column, link);
		describe_column(column, buf, flags);
		if (! list_is_last(&config->columns.list, &column->link))
			buf_write(buf, ", ", 2);
	}

	// primary key ((partitioning key), key)
	buf_format(buf, ", primary key((");

	auto partitioning = true;
	auto keys = table_keys(self);
	for (auto at = 0; at < keys->count; at++)
	{
		auto key = keys_at(keys, at);
		if (partitioning && !key->partitioning)
		{
			buf_format(buf, ")");
			partitioning = false;
		}
		if (at > 0)
			buf_write(buf, ", ", 2);
		buf_format(buf, "{str}", &key->column->name);
	}
	if (partitioning)
		buf_write(buf, ")", 1);

	buf_write(buf, "))", 2);

	// id
	char id[UUID_SZ];
	uuid_get(&config->id, id, sizeof(id));
	buf_format(buf, " id {qs}", id);

	// description
	if (! str_empty(&config->description))
		buf_format(buf, " description {qstr}", &config->description);

	// partitions
	buf_format(buf, " partitions {d}", config->parts_count);

	// timeline
	buf_format(buf, " timeline {i64}", config->timeline);
}

static void
describe_clone(Clone* self, Buf* buf, int flags)
{
	auto config = self->config;
	unused(flags);

	// create clone
	buf_format(buf, "create clone {str}.{str} of {str}.{str}",
	           &config->user, &config->name,
	           &config->table_user, &config->table);

	// id
	char id[UUID_SZ];
	uuid_get(&config->id, id, sizeof(id));
	buf_format(buf, " id {qs}", id);

	// description
	if (! str_empty(&config->description))
		buf_format(buf, " description {qstr}", &config->description);

	// timeline
	buf_format(buf, " timeline {i64}", config->timeline.timeline);
}

static void
describe_topic(Topic* self, Buf* buf, int flags)
{
	auto config = self->config;
	unused(flags);

	// create topic
	buf_format(buf, "create topic {str}.{str}", &config->user,
	           &config->name);

	// id
	char id[UUID_SZ];
	uuid_get(&config->id, id, sizeof(id));
	buf_format(buf, " id {qs}", id);

	// description
	if (! str_empty(&config->description))
		buf_format(buf, " description {qstr}", &config->description);
}

static void
describe_subscription(Sub* self, Buf* buf, int flags)
{
	auto config = self->config;
	unused(flags);

	// create subscription
	buf_format(buf, "create subscription {str}.{str} on {str}.{str}",
	           &config->user,
	           &config->name,
	           &config->rel_user,
	           &config->rel);

	// description
	if (! str_empty(&config->description))
		buf_format(buf, " description {qstr}", &config->description);

	// lsn
	buf_format(buf, " lsn {i64}", config->lsn);
}

static void
describe_udf(Udf* self, Buf* buf, int flags)
{
	auto config = self->config;
	unused(flags);

	// create function
	buf_format(buf, "create function {str}.{str}", &config->user,
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

	// description
	if (! str_empty(&config->description))
		buf_format(buf, " description {qstr}", &config->description);

	// begin text end
	buf_format(buf, " begin ");
	buf_write_str(buf, &config->text);
	buf_format(buf, " end");
}

static void
describe_user(User* self, Buf* buf, int flags)
{
	auto config = self->config;
	unused(flags);

	// create user
	if (config->agent)
		buf_format(buf, "create agent {str}.{str}", &config->parent,
		           &config->name);
	else
		buf_format(buf, "create user {str}.{str}", &config->parent,
		           &config->name);

	// id
	char id[UUID_SZ];
	uuid_get(&config->id, id, sizeof(id));
	buf_format(buf, " id {qs}", id);

	// description
	if (! str_empty(&config->description))
		buf_format(buf, " description {qstr}", &config->description);

	// created
	if (! str_empty(&config->created_at))
		buf_format(buf, " created {qstr}", &config->created_at);

	// revoked
	if (! str_empty(&config->revoked_at))
		buf_format(buf, " revoked {qstr}", &config->revoked_at);

	// limit name = value, ...
	auto limit_clause = false;	
	auto limits = &config->limits;
	for (auto i = 0; i < LIMIT_MAX; i++)
	{
		if (! limits_is_set(limits, i))
			continue;

		if (! limit_clause)
		{
			buf_format(buf, " limit ");
			limit_clause = true;
		} else {
			buf_format(buf, ", ");
		}
		buf_format(buf, "{s} = {i64}", limits_of(i),
		           limits->limits[i]);
	}
}

void
describe(Rel* self, Buf* buf, int flags)
{
	auto offset = buf_size(buf);
	encode_str32(buf, 0);

	switch (self->type) {
	case REL_TABLE:
		describe_table(table_of(self), buf, flags);
		break;
	case REL_CLONE:
		describe_clone(clone_of(self), buf, flags);
		break;
	case REL_TOPIC:
		describe_topic(topic_of(self), buf, flags);
		break;
	case REL_SUBSCRIPTION:
		describe_subscription(sub_of(self), buf, flags);
		break;
	case REL_UDF:
		describe_udf(udf_of(self), buf, flags);
		break;
	case REL_USER:
		describe_user(user_of(self), buf, flags);
		break;
	default:
		abort();
		break;
	}
	buf_write(buf, ";", 1);

	// update generated string size
	auto start = buf->start + offset;
	pack_str32(&start, buf_size(buf) - (offset + data_size_str32()));
}
