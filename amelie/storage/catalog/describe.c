
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
#include <amelie_heap>
#include <amelie_transaction.h>
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
describe_table(Table* self, Buf* buf, int flags)
{
	auto verbose = !flags_has(flags, FMINIMAL);
	auto config  = self->config;

	// create table
	buf_format(buf, "create table {str}\n(\n", &config->name);

	// (columns)
	list_foreach(&config->columns.list)
	{
		auto column = list_at(Column, link);
		buf_write(buf, "  ", 2);
		describe_column(column, buf, flags);
		if (! list_is_last(&config->columns.list, &column->link))
			buf_write(buf, ",\n", 2);
	}

	// primary key
	auto primary = table_primary(self);
	if (primary)
	{
		auto keys = &primary->keys;
		buf_format(buf, ",\n  primary key (");
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
			buf_write(buf, " using hash\n", 12);
		else
			buf_write(buf, " using tree\n", 12);
	} else {
		// last column
		buf_write(buf, "\n", 1);
	}
	buf_write(buf, ")", 1);

	// description
	if (! str_empty(&config->description))
		buf_format(buf, "  description {qstr}\n", &config->description);

	// retention
	if (config->retention > 0)
		buf_format(buf, "  retention {i64}\n", config->retention);

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
		auto keys = &index->keys;
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
}

static void
describe_udf(Udf* self, Buf* buf, int flags)
{
	auto verbose = !flags_has(flags, FMINIMAL);
	auto config = self->config;

	// create function
	buf_format(buf, "create function {str} ", &config->name);

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
}

static void
describe_db(Db* self, Buf* buf, int flags)
{
	auto verbose = !flags_has(flags, FMINIMAL);
	auto config = self->config;

	// create database
	buf_format(buf, "create database {str}\n", &config->name);

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

	if (buf->position[-1] != '\n')
		buf_write(buf, "\n", 1);
}

void
describe_text(Rel* self, Buf* buf, int flags)
{
	switch (self->type) {
	case REL_DB:
		describe_db(db_of(self), buf, flags);
		break;
	case REL_TABLE:
		describe_table(table_of(self), buf, flags);
		break;
	case REL_UDF:
		describe_udf(udf_of(self), buf, flags);
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
describe(Rel* self, Buf* buf, int flags)
{
	auto offset = buf_size(buf);
	encode_str32(buf, 0);

	// generate schema
	describe_text(self, buf, flags);

	// update generated string size
	auto start = buf->start + offset;
	pack_str32(&start, buf_size(buf) - (offset + data_size_str32()));
}

static void
describe_rels(Rels* rels, RelType type, Buf* schema)
{
	list_foreach(&rels->list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != type)
			continue;
		describe_text(rel, schema, 0);
		buf_write(schema, "\n\n", 2);
	}
}

void
describe_catalog(Catalog* self, Buf* schema)
{
	// tables, udfs
	describe_rels(&self->rels, REL_TABLE, schema);
	describe_rels(&self->rels, REL_UDF, schema);
}
