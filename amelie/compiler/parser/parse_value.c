
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

hot void
parse_vector(Lex* self, Buf* buf)
{
	// [
	if (! lex_if(self, '['))
		error("invalid vector");

	auto offset = buf_size(buf);
	buf_write_i32(buf, 0);

	// []
	if (lex_if(self, ']'))
		return;

	// [float [, ...]]
	int count = 0;
	for (;;)
	{
		auto ast = lex_next(self);

		// int or float
		float value;
		if (likely(ast->id == KINT))
			value = ast->integer;
		else
		if (ast->id == KREAL)
			value = ast->real;
		else
			error("invalid vector value");
		buf_write_float(buf, value);
		count++;

		// ,
		ast = lex_next(self);
		if (ast->id == ',')
			continue;
		if (ast->id == ']')
			break;
		error("vector array syntax error");
	}

	*(uint32_t*)(buf->start + offset) = count;
}

hot void
parse_value(Lex*    self, Local* local,
            Json*   json,
            Column* column,
            Value*  value)
{
	auto ast = lex_next(self);
	if (ast->id == KNULL)
	{
		value_set_null(value);
		return;
	}
	switch (column->type) {
	case TYPE_BOOL:
		if (ast->id != KTRUE && ast->id != KFALSE)
			break;
		value_set_bool(value, ast->id == KTRUE);
		return;
	case TYPE_INT:
	{
		if (likely(ast->id == KINT))
			value_set_int(value, ast->integer);
		else
		if (ast->id == KREAL)
			value_set_int(value, ast->real);
		else
			break;
		return;
	}
	case TYPE_DOUBLE:
	{
		if (likely(ast->id == KINT))
			value_set_double(value, ast->integer);
		else
		if (ast->id == KREAL)
			value_set_double(value, ast->real);
		else
			break;
		return;
	}
	case TYPE_STRING:
	{
		if (likely(ast->id != KSTRING))
			break;
		value_set_string(value, &ast->string, NULL);
		return;
	}
	case TYPE_JSON:
	{
		// parse and encode json value
		lex_push(self, ast);
		auto pos = self->pos;
		while (self->backlog)
		{
			pos = self->backlog->pos;
			self->backlog = self->backlog->prev;
		}
		json_reset(json);
		Str in;
		str_set(&in, pos, self->end - pos);
		auto buf = buf_create();
		guard_buf(buf);
		json_parse(json, &in, buf);
		self->pos = json->pos;
		unguard();
		value_set_json_buf(value, buf);
		return;
	}
	case TYPE_TIMESTAMP:
	{
		// current_timestamp
		if (ast->id == KCURRENT_TIMESTAMP) {
			value_set_timestamp(value, local->time_us);
			return;
		}

		// unixtime
		if (ast->id == KINT) {
			value_set_timestamp(value, ast->integer);
			return;
		}

		// [TIMESTAMP] string
		if (ast->id == KTIMESTAMP)
			ast = lex_next(self);
		if (likely(ast->id != KSTRING))
			break;

		Timestamp ts;
		timestamp_init(&ts);
		timestamp_read(&ts, &ast->string);
		value_set_timestamp(value, timestamp_of(&ts, local->timezone));
		return;
	}
	case TYPE_INTERVAL:
	{
		// [INTERVAL] string
		if (ast->id == KINTERVAL)
			ast = lex_next(self);
		if (likely(ast->id != KSTRING))
			break;
		Interval iv;
		interval_init(&iv);
		interval_read(&iv, &ast->string);
		value_set_interval(value, &iv);
		return;
	}
	case TYPE_VECTOR:
	{
		// [VECTOR] [array]
		if (ast->id == KVECTOR)
			ast = lex_next(self);
		lex_push(self, ast);
		auto buf = buf_create();
		guard_buf(buf);
		parse_vector(self, buf);
		unguard();
		value_set_vector_buf(value, buf);
		return;
	}
	}

	error("column <%.*s> value expected to be '%s'", str_size(&column->name),
	      str_of(&column->name), type_of(column->type));
}

hot void
parse_value_default(Column*  column,
                    Value*   column_value,
                    uint64_t serial)
{
	// SERIAL, RANDOM or DEFAULT
	auto cons = &column->constraints;
	if (cons->serial)
	{
		value_set_int(column_value, serial);
	} else
	if (cons->random)
	{
		auto value = random_generate(global()->random) % cons->random_modulo;
		value_set_int(column_value, value);
	} else
	{
		value_decode(column_value, cons->value.start, NULL);
	}
}

void
parse_value_validate(Column* column, Value* column_value)
{
	// ensure NOT NULL constraint
	if (column_value->type == TYPE_NULL)
	{
		// value can be NULL for generated column (will be rechecked later)
		if (! str_empty(&column->constraints.as_stored))
			return;

		if (column->constraints.not_null)
			error("column <%.*s> value cannot be NULL", str_size(&column->name),
			      str_of(&column->name));
	}
}
