
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
#include <amelie_data.h>
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
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

hot void
parse_value_for(Lex* self, Local* local, Json* json, Buf* data, Column* column)
{
	// null
	if (column->type != TYPE_AGG && lex_if(self, KNULL))
	{
		encode_null(data);
		return;
	}
	auto ast = lex_next(self);
	switch (column->type) {
	case TYPE_INT:
	{
		auto minus = ast->id == '-';
		if (minus)
			ast = lex_next(self);
		if (likely(ast->id == KINT)) {
			if (minus)
				ast->integer = -ast->integer;
			encode_integer(data, ast->integer);
			return;
		}
		if (ast->id == KREAL) {
			if (minus)
				ast->real = -ast->real;
			encode_integer(data, (int64_t)ast->real);
			return;
		}
		break;
	}
	case TYPE_BOOL:
	{
		if (ast->id == KTRUE) {
			encode_bool(data, true);
			return;
		}
		if (ast->id == KFALSE) {
			encode_bool(data, false);
			return;
		}
		break;
	}
	case TYPE_REAL:
	{
		auto minus = ast->id == '-';
		if (minus)
			ast = lex_next(self);
		if (likely(ast->id == KREAL)) {
			if (minus)
				ast->real = -ast->real;
			encode_real(data, ast->real);
			return;
		}
		if (ast->id == KINT) {
			if (minus)
				ast->integer = -ast->integer;
			encode_real(data, ast->integer);
			return;
		}
		break;
	}
	case TYPE_STRING:
	{
		if (likely(ast->id == KSTRING)) {
			encode_string(data, &ast->string);
			return;
		}
		break;
	}
	case TYPE_TIMESTAMP:
	{
		// current_timestamp
		if (ast->id == KCURRENT_TIMESTAMP) {
			encode_timestamp(data, local->time_us);
			return;
		}

		// [TIMESTAMP] string
		if (ast->id == KTIMESTAMP)
			ast = lex_next(self);

		if (likely(ast->id == KSTRING)) {
			Timestamp ts;
			timestamp_init(&ts);
			timestamp_read(&ts, &ast->string);
			encode_timestamp(data, timestamp_of(&ts, local->timezone));
			return;
		}
		if (ast->id == KINT) {
			encode_timestamp(data, ast->integer);
			return;
		}
		break;
	}
	case TYPE_INTERVAL:
	{
		// [INTERVAL] string
		if (ast->id == KINTERVAL)
			ast = lex_next(self);

		if (likely(ast->id == KSTRING)) {
			Interval iv;;
			interval_init(&iv);
			interval_read(&iv, &ast->string);
			encode_interval(data, &iv);
			return;
		}
		break;
	}
	case TYPE_VECTOR:
	{
		// [
		if (ast->id != '[')
			break;
		auto offset = buf_size(data);
		buf_reserve(data, data_size_vector(0));
		data_write_vector_size(&data->position, 0);
		if (lex_if(self, ']'))
			return;
		int count = 0;
		for (;;)
		{
			// -
			ast = lex_next(self);
			auto minus = ast->id == '-';
			if (minus)
				ast = lex_next(self);

			// real or int
			float value;
			if (ast->id == KREAL) {
				value = ast->real;
			} else
			if (ast->id == KINT) {
				value = ast->integer;
			} else {
				error("column <%.*s> invalid vector value", str_size(&column->name),
				      str_of(&column->name));
			}
			if (minus)
				value = -value;
			buf_write(data, &value, sizeof(float));
			count++;

			// ,
			ast = lex_next(self);
			if (ast->id == ',')
				continue;
			if (ast->id == ']')
				break;
			error("column <%.*s> vector array syntax error", str_size(&column->name),
			      str_of(&column->name));
		}
		uint8_t* pos = data->start + offset;
		data_write_vector_size(&pos, count);
		return;
	}
	case TYPE_AGG:
	{
		auto minus = ast->id == '-';
		if (minus)
			ast = lex_next(self);

		Agg agg;
		agg_init(&agg);
		int      value_type;
		AggValue value;
		memset(&value, 0, sizeof(value));
		if (ast->id == KNULL) {
			value_type = AGG_NULL;
		} else
		if (ast->id == KINT) {
			value_type = AGG_INT;
			value.integer = ast->integer;
			if (minus)
				value.integer = -value.integer;
		} else
		if (ast->id == KREAL) {
			value_type = AGG_REAL;
			value.real = ast->real;
			if (minus)
				value.real = -value.real;
		} else {
			// error
			break;
		}
		agg_step(&agg, column->constraint.aggregate, value_type, &value);
		encode_agg(data, &agg);
		return;
	}
	case TYPE_OBJ:
	case TYPE_ARRAY:
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
		json_parse(json, &in, data);
		self->pos = json->pos;
		return;
	}
	}

	error("column <%.*s> value expected to be '%s'", str_size(&column->name),
	      str_of(&column->name), type_of(column->type));
}

hot void
parse_value(Stmt* self, Column* column)
{
	parse_value_for(self->lex, self->local, self->json,
	                &self->data->data, column);
}
