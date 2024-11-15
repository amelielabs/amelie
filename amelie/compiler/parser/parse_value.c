
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

hot static inline bool
parse_int(Lex* self, Ast* ast, int64_t* value)
{
	auto minus = ast->id == '-';
	if (minus)
		ast = lex_next(self);
	if (likely(ast->id == KINT)) {
		if (minus)
			ast->integer = -ast->integer;
		*value = ast->integer;
	} else
	if (ast->id == KREAL) {
		if (minus)
			ast->real = -ast->real;
		*value = ast->real;
	} else {
		return false;
	}
	return true;
}

hot static inline bool
parse_double(Lex* self, Ast* ast, double* value)
{
	auto minus = ast->id == '-';
	if (minus)
		ast = lex_next(self);
	if (likely(ast->id == KINT)) {
		if (minus)
			ast->integer = -ast->integer;
		*value = ast->integer;
	} else
	if (ast->id == KREAL) {
		if (minus)
			ast->real = -ast->real;
		*value = ast->real;
	} else {
		return false;
	}
	return true;
}

hot static inline bool
parse_vector(Lex* self, Ast* ast, Buf* data, Column* column)
{
	// [
	if (ast->id != '[')
		return false;
	auto offset = buf_size(data);
	buf_write_i32(data, 0);

	// []
	if (lex_if(self, ']'))
		return true;

	// [float [, ...]]
	int count = 0;
	for (;;)
	{
		ast = lex_next(self);
		double value;
		if (! parse_double(self, ast, &value))
			error("column <%.*s> invalid vector value", str_size(&column->name),
			      str_of(&column->name));
		buf_write_float(data, value);
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

	*(uint32_t*)(data->start + offset) = count;
	return true;
}

hot void
parse_value(Lex* self, Local* local, Json* json, Buf* data, Column* column)
{
	// null
	if (lex_if(self, KNULL))
		return;

	int64_t integer;
	double  dbl;
	auto ast = lex_next(self);
	switch (column->type) {
	case TYPE_BOOL:
		if (ast->id == KTRUE)
			buf_write_i8(data, true);
		else
		if (ast->id == KFALSE)
			buf_write_i8(data, false);
		else
			break;
		return;
	case TYPE_INT8:
		if (! parse_int(self, ast, &integer))
			break;
		buf_write_i8(data, integer);
		return;
	case TYPE_INT16:
		if (! parse_int(self, ast, &integer))
			break;
		buf_write_i16(data, integer);
		return;
	case TYPE_INT32:
		if (! parse_int(self, ast, &integer))
			break;
		buf_write_i32(data, integer);
		return;
	case TYPE_INT64:
		if (! parse_int(self, ast, &integer))
			break;
		buf_write_i64(data, integer);
		return;
	case TYPE_FLOAT:
		if (! parse_double(self, ast, &dbl))
			break;
		buf_write_float(data, dbl);
		return;
	case TYPE_DOUBLE:
		if (! parse_double(self, ast, &dbl))
			break;
		buf_write_double(data, dbl);
		return;
	case TYPE_TIMESTAMP:
	{
		// current_timestamp
		if (ast->id == KCURRENT_TIMESTAMP) {
			buf_write_i64(data, local->time_us);
			return;
		}

		// [TIMESTAMP] string
		if (ast->id == KTIMESTAMP)
			ast = lex_next(self);

		if (likely(ast->id == KSTRING)) {
			Timestamp ts;
			timestamp_init(&ts);
			timestamp_read(&ts, &ast->string);
			buf_write_i64(data, timestamp_of(&ts, local->timezone));
			return;
		}

		// unixtime
		if (ast->id == KINT) {
			buf_write_i64(data, ast->integer);
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
			Interval iv;
			interval_init(&iv);
			interval_read(&iv, &ast->string);
			buf_write(data, &iv, sizeof(iv));
			return;
		}
		break;
	}
	case TYPE_TEXT:
	{
		if (likely(ast->id == KSTRING)) {
			encode_string(data, &ast->string);
			return;
		}
		break;
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
		json_parse(json, &in, data);
		self->pos = json->pos;
		return;
	}
	case TYPE_VECTOR:
	{
		if (! parse_vector(self, ast, data, column))
			break;
		return;
	}
	}

	error("column <%.*s> value expected to be '%s'", str_size(&column->name),
	      str_of(&column->name), type_of(column->type));
}
