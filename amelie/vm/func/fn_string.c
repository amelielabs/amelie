
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
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

hot static void
fn_length(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	if (unlikely(arg->type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	int64_t rc = 0;
	switch (arg->type) {
	case TYPE_STRING:
		rc = utf8_strlen(&arg->string);
		break;
	case TYPE_JSON:
		if (json_is_array(arg->json))
			rc = json_array_size(arg->json);
		else
		if (json_is_obj(arg->json))
			rc = json_obj_size(arg->json);
		else
		if (json_is_string(arg->json))
		{
			uint8_t* pos = arg->json;
			Str str;
			json_read_string(&pos, &str);
			rc = utf8_strlen(&str);
		} else
			call_error_arg(self, 0, "unsupported json value");
		break;
	case TYPE_VECTOR:
		rc = arg->vector->size;
		break;
	default:
		call_unsupported(self, 0);
		break;
	}
	value_set_int(self->result, rc);
}

hot static void
fn_octet_length(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	if (unlikely(arg->type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	int64_t rc = 0;
	switch (arg->type) {
	case TYPE_STRING:
		rc = str_size(&arg->string);
		break;
	case TYPE_JSON:
		rc = arg->json_size;
		break;
	case TYPE_VECTOR:
		rc = vector_size(arg->vector);
		break;
	default:
		call_unsupported(self, 0);
		break;
	}
	value_set_int(self->result, rc);
}

static void
fn_concat(Call* self)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	auto argv = self->argv;
	for (int i = 0; i < self->argc; i++)
	{
		if (argv[i].type == TYPE_NULL)
			continue;
		if (argv[i].type != TYPE_STRING)
			call_error_arg(self, i, "string expected");
		buf_write_str(buf, &argv[i].string);
	}

	Str string;
	buf_str(buf, &string);
	value_set_string(self->result, &string, buf);
}

static void
fn_lower(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	if (unlikely(arg->type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_expect_arg(self, 0, TYPE_STRING);

	auto src = str_of(&arg->string);
	auto src_size = str_size(&arg->string);
	auto buf = buf_create();
	buf_reserve(buf, src_size);

	auto dst = buf->position;
	for (int i = 0; i < src_size; i++)
		dst[i] = tolower(src[i]);
	buf_advance(buf, src_size);

	Str string;
	buf_str(buf, &string);
	value_set_string(self->result, &string, buf);
}

static void
fn_upper(Call* self)
{
	auto arg = &self->argv[0];
	call_expect(self, 1);
	if (unlikely(arg->type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_expect_arg(self, 0, TYPE_STRING);

	auto src = str_of(&arg->string);
	auto src_size = str_size(&arg->string);
	auto buf = buf_create();
	buf_reserve(buf, src_size);

	auto dst = buf->position;
	for (int i = 0; i < src_size; i++)
		dst[i] = toupper(src[i]);
	buf_advance(buf, src_size);

	Str string;
	buf_str(buf, &string);
	value_set_string(self->result, &string, buf);
}

static void
fn_substr(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2 || self->argc > 3)
		call_error(self, "unexpected number of arguments");

	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}

	// (string, pos)
	call_expect_arg(self, 0, TYPE_STRING);
	call_expect_arg(self, 1, TYPE_INT);
	auto src = &argv[0].string;
	auto pos = argv[1].integer;

	// position starts from 1
	if (pos == 0)
		call_error_arg(self, 1, "position is out of bounds");
	pos--;

	int size = utf8_strlen(src);
	if (pos >= size)
		call_error_arg(self, 1, "position is out of bounds");

	// (string, pos, count)
	int count = size - pos;
	if (self->argc == 3)
	{
		call_expect_arg(self, 2, TYPE_INT);
		count = argv[2].integer;
		if ((pos + count) > size)
			call_error_arg(self, 2, "position is out of bounds");
	}

	Str result;
	result.allocated = false;
	result.pos = utf8_at(src->pos, pos);
	result.end = result.pos;
	while (count-- > 0)
		utf8_forward(&result.end);

	auto buf = buf_create();
	buf_write_str(buf, &result);
	buf_str(buf, &result);
	value_set_string(self->result, &result, buf);
}

static void
fn_strpos(Call* self)
{
	// (string, substring)
	auto argv = self->argv;
	call_expect(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_expect_arg(self, 0, TYPE_STRING);
	call_expect_arg(self, 1, TYPE_STRING);

	auto src = &argv[0].string;
	auto substr = &argv[1].string;
	if (str_size(substr) == 0)
	{
		value_set_int(self->result, 0);
		return;
	}

	auto pos = src->pos;
	auto end = src->end;
	auto n   = 1;
	while (pos < end)
	{
		if ((end - pos) < str_size(substr))
			break;
		if (*pos == *str_of(substr))
		{
			if (str_is(substr, pos, str_size(substr)))
			{
				value_set_int(self->result, n);
				return;
			}
		}
		utf8_forward(&pos);
		n++;
	}

	value_set_int(self->result, 0);
}

static void
fn_replace(Call* self)
{
	// (string, from, to)
	auto argv = self->argv;
	call_expect(self, 3);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_expect_arg(self, 0, TYPE_STRING);
	call_expect_arg(self, 1, TYPE_STRING);
	call_expect_arg(self, 2, TYPE_STRING);

	auto src  = &argv[0].string;
	auto from = &argv[1].string;
	auto to   = &argv[2].string;
	if (str_size(from) == 0)
	{
		call_error_arg(self, 1, "invalid argument");
		return;
	}

	auto buf = buf_create();
	auto pos = src->pos;
	auto end = src->end;
	while (pos < end)
	{
		if ((end - pos) < str_size(from))
		{
			buf_write(buf, pos, end - pos);
			break;
		}

		if (*pos == *str_of(from))
		{
			if (str_is(from, pos, str_size(from)))
			{
				if (str_size(to) > 0)
					buf_write_str(buf, to);
				pos += str_size(from);
				continue;
			}
		}

		auto pos_size = utf8_sizeof(*pos);
		buf_write(buf, pos, pos_size);
		pos += pos_size;
	}

	Str string;
	buf_str(buf, &string);
	value_set_string(self->result, &string, buf);
}

static inline void
str_ltrim(Str* self, char* filter, char* filter_end)
{
	auto pos = self->pos;
	auto end = self->end;
	while (pos < end)
	{
		auto pos_size = utf8_sizeof(*pos);
		auto match = false;
		for (auto at = filter; at < filter_end; )
		{
			auto at_size = utf8_sizeof(*at);
			if (pos_size == at_size && !memcmp(pos, at, at_size))
			{
				match = true;
				break;
			}
			at += at_size;
		}
		if (! match)
			break;
		self->pos += pos_size;
		pos += pos_size;
	}
}

static inline void
str_rtrim(Str* self, char* filter, char* filter_end)
{
	if (str_size(self) == 0)
		return;
	auto pos   = self->end - 1;
	auto start = self->pos;
	while (pos >= start)
	{
		utf8_backward(&pos);
		auto pos_size = utf8_sizeof(*pos);
		auto match = false;
		for (auto at = filter; at < filter_end; )
		{
			auto at_size = utf8_sizeof(*at);
			if (pos_size == at_size && !memcmp(pos, at, at_size))
			{
				match = true;
				break;
			}
			at += at_size;
		}
		if (! match)
			break;
		self->end -= pos_size;
		pos--;
	}
}

static void
trim(Call* self, bool left, bool right)
{
	auto argv = self->argv;
	if (self->argc < 1 || self->argc > 2)
		call_error(self, "unexpected number of arguments");
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_expect_arg(self, 0, TYPE_STRING);

	// (string [, filter])
	char* filter = " \t\v\n\f";
	char* filter_end;
	if (self->argc == 2)
	{
		call_expect_arg(self, 1, TYPE_STRING);
		filter = str_of(&argv[1].string);
		filter_end = filter + str_size(&argv[1].string);
	} else {
		filter_end = filter + strlen(filter);
	}

	auto src = &argv[0].string;
	Str string;
	str_set_str(&string, src);
	if (left)
		str_ltrim(&string, filter, filter_end);
	if (right)
		str_rtrim(&string, filter, filter_end);

	auto buf = buf_create();
	buf_write_str(buf, &string);
	buf_str(buf, &string);
	value_set_string(self->result, &string, buf);
}

static void
fn_ltrim(Call* self)
{
	trim(self, true, false);
}

static void
fn_rtrim(Call* self)
{
	trim(self, false, true);
}

static void
fn_trim(Call* self)
{
	trim(self, true, true);
}

static void
fn_like(Call* self)
{
	auto argv = self->argv;
	call_expect(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_expect_arg(self, 0, TYPE_STRING);
	call_expect_arg(self, 1, TYPE_STRING);
	value_like(self->result, &argv[0], &argv[1]);
}

FunctionDef fn_string_def[] =
{
	{ "public", "size",         TYPE_INT,    fn_length,       FN_NONE },
	{ "public", "length",       TYPE_INT,    fn_length,       FN_NONE },
	{ "public", "octet_length", TYPE_INT,    fn_octet_length, FN_NONE },
	{ "public", "concat",       TYPE_STRING, fn_concat,       FN_NONE },
	{ "public", "lower",        TYPE_STRING, fn_lower,        FN_NONE },
	{ "public", "upper",        TYPE_STRING, fn_upper,        FN_NONE },
	{ "public", "substr",       TYPE_STRING, fn_substr,       FN_NONE },
	{ "public", "strpos",       TYPE_INT,    fn_strpos,       FN_NONE },
	{ "public", "replace",      TYPE_STRING, fn_replace,      FN_NONE },
	{ "public", "ltrim",        TYPE_STRING, fn_ltrim,        FN_NONE },
	{ "public", "rtrim",        TYPE_STRING, fn_rtrim,        FN_NONE },
	{ "public", "trim",         TYPE_STRING, fn_trim,         FN_NONE },
	{ "public", "like",         TYPE_BOOL,   fn_like,         FN_NONE },
	{  NULL,     NULL,          TYPE_NULL,   NULL,            FN_NONE }
};
