
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
#include <amelie_func.h>

hot static void
fn_length(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self, 1);
	if (arg->type == VALUE_SET)
		value_set_int(self->result, ((Set*)arg->store)->list_count);
	else
		value_length(self->result, arg);
}

static void
fn_concat(Call* self)
{
	auto argv = self->argv;
	int  size = 0;
	for (int i = 0; i < self->argc; i++)
	{
		if (argv[i]->type == VALUE_NULL)
			continue;
		if (argv[i]->type != VALUE_STRING)
			error("concat(): string argument expected");
		size += str_size(&argv[i]->string);
	}

	auto buf = buf_create();
	auto pos = buf_reserve(buf, data_size_string(size));
	data_write_raw(pos, NULL, size);
	for (int i = 0; i < self->argc; i++)
	{
		if (argv[i]->type == VALUE_NULL)
			continue;
		auto at_size = str_size(&argv[i]->string);
		memcpy(*pos, str_of(&argv[i]->string), at_size);
		*pos += at_size;
	}

	Str string;
	uint8_t* pos_str = buf->start;
	data_read_string(&pos_str, &string);
	value_set_string(self->result, &string, buf);
}

static void
fn_lower(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self, 1);
	call_validate_arg(self, 0, VALUE_STRING);

	auto src = str_of(&arg->string);
	int  src_size = str_size(&arg->string);

	auto buf = buf_create();
	auto pos = buf_reserve(buf, data_size_string(src_size));
	data_write_raw(pos, NULL, src_size);
	auto dst = *pos;
	for (int i = 0; i < src_size; i++)
		dst[i] = tolower(src[i]);
	*pos += src_size;

	Str string;
	uint8_t* pos_str = buf->start;
	data_read_string(&pos_str, &string);
	value_set_string(self->result, &string, buf);
}

static void
fn_upper(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self, 1);
	call_validate_arg(self, 0, VALUE_STRING);

	auto src = str_of(&arg->string);
	int  src_size = str_size(&arg->string);

	auto buf = buf_create();
	auto pos = buf_reserve(buf, data_size_string(src_size));
	data_write_raw(pos, NULL, src_size);
	auto dst = *pos;
	for (int i = 0; i < src_size; i++)
		dst[i] = toupper(src[i]);
	*pos += src_size;

	Str string;
	uint8_t* pos_str = buf->start;
	data_read_string(&pos_str, &string);
	value_set_string(self->result, &string, buf);
}

static void
fn_substr(Call* self)
{
	auto argv = self->argv;
	if (self->argc < 2 || self->argc > 3)
		error("substr(): unexpected number of arguments");

	// (string, pos)
	call_validate_arg(self, 0, VALUE_STRING);
	call_validate_arg(self, 1, VALUE_INT);
	auto src = &argv[0]->string;
	auto pos = argv[1]->integer;

	// position starts from 1
	if (pos == 0)
		error("substr(): position is out of bounds");
	pos--;

	if (pos >= str_size(src))
		error("substr(): position is out of bounds");

	// (string, pos, count)
	int count = str_size(src) - pos;
	if (self->argc == 3)
	{
		call_validate_arg(self, 2, VALUE_INT);
		count = argv[2]->integer;
		if ((pos + count) > str_size(src))
			error("substr(): position is out of bounds");
	}

	Str string;
	str_set(&string, str_of(src) + pos, count);
	auto buf = buf_create();
	encode_string(buf, &string);

	Str result;
	uint8_t* pos_str = buf->start;
	data_read_string(&pos_str, &result);
	value_set_string(self->result, &result, buf);
}

static void
fn_strpos(Call* self)
{
	// (string, substring)
	auto argv = self->argv;
	call_validate(self, 2);
	call_validate_arg(self, 0, VALUE_STRING);
	call_validate_arg(self, 1, VALUE_STRING);

	auto src = &argv[0]->string;
	auto substr = &argv[1]->string;
	if (str_size(substr) == 0)
	{
		value_set_int(self->result, 0);
		return;
	}

	auto pos = src->pos;
	auto end = src->end;
	while (pos < end)
	{
		if ((end - pos) < str_size(substr))
			break;
		if (*pos == *str_of(substr))
		{
			if (str_is(substr, pos, str_size(substr)))
			{
				value_set_int(self->result, (pos - src->pos) + 1);
				return;
			}
		}
		pos++;
	}

	value_set_int(self->result, 0);
}

static void
fn_replace(Call* self)
{
	// (string, from, to)
	auto argv = self->argv;
	call_validate(self, 3);
	call_validate_arg(self, 0, VALUE_STRING);
	call_validate_arg(self, 1, VALUE_STRING);
	call_validate_arg(self, 2, VALUE_STRING);

	auto src  = &argv[0]->string;
	auto from = &argv[1]->string;
	auto to   = &argv[2]->string;
	if (str_size(from) == 0)
	{
		error("replace(): invalid from argument");
		return;
	}

	auto buf = buf_create();
	encode_string32(buf, 0);

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

		buf_write(buf, pos, 1);
		pos++;
	}

	// update string size
	int size = buf_size(buf) - data_size_string32();
	uint8_t* pos_str = buf->start;
	data_write_string32(&pos_str, size);

	Str string;
	pos_str = buf->start;
	data_read_string(&pos_str, &string);
	value_set_string(self->result, &string, buf);
}

static inline void
str_ltrim(Str* self, char* filter, char* filter_end)
{
	auto pos = self->pos;
	auto end = self->end;
	while (pos < end)
	{
		auto match = false;
		for (auto at = filter; at < filter_end; at++)
		{
			if (*pos == *at)
			{
				match = true;
				break;
			}
		}
		if (! match)
			break;
		self->pos++;
		pos++;
	}
}

static inline void
str_rtrim(Str* self, char* filter, char* filter_end)
{
	if (str_size(self) == 0)
		return;
	auto pos = self->end - 1;
	auto start = self->pos;
	while (pos >= start)
	{
		auto match = false;
		for (auto at = filter; at < filter_end; at++)
		{
			if (*pos == *at)
			{
				match = true;
				break;
			}
		}
		if (! match)
			break;
		self->end--;
		pos--;
	}
}

static void
trim(Call* self, bool left, bool right)
{
	auto argv = self->argv;
	if (self->argc < 1 || self->argc > 2)
		error("trim(): unexpected number of arguments");
	call_validate_arg(self, 0, VALUE_STRING);

	// (string [, filter])
	char* filter = " \t\v\n\f";
	char* filter_end;
	if (self->argc == 2)
	{
		call_validate_arg(self, 1, VALUE_STRING);
		filter = str_of(&argv[1]->string);
		filter_end = filter + str_size(&argv[1]->string);
	} else {
		filter_end = filter + strlen(filter);
	}

	auto src = &argv[0]->string;
	Str string;
	str_set_str(&string, src);
	if (left)
		str_ltrim(&string, filter, filter_end);
	if (right)
		str_rtrim(&string, filter, filter_end);

	auto buf = buf_create();
	encode_string(buf, &string);
	Str result;
	uint8_t* pos_str = buf->start;
	data_read_string(&pos_str, &result);
	value_set_string(self->result, &result, buf);
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
	call_validate(self, 2);
	call_validate_arg(self, 0, VALUE_STRING);
	call_validate_arg(self, 1, VALUE_STRING);
	value_like(self->result, argv[0], argv[1]);
}

FunctionDef fn_string_def[] =
{
	{ "public", "length",  fn_length,  false },
	{ "public", "size",    fn_length,  false },
	{ "public", "concat",  fn_concat,  false },
	{ "public", "lower",   fn_lower,   false },
	{ "public", "upper",   fn_upper,   false },
	{ "public", "substr",  fn_substr,  false },
	{ "public", "strpos",  fn_strpos,  false },
	{ "public", "replace", fn_replace, false },
	{ "public", "ltrim",   fn_ltrim,   false },
	{ "public", "rtrim",   fn_rtrim,   false },
	{ "public", "trim",    fn_trim,    false },
	{ "public", "like",    fn_like,    false },
	{  NULL,     NULL,     NULL,       false }
};
