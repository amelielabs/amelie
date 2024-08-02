
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_func.h>

hot static void
fn_length(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	if (arg->type == VALUE_SET)
		value_set_int(self->result, ((Set*)arg->obj)->list_count);
	else
		value_length(self->result, arg);
}

hot static void
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

	auto buf = buf_begin();
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
	buf_end(buf);

	Str string;
	uint8_t* pos_str = buf->start;
	data_read_string(&pos_str, &string);
	value_set_string(self->result, &string, buf);
}

hot static void
fn_lower(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	call_validate_arg(self, 0, VALUE_STRING);

	auto src = str_of(&arg->string);
	int  src_size = str_size(&arg->string);

	auto buf = buf_begin();
	auto pos = buf_reserve(buf, data_size_string(src_size));
	data_write_raw(pos, NULL, src_size);
	auto dst = *pos;
	for (int i = 0; i < src_size; i++)
		dst[i] = tolower(src[i]);
	*pos += src_size;
	buf_end(buf);

	Str string;
	uint8_t* pos_str = buf->start;
	data_read_string(&pos_str, &string);
	value_set_string(self->result, &string, buf);
}

hot static void
fn_upper(Call* self)
{
	auto arg = self->argv[0];
	call_validate(self);
	call_validate_arg(self, 0, VALUE_STRING);

	auto src = str_of(&arg->string);
	int  src_size = str_size(&arg->string);

	auto buf = buf_begin();
	auto pos = buf_reserve(buf, data_size_string(src_size));
	data_write_raw(pos, NULL, src_size);
	auto dst = *pos;
	for (int i = 0; i < src_size; i++)
		dst[i] = toupper(src[i]);
	*pos += src_size;
	buf_end(buf);

	Str string;
	uint8_t* pos_str = buf->start;
	data_read_string(&pos_str, &string);
	value_set_string(self->result, &string, buf);
}

FunctionDef fn_string_def[] =
{
	{ "public", "length", fn_length, 1 },
	{ "public", "size",   fn_length, 1 },
	{ "public", "concat", fn_concat, 0 },
	{ "public", "lower",  fn_lower,  1 },
	{ "public", "upper",  fn_upper,  1 },
	{  NULL,     NULL,    NULL,      0 }
};
