
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
#include <amelie_func.h>

#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>

static inline pcre2_code*
fn_regexp_init(Call* self, Str* pattern)
{
	pcre2_code* re = *self->context;
	if (re)
		return re;
	int        error_number = 0;
	PCRE2_SIZE error_offset = 0;
	re = pcre2_compile((PCRE2_SPTR)str_of(pattern), str_size(pattern), 0,
	                   &error_number,
	                   &error_offset, NULL);
	if (! re)
	{
		PCRE2_UCHAR msg[256];
		pcre2_get_error_message(error_number, msg, sizeof(msg));
		error("regexp: %s", msg);
	}
	*self->context = re;
	return re;
}

static inline bool
fn_regexp_cleanup(Call* self)
{
	if (self->type != CALL_CLEANUP)
		return false;
	pcre2_code* re = *self->context;
	pcre2_code_free(re);
	*self->context = NULL;
	return true;
}

hot static void
fn_regexp_like(Call* self)
{
	if (fn_regexp_cleanup(self))
		return;

	// (string, pattern)
	auto argv = self->argv;
	call_validate(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_validate_arg(self, 0, TYPE_STRING);
	call_validate_arg(self, 1, TYPE_STRING);

	// first call, compile pattern
	auto pattern = &argv[1].string;
	pcre2_code* re = fn_regexp_init(self, pattern);

	auto match_data = pcre2_match_data_create_from_pattern(re, NULL);
	if (! match_data)
		error("regexp_like(): failed to allocate match data");
	auto string = &argv[0].string;
	auto rc = pcre2_match(re, (PCRE2_SPTR)str_of(string), str_size(string),
	                      0, 0, match_data, NULL);
	pcre2_match_data_free(match_data);
	value_set_bool(self->result, rc > 0);
}

hot static void
fn_regexp_substr(Call* self)
{
	if (fn_regexp_cleanup(self))
		return;

	// (string, pattern)
	auto argv = self->argv;
	call_validate(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_validate_arg(self, 0, TYPE_STRING);
	call_validate_arg(self, 1, TYPE_STRING);

	// first call, compile pattern
	auto pattern = &argv[1].string;
	pcre2_code* re = fn_regexp_init(self, pattern);

	auto match_data = pcre2_match_data_create_from_pattern(re, NULL);
	if (! match_data)
		error("regexp_substr(): failed to allocate match data");
	guard(pcre2_match_data_free, match_data);

	auto string = &argv[0].string;
	auto rc = pcre2_match(re, (PCRE2_SPTR)str_of(string), str_size(string),
	                      0, 0, match_data, NULL);
	if (rc <= 0)
	{
		value_set_null(self->result);
		return;
	}
	auto ovector = pcre2_get_ovector_pointer(match_data);

	Str first;
	str_set(&first, str_of(string) + ovector[0], ovector[1] - ovector[0]);

	auto buf = buf_create();
	encode_string(buf, &first);
	Str result;
	uint8_t* pos_str = buf->start;
	json_read_string(&pos_str, &result);
	value_set_string(self->result, &result, buf);
}

hot static void
fn_regexp_match(Call* self)
{
	if (fn_regexp_cleanup(self))
		return;

	// (string, pattern)
	auto argv = self->argv;
	call_validate(self, 2);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_validate_arg(self, 0, TYPE_STRING);
	call_validate_arg(self, 1, TYPE_STRING);

	// first call, compile pattern
	auto pattern = &argv[1].string;
	pcre2_code* re = fn_regexp_init(self, pattern);

	auto match_data = pcre2_match_data_create_from_pattern(re, NULL);
	if (! match_data)
		error("regexp_match(): failed to allocate match data");
	guard(pcre2_match_data_free, match_data);

	auto string = &argv[0].string;
	auto rc = pcre2_match(re, (PCRE2_SPTR)str_of(string), str_size(string),
	                      0, 0, match_data, NULL);
	if (rc <= 0)
	{
		value_set_null(self->result);
		return;
	}
	auto ovector = pcre2_get_ovector_pointer(match_data);

	auto buf = buf_create();
	encode_array(buf);
	for (int i = 0; i < rc; i++)
	{
		Str ref;
		str_set(&ref, str_of(string) + ovector[2 * i],
		        ovector[2 * i + 1] - ovector[2*i]);
		encode_string(buf, &ref);
	}
	encode_array_end(buf);
	value_set_json_buf(self->result, buf);
}

hot static void
fn_regexp_replace(Call* self)
{
	if (fn_regexp_cleanup(self))
		return;

	// (string, pattern, string)
	auto argv = self->argv;
	call_validate(self, 3);
	if (unlikely(argv[0].type == TYPE_NULL))
	{
		value_set_null(self->result);
		return;
	}
	call_validate_arg(self, 0, TYPE_STRING);
	call_validate_arg(self, 1, TYPE_STRING);
	call_validate_arg(self, 2, TYPE_STRING);

	// first call, compile pattern
	auto pattern = &argv[1].string;
	pcre2_code* re = fn_regexp_init(self, pattern);

	auto match_data = pcre2_match_data_create_from_pattern(re, NULL);
	if (! match_data)
		error("regexp_match(): failed to allocate match data");
	guard(pcre2_match_data_free, match_data);

	auto string  = &argv[0].string;
	auto replace = &argv[2].string;

	auto buf = buf_create();
	encode_string32(buf, 0);
	buf_reserve(buf, str_size(string) + str_size(string) / 3);
	for (;;)
	{
		const uint32_t options = PCRE2_SUBSTITUTE_OVERFLOW_LENGTH |
		                         PCRE2_SUBSTITUTE_EXTENDED |
		                         PCRE2_SUBSTITUTE_GLOBAL;
		PCRE2_SIZE outlen = buf_size_unused(buf);
		auto rc = pcre2_substitute(re, (PCRE2_SPTR)str_of(string), str_size(string),
		                           0, options,
		                           match_data,
		                           NULL,
		                           (PCRE2_SPTR)str_of(replace), str_size(replace),
		                           (PCRE2_UCHAR*)buf->position, &outlen);
		if (rc == PCRE2_ERROR_NOMEMORY)
		{
			buf_reserve(buf, outlen);
			continue;
		}

		buf_advance(buf, outlen);
		break;
	}

	// update string size
	int size = buf_size(buf) - json_size_string32();
	uint8_t* pos_str = buf->start;
	json_write_string32(&pos_str, size);

	Str result;
	pos_str = buf->start;
	json_read_string(&pos_str, &result);
	value_set_string(self->result, &result, buf);
}

FunctionDef fn_regexp_def[] =
{
	{ "public", "regexp_like",    TYPE_BOOL,   fn_regexp_like,     FN_CONTEXT },
	{ "public", "regexp_substr",  TYPE_STRING, fn_regexp_substr,   FN_CONTEXT },
	{ "public", "regexp_match",   TYPE_JSON,   fn_regexp_match,    FN_CONTEXT },
	{ "public", "regexp_replace", TYPE_STRING, fn_regexp_replace,  FN_CONTEXT },
	{  NULL,     NULL,            TYPE_NULL,   NULL,               FN_NONE    }
};
