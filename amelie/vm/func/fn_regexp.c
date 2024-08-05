
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

#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>

hot static void
fn_regexp_like(Call* self)
{
	assert(self->context);

	pcre2_code* re = *self->context;
	if (self->type == CALL_CLEANUP)
	{
		pcre2_code_free(re);
		*self->context = NULL;
		return;
	}

	// (string, pattern)
	auto argv = self->argv;
	call_validate(self, 2);
	call_validate_arg(self, 0, VALUE_STRING);
	call_validate_arg(self, 1, VALUE_STRING);

	// first call, compile pattern
	auto pattern = &argv[1]->string;
	if (re == NULL)
	{
		int        error_number = 0;
		PCRE2_SIZE error_offset = 0;
		re = pcre2_compile((PCRE2_SPTR)str_of(pattern), str_size(pattern), 0,
		                   &error_number,
		                   &error_offset, NULL);
		if (! re)
		{
			PCRE2_UCHAR msg[256];
			pcre2_get_error_message(error_number, msg, sizeof(msg));
			error("regexp_like(): %s", msg);
		}
		*self->context = re;
	}

	auto match_data = pcre2_match_data_create_from_pattern(re, NULL);
	if (! match_data)
		error("regexp_like(): failed to allocate match data");
	auto string = &argv[0]->string;
	auto rc = pcre2_match(re, (PCRE2_SPTR)str_of(string), str_size(string),
	                      0, 0, match_data, NULL);
	pcre2_match_data_free(match_data);
	value_set_bool(self->result, rc > 0);
}

FunctionDef fn_regexp_def[] =
{
	{ "public", "regexp_like", fn_regexp_like, true  },
	{  NULL,     NULL,         NULL,           false }
};
