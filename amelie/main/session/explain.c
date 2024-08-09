
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
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_cluster.h>
#include <amelie_frontend.h>
#include <amelie_session.h>

void
explain_init(Explain* self)
{
	self->time_run_us    = 0;
	self->time_commit_us = 0;
}

void
explain_reset(Explain* self)
{
	self->time_run_us    = 0;
	self->time_commit_us = 0;
}

Buf*
explain(Explain*  self,
        Code*     coordinator,
        Code*     node,
        CodeData* data,
        Plan*     plan,
        Buf*      body,
        bool      profile)
{
	auto buf = buf_begin();

	// coordinator code
	Str str;
	str_init(&str);
	str_set_cstr(&str, "coordinator");
	op_dump(coordinator, data, buf, &str);

	// node code
	if (code_count(node) > 0)
	{
		str_set_cstr(&str, "node");
		op_dump(node, data, buf, &str);
	}

	// profiler stats
	if (profile)
	{
		unused(plan);

		uint64_t time_us = self->time_run_us + self->time_commit_us;
		buf_printf(buf, "%s", "\n");
		buf_printf(buf, "%s", "profiler\n");
		buf_printf(buf, "%s", "--------\n");
		buf_printf(buf, " time run:     %.3f ms\n", self->time_run_us / 1000.0);
		buf_printf(buf, " time commit:  %.3f ms\n", self->time_commit_us / 1000.0);
		buf_printf(buf, " time:         %.3f ms\n", time_us / 1000.0);
		buf_printf(buf, " sent total:   %d\n", buf_size(body));
	}

	auto string = buf_begin();
	str_set(&str, (char*)buf->start, buf_size(buf));
	encode_string(string, &str);
	buf_end(string);
	buf_end(buf);
	buf_free(buf);
	return string;
}
