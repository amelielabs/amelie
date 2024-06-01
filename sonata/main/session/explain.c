
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_node.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_semantic.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>
#include <sonata_repl.h>
#include <sonata_cluster.h>
#include <sonata_frontend.h>
#include <sonata_session.h>

void
explain_init(Explain* self)
{
	self->active         = false;
	self->time_run_us    = 0;
	self->time_commit_us = 0;
	self->sent_size      = 0;
}

void
explain_reset(Explain* self)
{
	self->active         = false;
	self->time_run_us    = 0;
	self->time_commit_us = 0;
	self->sent_size      = 0;
}

Buf*
explain(Explain*  self,
        Code*     coordinator,
        Code*     node,
        CodeData* data,
        Plan*     plan,
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
		buf_printf(buf, " sent total:   %d\n", self->sent_size);
	}

	auto string = buf_begin();
	str_set(&str, (char*)buf->start, buf_size(buf));
	encode_string(string, &str);
	buf_end(string);
	buf_end(buf);
	buf_free(buf);
	return string;
}
