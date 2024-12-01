
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
        Dtr*      dtr,
        Buf*      body,
        bool      profile)
{
	auto buf = buf_create();
	encode_obj(buf);
	unused(dtr);

	// bytecode
	encode_raw(buf, "bytecode", 8);
	encode_obj(buf);

	// coordinator code
	encode_raw(buf, "coordinator", 11);
	op_dump(coordinator, data, buf);

	// node code
	if (code_count(node) > 0)
	{
		encode_raw(buf, "node", 4);
		op_dump(node, data, buf);
	}
	encode_obj_end(buf);

	// profiler
	if (profile)
	{
		encode_raw(buf, "profiler", 8);
		encode_obj(buf);

		uint64_t time_us = self->time_run_us + self->time_commit_us;

		// time_run
		encode_raw(buf, "time_run_ms", 11);
		encode_real(buf, self->time_run_us / 1000.0);

		// time_commit
		encode_raw(buf, "time_commit_ms", 14);
		encode_real(buf, self->time_commit_us / 1000.0);

		// time
		encode_raw(buf, "time_ms", 7);
		encode_real(buf, time_us / 1000.0);

		// sent_total
		encode_raw(buf, "sent_total", 10);
		encode_integer(buf, buf_size(body));

		encode_obj_end(buf);
	}

	encode_obj_end(buf);
	return buf;
}
