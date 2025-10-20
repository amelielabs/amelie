
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_compiler.h>
#include <amelie_frontend.h>
#include <amelie_backend.h>
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

static void
explain_call(Udf* udf, Buf* buf)
{
	auto program = (Program*)udf->data;
	encode_obj(buf);

	// target
	encode_raw(buf, "function", 8);
	encode_target(buf, &udf->config->schema, &udf->config->name);

	// bytecode
	encode_raw(buf, "bytecode", 8);
	encode_obj(buf);

	// frontend section
	encode_raw(buf, "frontend", 8);
	op_dump(program, &program->code, buf);

	// backend section
	if (code_count(&program->code_backend) > 0)
	{
		encode_raw(buf, "backend", 7);
		op_dump(program, &program->code_backend, buf);
	}
	encode_obj_end(buf);

	// access
	encode_raw(buf, "access", 6);
	access_encode(&program->access, buf);

	encode_obj_end(buf);
}

void
explain_run(Explain* self,
            Program* program,
            Local*   local,
            Content* content,
            bool     profile)
{
	auto buf = buf_create();
	defer_buf(buf);

	encode_obj(buf);

	// bytecode
	encode_raw(buf, "bytecode", 8);
	encode_obj(buf);

	// frontend section
	encode_raw(buf, "frontend", 8);
	op_dump(program, &program->code, buf);

	// backend section
	if (code_count(&program->code_backend) > 0)
	{
		encode_raw(buf, "backend", 7);
		op_dump(program, &program->code_backend, buf);
	}
	encode_obj_end(buf);

	// access
	auto access = &program->access;
	encode_raw(buf, "access", 6);
	auto has_call = access_encode(access, buf);

	// calls
	if (has_call)
	{
		encode_raw(buf, "calls", 5);
		encode_array(buf);
		for (auto i = 0; i < access->list_count; i++)
		{
			auto record = access_at(access, i);
			if (record->type == ACCESS_CALL)
				explain_call(udf_of(record->rel), buf);
		}
		encode_array_end(buf);
	}

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
		encode_integer(buf, buf_size(content->content));

		encode_obj_end(buf);
	}

	encode_obj_end(buf);

	// set new content
	content_reset(content);
	Str name;
	str_set(&name, "explain", 7);
	content_write_json_buf(content, local->format, &name, buf);
}
