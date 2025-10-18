
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

Session*
session_create(Frontend* frontend)
{
	auto self = (Session*)am_malloc(sizeof(Session));
	self->ql        = NULL;
	self->program   = program_allocate();
	self->lock_type = LOCK_NONE;
	self->lock      = NULL;
	self->lock_ref  = NULL;
	self->frontend  = frontend;
	local_init(&self->local);
	explain_init(&self->explain);
	vm_init(&self->vm, NULL, &self->dtr);
	dtr_init(&self->dtr, &self->local, share()->core_mgr);

	// register query languages based on the supported content types
	auto ql_mgr = &self->ql_mgr;
	ql_mgr_init(ql_mgr);

	// text/plain
	Str type;
	str_set(&type, "text/plain", 10);
	ql_mgr_add(ql_mgr, &self->local, &ql_sql_if, &type);

	// application/sql
	str_set(&type, "application/sql", 15);
	ql_mgr_add(ql_mgr, &self->local, &ql_sql_if, &type);

	// text/csv
	str_set(&type, "text/csv", 8);
	ql_mgr_add(ql_mgr, &self->local, &ql_csv_if, &type);

	// application/json
	str_set(&type, "application/json", 16);
	ql_mgr_add(ql_mgr, &self->local, &ql_json_if, &type);

	// application/jsonl
	str_set(&type, "application/jsonl", 17);
	ql_mgr_add(ql_mgr, &self->local, &ql_jsonl_if, &type);
	return self;
}

static inline void
session_reset_ql(Session* self)
{
	if (self->ql)
	{
		ql_reset(self->ql);
		self->ql = NULL;
	}
	palloc_reset();
}

static inline void
session_reset(Session* self)
{
	assert(! self->ql);
	vm_reset(&self->vm);
	program_reset(self->program);
	dtr_reset(&self->dtr);
	explain_reset(&self->explain);
}

void
session_free(Session *self)
{
	assert(self->lock_type == LOCK_NONE);
	session_reset(self);
	vm_free(&self->vm);
	ql_mgr_free(&self->ql_mgr);
	program_free(self->program);
	dtr_free(&self->dtr);
	local_free(&self->local);
	am_free(self);
}

void
session_lock(Session* self, int type)
{
	if (self->lock_type == type)
		return;

	// downgrade or upgrade lock request
	if (self->lock_type != LOCK_NONE)
		session_unlock(self);

	switch (type) {
	case LOCK_SHARED:
		// take shared frontend lock
		self->lock = lock_mgr_lock(&self->frontend->lock_mgr, type);
		break;
	case LOCK_EXCLUSIVE:
		control_lock();
		break;
	case LOCK_NONE:
		break;
	}

	self->lock_type = type;
}

void
session_unlock(Session* self)
{
	switch (self->lock_type) {
	case LOCK_SHARED:
		lock_mgr_unlock(&self->frontend->lock_mgr, self->lock_type, self->lock);
		break;
	case LOCK_EXCLUSIVE:
		control_unlock();
		break;
	case LOCK_NONE:
		break;
	}
	self->lock_type = LOCK_NONE;
}

hot static inline void
session_execute_distributed(Session* self, Content* output)
{
	auto program = self->program;
	auto explain = &self->explain;
	auto dtr     = &self->dtr;

	reg_prepare(&self->vm.r, program->code.regs);

	// prepare distributed transaction
	dtr_create(dtr, program);

	// [PROFILE]
	if (program->profile)
		explain_start(&explain->time_run_us);

	// execute coordinator
	Return ret;
	return_init(&ret);
	auto on_error = error_catch
	(
		vm_run(&self->vm, &self->local,
		       NULL,
		       program,
		       &program->code,
		       &program->code_data,
		       NULL,
		       NULL,
		       &ret,
		       0);
	);

	Buf* error = NULL;
	if (on_error)
		error = error_create(&am_self()->error);

	if (program->profile)
	{
		explain_end(&explain->time_run_us);
		explain_start(&explain->time_commit_us);
	}

	// coordinate wal write and group commit, handle group abort
	commit(share()->commit, dtr, error);

	// explain profile
	if (program->profile)
	{
		explain_end(&explain->time_commit_us);
		explain_run(explain, program, &self->local,
		            output, true);
	} else
	{
		if (ret.value)
			content_write(output, ret.fmt, ret.columns, ret.value);
	}
}

hot static inline void
session_execute_utility(Session* self, Content* output)
{
	auto program = self->program;
	reg_prepare(&self->vm.r, program->code.regs);

	// [PROFILE]
	auto explain = &self->explain;
	if (program->profile)
		explain_start(&explain->time_run_us);

	// execute utility/ddl transaction
	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);

	Return ret;
	return_init(&ret);
	auto on_error = error_catch
	(
		// begin
		tr_begin(&tr);

		vm_run(&self->vm, &self->local,
		       &tr,
		       program,
		       &program->code,
		       &program->code_data,
		       NULL,
		       NULL,
		       &ret,
		       0);
	);

	if (unlikely(on_error))
	{
		tr_abort(&tr);
		rethrow();
	}

	if (program->profile)
	{
		explain_end(&explain->time_run_us);
		explain_start(&explain->time_commit_us);
	} else
	{
		if (ret.value)
		{
			Str column;
			str_set(&column, "result", 6);
			content_write_json(output, ret.fmt, &column, ret.value);
		}
	}

	// wal write
	if (! tr_read_only(&tr))
	{
		// respect system read-only state
		if (opt_int_of(&state()->read_only))
			error("system is in read-only mode");

		Write write;
		write_init(&write);
		defer(write_free, &write);
		write_begin(&write);
		write_add(&write, &tr.log.write_log);

		WriteList write_list;
		write_list_init(&write_list);
		write_list_add(&write_list, &write);
		wal_mgr_write(&share()->db->wal_mgr, &write_list);
	}

	// commit
	tr_commit(&tr);

	// explain profile
	if (program->profile)
	{
		explain_end(&explain->time_commit_us);
		explain_run(explain, program, &self->local,
		            output, true);
	}
}

hot static void
session_execute_main(Session* self,
                     Str*     url,
                     Str*     text,
                     Str*     content_type,
                     Content* output)
{
	// set transaction time
	local_update_time(&self->local);

	// POST /
	// POST /v1/execute
	// POST /v1/db/schema/relation

	// find query parser based on the content type
	self->ql = ql_mgr_find(&self->ql_mgr, content_type);
	if (unlikely(! self->ql))
		error("unsupported API operation");

	// parse and compile request program
	auto program = self->program;
	if (self->ql->iface == &ql_sql_if)
	{
		if (unlikely(!str_is(url, "/", 1) &&
		             !str_is(url, "/v1/execute", 11)))
			error("unsupported API operation");
	}
	ql_parse(self->ql, program, text, url);

	if (program_empty(program))
		return;

	// [EXPLAIN]
	if (program->explain)
	{
		explain_run(&self->explain, program,
		            &self->local,
		            output, false);
		return;
	}

	// take the required program lock
	session_lock(self, program->lock);

	// execute utility, DDL, DML or Query
	if (program->utility)
		session_execute_utility(self, output);
	else
		session_execute_distributed(self, output);
}

hot bool
session_execute(Session* self,
                Str*     url,
                Str*     text,
                Str*     content_type,
                Content* output)
{
	cancel_pause();

	// execute request based on the content-type
	auto on_error = error_catch
	(
		// prepare session state for execution
		session_reset(self);

		content_set_local(output, &self->local);

		// take shared session lock (for catalog access)
		session_lock(self, LOCK_SHARED);

		// parse and execute request
		session_execute_main(self, url, text, content_type, output);

		// done
		session_unlock(self);
	);

	session_reset_ql(self);

	if (on_error)
	{
		content_reset(output);
		content_write_json_error(output, &am_self()->error);
		session_unlock(self);
	}

	// cancellation point
	cancel_resume();
	return on_error;
}

void
session_execute_replay(Session* self, Primary* primary, Buf* data)
{
	// take shared lock
	session_lock(self, LOCK_SHARED);
	defer(session_unlock, self);

	// validate request fields and check current replication state

	// first join request has no data
	if (! primary_next(primary))
		return;

	// switch distributed transaction to replication state to write wal
	// while in read-only mode
	auto dtr = &self->dtr;
	auto program = self->program;
	program->repl = true;

	// replay writes
	auto pos = data->start;
	auto end = data->position;
	while (pos < end)
	{
		auto record = (Record*)pos;
		if (opt_int_of(&config()->wal_crc))
			if (unlikely(! record_validate(record)))
				error("repl: record crc mismatch, system LSN is: %" PRIu64,
				      state_lsn());

		if (likely(record_cmd_is_dml(record_cmd(record))))
		{
			// execute DML
			dtr_reset(dtr);
			dtr_create(dtr, program);
			replay(dtr, record);
		} else
		{
			// upgrade to exclusive lock
			session_lock(self, LOCK_EXCLUSIVE);

			// execute DDL
			recover_next(primary->recover, record);
		}
		pos += record->size;
	}
}
