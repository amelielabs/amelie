
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_engine>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_compiler>
#include <amelie_frontend.h>
#include <amelie_backend.h>
#include <amelie_session.h>

Session*
session_create(Frontend* frontend)
{
	auto self = (Session*)am_malloc(sizeof(Session));
	self->program   = program_allocate();
	self->lock_type = LOCK_NONE;
	self->lock      = NULL;
	self->lock_ref  = NULL;
	self->frontend  = frontend;
	local_init(&self->local);
	set_cache_init(&self->set_cache);
	compiler_init(&self->compiler, &self->local, &self->set_cache);
	vm_init(&self->vm, NULL, &self->dtr);
	profile_init(&self->profile);
	dtr_init(&self->dtr, &self->local);
	return self;
}

static inline void
session_reset_query(Session* self)
{
	compiler_reset(&self->compiler);
	palloc_reset();
}

static inline void
session_reset(Session* self)
{
	vm_reset(&self->vm);
	program_reset(self->program, &self->set_cache);
	dtr_reset(&self->dtr);
	profile_reset(&self->profile);
	local_reset(&self->local);
}

void
session_free(Session *self)
{
	assert(self->lock_type == LOCK_NONE);
	session_reset_query(self);
	session_reset(self);
	compiler_free(&self->compiler);
	vm_free(&self->vm);
	program_free(self->program);
	set_cache_free(&self->set_cache);
	dtr_free(&self->dtr);
	local_free(&self->local);
	am_free(self);
}

static void
session_set(Session* self, Endpoint* endpoint, Output* output)
{
	// set timezone / format
	auto local = &self->local;
	local->timezone = output->timezone;
	local->format   = output->format;
	local->db       = endpoint->db.string;

	// update transaction time
	local_update_time(local);

	// validate database
	if (str_empty(&local->db))
		error("database is not defined");
	db_mgr_find(&share()->storage->catalog.db_mgr, &local->db, true);
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
session_execute_distributed(Session* self, Output* output)
{
	auto compiler = &self->compiler;
	auto program  = compiler->program;
	auto profile  = &self->profile;
	auto dtr      = &self->dtr;

	reg_prepare(&self->vm.r, program->code.regs);

	// prepare distributed transaction
	dtr_create(dtr, program);

	// [PROFILE]
	if (compiler->program_profile)
		profile_start(&profile->time_run_us);

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
		       compiler->program_args,
		       &ret,
		       true,
		       0);
	);

	Buf* error = NULL;
	if (on_error)
		error = error_create(&am_self()->error);

	if (compiler->program_profile)
	{
		profile_end(&profile->time_run_us);
		profile_start(&profile->time_commit_us);
	}

	// coordinate wal write and group commit, handle group abort
	commit(share()->commit, dtr, error);

	// write result
	auto returning = compiler->program_returning;
	if (returning && ret.value)
		output_write(output, returning, ret.value);

	// explain profile
	if (compiler->program_profile)
	{
		profile_end(&profile->time_commit_us);
		profile_create(profile, program, output);
	}
}

hot static inline void
session_execute_utility(Session* self, Output* output)
{
	auto compiler = &self->compiler;
	auto program  = compiler->program;
	reg_prepare(&self->vm.r, program->code.regs);

	// [PROFILE]
	auto profile = &self->profile;
	if (compiler->program_profile)
		profile_start(&profile->time_run_us);

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
		       NULL,
		       &ret,
		       true,
		       0);
	);

	if (unlikely(on_error))
	{
		tr_abort(&tr);
		rethrow();
	}

	if (compiler->program_profile)
	{
		profile_end(&profile->time_run_us);
		profile_start(&profile->time_commit_us);
	} else
	{
		// write result
		if (ret.value && compiler->program_returning)
		{
			Str column;
			str_set(&column, "result", 6);
			output_write_json(output, &column, ret.value->json, true);
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
		wal_mgr_write(&share()->storage->wal_mgr, &write_list);
	}

	// commit
	tr_commit(&tr);

	// profile
	if (compiler->program_profile)
	{
		profile_end(&profile->time_commit_us);
		profile_create(profile, program, output);
	}
}

hot static void
session_endpoint(Session*  self,
                 Endpoint* endpoint,
                 Str*      content,
                 Output*   output)
{
	// POST /v1/db/<db>
	// POST /v1/db/<db>/<relation>
	auto db = &endpoint->db.string;
	if (unlikely(str_empty(db)))
		error("database is not specified");

	// parse request
	auto compiler = &self->compiler;
	compiler_set(compiler, self->program);

	if (str_empty(&endpoint->relation.string))
	{
		// validate content-type
		auto content_type = &endpoint->content_type.string;
		if (! str_is(content_type, "plain/text", 10))
			error("unsupported operation content-type");
		compiler_parse(compiler, content);
	} else
	{
		// parse and translate request into statements
		compiler_parse_endpoint(compiler, endpoint, content);
	}

	// generate bytecode (unless EXECUTE)
	auto stmt = compiler_stmt(compiler);
	if (stmt && !compiler->program_udf)
		compiler_emit(compiler);

	auto program = compiler->program;
	if (program_empty(program))
		return;

	// [EXPLAIN]
	if (compiler->program_explain || compiler->program_profile)
		explain(compiler, NULL);

	// explain output
	if (compiler->program_explain)
	{
		Str column;
		str_set(&column, "explain", 7);
		output_write_json(output, &column, program->explain.start, false);
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
session_execute(Session*  self,
                Endpoint* endpoint,
                Str*      content,
                Output*   output)
{
	cancel_pause();

	// execute request based on the content-type
	auto on_error = error_catch
	(
		// reset session session state
		session_reset(self);

		// take shared session lock (for catalog access)
		session_lock(self, LOCK_SHARED);

		// set local settings
		session_set(self, endpoint, output);

		// parse and execute request
		session_endpoint(self, endpoint, content, output);

		// done
		session_unlock(self);
	);

	session_reset_query(self);

	if (on_error)
	{
		buf_reset(output->buf);
		output_write_error(output, &am_self()->error);
		session_unlock(self);
	}

	// cancellation point
	cancel_resume();
	return on_error;
}
