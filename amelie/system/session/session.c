
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
#include <amelie_db>
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_compiler>
#include <amelie_frontend.h>
#include <amelie_backend.h>
#include <amelie_session.h>

Session*
session_create(void)
{
	auto self = (Session*)am_malloc(sizeof(Session));
	self->program = program_allocate();
	self->req     = NULL;
	local_init(&self->local);
	set_cache_init(&self->set_cache);
	compiler_init(&self->compiler, &self->local, &self->set_cache);
	vm_init(&self->vm, NULL);
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
	self->req = NULL;
	vm_reset(&self->vm);
	program_reset(self->program, &self->set_cache);
	dtr_reset(&self->dtr);
	profile_reset(&self->profile);
	local_reset(&self->local);
}

void
session_free(Session *self)
{
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

hot static inline void
session_set(Session* self, Request* req)
{
	// set request
	self->req = req;

	// set timezone
	auto local = &self->local;
	local->timezone = req->output.timezone;
	str_set_str(&local->user, &req->user->config->name);

	// update transaction time
	local_update_time(local);
}

hot static inline void
session_run(Session* self)
{
	auto compiler = &self->compiler;
	auto program  = compiler->program;
	auto profile  = &self->profile;
	auto dtr      = &self->dtr;

	// prevent writes on replica
	if (!program->ro && opt_int_of(&state()->read_only))
		error("system is in read-only mode");

	// take transaction locks
	lock_access(&program->access);

	reg_prepare(&self->vm.r, program->code.regs);

	// prepare distributed transaction
	dtr_prepare(dtr, program);
	tr_set_user(&dtr->tr, &self->req->user->rel);

	// [PROFILE]
	if (compiler->program_profile)
		profile_start(&profile->time_run_us);

	// execute coordinator
	Return ret;
	return_init(&ret);
	auto on_error = error_catch
	(
		vm_run(&self->vm, &self->local,
		       dtr,
		       &dtr->tr,
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

	// do group commit and wal write, handle group abort
	commit(share()->commit, dtr, error);

	// write result
	auto returning = compiler->program_returning;
	auto output    = &self->req->output;
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
session_run_utility(Session* self)
{
	auto compiler = &self->compiler;
	auto program  = compiler->program;
	reg_prepare(&self->vm.r, program->code.regs);

	// switch session lock to match the program catalog lock
	//
	// note: user not changed
	//
	request_lock(self->req, program->lock_catalog);

	// prevent concurrent ddls
	lock_system(REL_DDL, program->lock_ddl);

	// [PROFILE]
	auto profile = &self->profile;
	if (compiler->program_profile)
		profile_start(&profile->time_run_us);

	// execute utility/ddl transaction
	Tr tr;
	tr_init(&tr);
	tr_set_user(&tr, &self->req->user->rel);
	defer(tr_free, &tr);

	Return ret;
	return_init(&ret);
	auto on_error = error_catch
	(
		vm_run(&self->vm, &self->local,
		       &self->dtr,
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

	auto output = &self->req->output;
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
	if (tr_active(&tr))
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
		db_write(share()->db, &write_list);

		// update catalog pending lsn
		opt_int_set(&state()->catalog_pending, write.header.lsn);
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
session_request(Session* self)
{
	// parser sql
	auto req = self->req;
	auto compiler = &self->compiler;
	compiler_set(compiler, self->program);

	switch (req->type) {
	case REQUEST_SQL:
		compiler_parse(compiler, &req->text);
		break;
	case REQUEST_INSERT:
	case REQUEST_EXECUTE:
	case REQUEST_PUBLISH:
		break;
	default:
		abort();
		break;
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
		explain(compiler, NULL, NULL);

	// explain output
	if (compiler->program_explain)
	{
		Str column;
		str_set(&column, "explain", 7);
		output_write_json(&req->output, &column, program->explain.start, false);
		return;
	}

	// validate user permissions
	user_check_access(req->user, &program->access);

	// execute utility, DDL, DML or Query
	if (program->utility)
		session_run_utility(self);
	else
		session_run(self);
}

hot bool
session_execute(Session* self, Request* req)
{
	cancel_pause();

	// execute request based on the content-type
	auto on_error = error_catch
	(
		// reset session session state
		session_reset(self);

		// set session local settings
		session_set(self, req);

		// parse and execute request
		session_request(self);

		// done
		unlock_all();
	);

	if (on_error)
	{
		buf_reset(req->output.buf);
		output_write_error(&req->output, &am_self()->error);
		unlock_all();
	}

	session_reset_query(self);

	// cancellation point
	cancel_resume();

	// note: request keeps catalog lock
	return !on_error;
}
