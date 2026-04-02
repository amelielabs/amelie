
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
session_create(Frontend* frontend)
{
	auto self = (Session*)am_malloc(sizeof(Session));
	self->program  = program_allocate();
	self->lock     = NULL;
	self->user     = NULL;
	self->frontend = frontend;
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
	self->lock = NULL;
	self->user = NULL;
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
session_auth(Session* self, Endpoint* endpoint, Output* output)
{
	// authenticate user request
	auto token = &endpoint->token.string;
	auto token_required = opt_int_of(&endpoint->auth);
	auto user_id = &endpoint->user.string;
	self->user = auth(&self->frontend->auth, user_id, token, token_required);

	/*
	// PERM_CONNECT permission
	if (token_required)
		user_check(self->user, PERM_CONNECT);
	*/

	// check service grants
	if (unlikely(! opt_string_empty(&endpoint->service)))
		user_check(self->user, PERM_SERVICE);

	// set timezone / format
	auto local = &self->local;
	local->timezone = output->timezone;
	local->format   = output->format;
	str_set_str(&local->user, user_id);

	// update transaction time
	local_update_time(local);
}

hot static inline void
session_execute_distributed(Session* self, Output* output)
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

	// do group commit and wal write, handle group abort
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

	// switch session lock to use program utility lock
	if (program->lock_catalog != LOCK_SHARED)
	{
		unlock(self->lock);
		self->lock = lock_system(REL_CATALOG, program->lock_catalog);
	}

	// prevent concurrent ddls
	lock_system(REL_DDL, program->lock_ddl);

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
		tr_set_user(&tr, &self->user->rel);

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

hot static SessionStatus
session_endpoint(Session*  self,
                 Endpoint* endpoint,
                 Str*      content,
                 Output*   output)
{
	// POST /v1/backup
	// POST /v1/repl
	auto service = &endpoint->service.string;
	if (unlikely(! str_empty(service)))
	{
		// backup or primary server connection
		if (str_is(service, "backup", 6))
			return SESSION_BACKUP;
		if (str_is(service, "repl", 4))
			return SESSION_REPL;
		return SESSION_ERROR;
	}

	// POST /v1/db
	// POST /v1/db/<relation>

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
		return SESSION_OK;

	// [EXPLAIN]
	if (compiler->program_explain || compiler->program_profile)
		explain(compiler, NULL, NULL);

	// explain output
	if (compiler->program_explain)
	{
		Str column;
		str_set(&column, "explain", 7);
		output_write_json(output, &column, program->explain.start, false);
		return SESSION_OK;
	}

	// validate user permissions
	user_check_access(self->user, &program->access);

	// execute utility, DDL, DML or Query
	if (program->utility)
		session_execute_utility(self, output);
	else
		session_execute_distributed(self, output);
	return SESSION_OK;
}

hot SessionStatus
session_execute(Session*  self,
                Endpoint* endpoint,
                Str*      content,
                Output*   output)
{
	SessionStatus status = SESSION_OK;
	cancel_pause();

	// execute request based on the content-type
	auto on_error = error_catch
	(
		// reset session session state
		session_reset(self);

		// take shared catalog lock
		self->lock = lock_system(REL_CATALOG, LOCK_SHARED);

		// authenticate and set session local settings
		session_auth(self, endpoint, output);

		// parse and execute request
		status = session_endpoint(self, endpoint, content, output);

		// done
		unlock_all();
	);

	if (on_error)
	{
		auto error = &am_self()->error;
		if (error->code == ERROR_AUTH)
			status = SESSION_ERROR_AUTH;
		else
			status = SESSION_ERROR;
		buf_reset(output->buf);
		output_write_error(output, &am_self()->error);
		unlock_all();
	}

	session_reset_query(self);

	// cancellation point
	cancel_resume();
	return status;
}
