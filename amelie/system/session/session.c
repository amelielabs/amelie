
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
#include <amelie_repl>
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
	self->portal  = NULL;
	self->req     = NULL;
	set_cache_init(&self->set_cache);
	compiler_init(&self->compiler, &self->set_cache);
	vm_init(&self->vm, NULL);
	profile_init(&self->profile);
	gtr_init(&self->gtr);
	return self;
}

static inline void
session_reset_compiler(Session* self)
{
	compiler_reset(&self->compiler);
	palloc_reset();
}

static inline void
session_reset(Session* self)
{
	self->portal = NULL;
	self->req    = NULL;
	vm_reset(&self->vm);
	program_reset(self->program, &self->set_cache);
	gtr_reset(&self->gtr);
	profile_reset(&self->profile);
}

void
session_free(Session *self)
{
	session_reset_compiler(self);
	session_reset(self);
	compiler_free(&self->compiler);
	vm_free(&self->vm);
	program_free(self->program);
	set_cache_free(&self->set_cache);
	gtr_free(&self->gtr);
	am_free(self);
}

hot static inline void
session_run(Session* self)
{
	auto portal   = self->portal;
	auto compiler = &self->compiler;
	auto program  = compiler->program;
	auto profile  = &self->profile;
	auto gtr      = &self->gtr;

	// prevent client writes on replica
	if (!program->ro && !state_is_primary())
		if (! self->req->recover)
			error("system is in read-only mode");

	reg_prepare(&self->vm.r, program->code.regs);

	// take transaction locks
	lock_access(&program->access);

	// prepare global transaction
	gtr_prepare(gtr, &portal->local, portal->user, program);

	// prepare request for the wal writer
	auto write = &gtr->write;
	if (! program->ro)
	{
		auto req = self->req;
		if (req->recover)
			write_set_recover(write, req->recover);
		else
			request_write(req, &portal->endpoint, &write->record_data);

		if (compiler_stmt(compiler)->id == STMT_ACKNOWLEDGE)
			write_set_flags(write, RECORD_UTILITY);
	}

	// [PROFILE]
	if (compiler->program_profile)
		profile_start(&profile->time_run_us);

	// execute coordinator
	Return ret;
	return_init(&ret);
	auto on_error = error_catch
	(
		vm_run(&self->vm, &portal->local,
		       gtr,
		       &gtr->tr,
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
	commit(share()->commit, gtr, error);

	// write result
	auto returning = compiler->program_returning;
	auto output    = &portal->output;
	if (returning && ret.value)
		output_value(output, returning, ret.value);

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
	auto portal   = self->portal;
	auto compiler = &self->compiler;
	auto program  = compiler->program;
	reg_prepare(&self->vm.r, program->code.regs);

	// prevent client writes on replica
	if (! state_is_primary())
	{
		if (!self->req->recover && !stmt_is_utility_ro(compiler_stmt(compiler)))
			error("system is in read-only mode");
	}

	// switch session lock to match the program catalog lock
	//
	// note: user not changed
	//
	portal_lock(portal, program->lock_catalog);

	// prevent concurrent ddls
	lock_system(REL_DDL, program->lock_ddl);

	// [PROFILE]
	auto profile = &self->profile;
	if (compiler->program_profile)
		profile_start(&profile->time_run_us);

	// execute utility/ddl transaction
	Tr tr;
	tr_init(&tr);
	tr_set_user(&tr, &portal->user->rel);
	tr_set_local(&tr, &portal->local);
	defer(tr_free, &tr);

	Return ret;
	return_init(&ret);
	auto on_error = error_catch
	(
		vm_run(&self->vm, &portal->local,
		       &self->gtr,
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

	auto output = &portal->output;
	if (compiler->program_profile)
	{
		profile_end(&profile->time_run_us);
		profile_start(&profile->time_commit_us);
	} else
	{
		// write result
		if (ret.value && compiler->program_returning)
		{
			Str name;
			str_set(&name, "result", 6);

			auto stmt = compiler_stmt(compiler);
			if (stmt->ret)
			{
				auto column = columns_first(&stmt->ret->columns);
				name = column->name;
			}
			output_data(output, &name, ret.value->json, true);
		}
	}

	// wal write
	if (tr_active(&tr))
	{
		Write write;
		write_init(&write);
		defer(write_free, &write);

		// prepare request for the wal writer
		auto req = self->req;
		if (req->recover) {
			write_set_recover(&write, req->recover);
		} else
		{
			write_set_flags(&write, RECORD_UTILITY);
			request_write(req, &portal->endpoint, &write.record_data);
		}

		WriteList write_list;
		write_list_init(&write_list);

		auto on_error = error_catch
		(
			write_list_add(&write_list, &write);

			db_write(share()->db, &write_list);
		);
		if (unlikely(on_error))
		{
			tr_abort(&tr);
			rethrow();
		}

		// capture user request
		if (tr.user->subs)
			write_cdc(&write, share()->cdc, tr.user->id, LOG_REQUEST);
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
session_main(Session* self, Portal* portal, Request* req)
{
	// set session local settings
	self->portal = portal;
	self->req    = req;

	// parser sql
	auto compiler = &self->compiler;
	compiler_set(compiler, &portal->local, self->program);

	switch (req->type) {
	case REQUEST_SQL:
	{
		user_check(portal->user, PERM_SQL);
		compiler_parse(compiler, &req->text);
		break;
	}
	case REQUEST_WRITE:
	case REQUEST_EXECUTE:
	{
		auto execute = req->type == REQUEST_EXECUTE;
		compiler_parse_import(compiler, &req->rel_user, &req->rel,
		                      req->args, execute);
		break;
	}
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
		if (! compiler->program_udf)
			explain(compiler, NULL, NULL);

	// explain output
	if (compiler->program_explain)
	{
		Str column;
		str_set(&column, "explain", 7);
		Str str;
		buf_str(&program->explain, &str);
		output_str(&portal->output, &column, &str);
		return;
	}

	// permission to EXECUTE
	if (stmt->id == STMT_EXECUTE)
		user_check_permission(portal->user, &compiler->program_udf->rel, PERM_EXECUTE);

	// validate user permissions
	user_check_access(portal->user, &program->access);

	// execute utility, DDL, DML or Query
	if (program->utility)
		session_run_utility(self);
	else
		session_run(self);
}

hot bool
session_execute(Session* self, Portal* portal, Request* req)
{
	cancel_pause();

	// execute request based on the content-type
	auto on_error = error_catch
	(
		// reset session session state
		session_reset(self);

		// parse and execute request
		session_main(self, portal, req);

		// done
		unlock_all();
	);

	if (on_error)
	{
		buf_reset(portal->output.buf);
		output_error(&portal->output, &am_self()->error);
		unlock_all();
	}

	session_reset_compiler(self);

	// cancellation point
	cancel_resume();

	// note: request keeps catalog lock
	return !on_error;
}
