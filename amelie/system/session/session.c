
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
#include <amelie_heap>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_compiler>
#include <amelie_compute.h>
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
	quota_init(&self->quota);
	vm_init(&self->vm, &self->quota);
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
	quota_reset(&self->quota);
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
session_retention(Access* access, User* user)
{
	// execute only if no commit pending wal writes for the user
	if (access->ro || user->commit > 0)
		return;

	for (auto i = 0; i < access->count; i++)
	{
		auto ref = access_at(access, i);
		if (! permission_is_dml(ref->perm))
			continue;
		auto table = table_of(ref->rel);
		if (table->config->retention == -1)
			continue;
		part_retention(table->part);
		subs_cancel(&table->part->subs);
	}
}

hot static inline void
session_streaming_resume(Access* access)
{
	if (access->ro)
		return;

	for (auto i = 0; i < access->count; i++)
	{
		auto ref = access_at(access, i);
		if (! permission_is_dml(ref->perm))
			continue;
		auto subs = &table_of(ref->rel)->part->subs;
		if (subs_empty(subs))
			continue;
		subs_resume(subs);
	}
}

hot static inline void
session_run(Session* self)
{
	auto portal   = self->portal;
	auto compiler = &self->compiler;
	auto program  = compiler->program;
	auto access   = &program->access;
	auto profile  = &self->profile;
	auto gtr      = &self->gtr;
	auto req      = self->req;

	// prevent client writes on replica
	auto ro = access_ro(access);
	if (!req->replay && !ro && !state_is_primary())
		error("system is in read-only mode");

	// [PROFILE]
	if (compiler->program_profile)
		profile_start(&profile->time_run_us);

	// set compute limit
	auto limits = portal->local.limits;
	if (limits)
	{
		auto quota = &self->quota;
		if (limits_is_set(limits, LIMIT_COMPUTE))
			quota_set(quota, limits_get(limits, LIMIT_COMPUTE));
	}

	// begin transaction
	gtr_begin(gtr, &portal->local, portal->user, access);

	// do retention
	session_retention(access, portal->user);

	// execute
	Return ret;
	return_init(&ret);
	auto on_error = error_catch
	(
		vm_run(&self->vm,
		       gtr,
		       program,
		       compiler->program_args,
		       &ret);
	);
	if (on_error)
	{
		tr_abort(&gtr->tr);
		rethrow();
	}

	if (compiler->program_profile)
	{
		profile_end(&profile->time_run_us);
		profile_start(&profile->time_commit_us);
	}

	// create reply
	auto returning = compiler->program_returning;
	auto output = &portal->output;
	if (returning && ret.value)
		output_value(output, returning, ret.value);

	// wal write
	if (!req->replay && !ro)
	{
		auto write = &gtr->write;
		request_write(req, &portal->endpoint, &write->record_data);

		commit(share()->commit, gtr);
	}

	// commit
	tr_commit(&gtr->tr);

	// explain profile
	if (compiler->program_profile)
	{
		profile_end(&profile->time_commit_us);
		profile_create(profile, &compiler->explain, output);
	}

	// resume streaming
	session_streaming_resume(access);
}

hot static inline void
session_run_utility(Session* self)
{
	auto portal   = self->portal;
	auto compiler = &self->compiler;
	auto program  = compiler->program;
	auto gtr      = &self->gtr;
	auto req      = self->req;

	// prevent client writes on replica
	auto ro = stmt_is_utility_ro(compiler_stmt(compiler));
	if (!req->replay && !ro && !state_is_primary())
		error("system is in read-only mode");

	// todo: take system lock

	// begin transaction
	gtr_begin(gtr, &portal->local, portal->user, &program->access);

	// [PROFILE]
	auto profile = &self->profile;
	if (compiler->program_profile)
		profile_start(&profile->time_run_us);

	// execute utility/ddl transaction
	Return ret;
	return_init(&ret);
	auto on_error = error_catch
	(
		vm_run(&self->vm,
		       gtr,
		       program,
		       NULL,
		       &ret);
	);

	if (unlikely(on_error))
	{
		tr_abort(&gtr->tr);
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
	if (tr_active(&gtr->tr) && !req->replay)
	{
		Write write;
		write_init(&write);
		defer(write_free, &write);
		write_set_flags(&write, RECORD_UTILITY);

		request_write(req, &portal->endpoint, &write.record_data);

		WriteList write_list;
		write_list_init(&write_list);
		write_list_add(&write_list, &write);
		db_write(share()->db, &write_list);
	}

#if 0
	// wal write
	if (!recovery && !ro && tr_active(&gtr->tr))
	{
		auto write = &gtr->write;
		write_set_flags(write, RECORD_UTILITY);
		request_write(self->req, &portal->endpoint, &write->record_data);

		commit(share()->commit, gtr);
	}
#endif

	// commit
	tr_commit(&gtr->tr);

	// profile
	if (compiler->program_profile)
	{
		profile_end(&profile->time_commit_us);
		profile_create(profile, &compiler->explain, output);
	}
}

hot static void
session_main(Session* self, Portal* portal, Request* req)
{
	// set session local settings
	self->portal = portal;
	self->req    = req;

	// parse request
	auto compiler = &self->compiler;
	compiler_set(compiler, portal->user, &portal->local, self->program);

	if (str_empty(&req->rel))
	{
		// check permission
		if (! portal->user->config->superuser)
			check_permission_on(&portal->user->config->grants, PERM_SQL);

		compiler_parse(compiler, &req->content);
	} else
	{
		compiler_parse_copy(compiler, &req->rel,
		                    &req->content_type,
		                    &req->content);
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
		explain(compiler, compiler->program, compiler->program_udf, true);

	// explain output
	if (compiler->program_explain)
	{
		Str column;
		str_set(&column, "explain", 7);
		Str str;
		buf_str(&compiler->explain, &str);
		output_str(&portal->output, &column, &str);
		return;
	}

#if 0
	// permission to EXECUTE
	if (stmt->id == STMT_EXECUTE)
		user_check_permission(portal->user, &compiler->program_udf->rel,
		                      PERM_EXECUTE);

	// validate user permissions
	user_check_access(portal->user, &program->access);
#endif

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
