
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
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
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
session_create(Client* client, FrontendMgr* frontend_mgr, Frontend* frontend)
{
	auto self = (Session*)am_malloc(sizeof(Session));
	self->client       = client;
	self->lock_type    = LOCK_NONE;
	self->lock         = NULL;
	self->lock_ref     = NULL;
	self->frontend_mgr = frontend_mgr;
	self->frontend     = frontend;
	local_init(&self->local, global());
	explain_init(&self->explain);
	content_init(&self->content, &self->local, &client->reply.content);
	compiler_init(&self->compiler, &self->local);
	vm_init(&self->vm, NULL, &self->dtr);
	dtr_init(&self->dtr, &self->local, share()->core_mgr);
	return self;
}

static inline void
session_reset(Session* self)
{
	vm_reset(&self->vm);
	compiler_reset(&self->compiler);
	dtr_reset(&self->dtr);
	explain_reset(&self->explain);
	content_reset(&self->content);
	palloc_truncate(0);
}

void
session_free(Session *self)
{
	assert(self->lock_type == LOCK_NONE);
	session_reset(self);
	vm_free(&self->vm);
	compiler_free(&self->compiler);
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
session_execute_distributed(Session* self, Program* program)
{
	auto explain  = &self->explain;
	auto dtr      = &self->dtr;
	auto executor = share()->executor;

	reg_prepare(&self->vm.r, program->code.regs);

	// prepare distributed transaction
	dtr_create(dtr, program);

	// [PROFILE]
	if (program->profile)
		explain_start(&explain->time_run_us);

	// execute coordinator
	auto on_error = error_catch
	(
		vm_run(&self->vm, &self->local,
		       NULL,
		       program,
		       &program->code,
		       &program->code_data,
		       NULL,
		       &self->vm.r,
		       NULL,
		       NULL,
		       &self->content, 0);
	);

	Buf* error = NULL;
	if (on_error)
		error = msg_error(&am_self()->error);

	if (program->profile)
	{
		explain_end(&explain->time_run_us);
		explain_start(&explain->time_commit_us);
	}

	// coordinate wal write and group commit, handle abort
	executor_commit(executor, dtr, error);

	// explain profile
	if (program->profile)
	{
		explain_end(&explain->time_commit_us);
		explain_run(explain, program,
		            &self->local,
		            &self->content,
		            true);
	}
}

hot static inline void
session_execute_utility(Session* self, Program* program)
{
	auto explain = &self->explain;
	reg_prepare(&self->vm.r, program->code.regs);

	// [PROFILE]
	if (program->profile)
		explain_start(&explain->time_run_us);

	// execute utility/ddl transaction
	Tr tr;
	tr_init(&tr);
	defer(tr_free, &tr);
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
		       &self->vm.r,
		       NULL,
		       NULL,
		       &self->content, 0);
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
		explain_run(explain, program,
		            &self->local,
		            &self->content,
		            true);
	}
}

static void
session_execute(Session* self)
{
	auto request  = &self->client->request;
	auto compiler = &self->compiler;

	// set transaction time
	local_update_time(&self->local);

	// POST /
	// POST /v1/execute
	// POST /v1/db/schema/relation
	auto type = http_find(request, "Content-Type", 12);
	if (unlikely(! type))
		error("Content-Type is missing");
	auto url = &request->options[HTTP_URL];

	Str text;
	buf_str(&request->content, &text);
	if (str_is(&type->value, "text/plain", 10) ||
	    str_is(&type->value, "application/sql", 15))
	{
		if (unlikely(!str_is(url, "/", 1) &&
		             !str_is(url, "/v1/execute", 11)))
			error("unsupported API operation");

		// parse SQL
		compiler_parse(compiler, &text);
	} else
	if (str_is(&type->value, "text/csv", 8))
	{
		// parse CSV
		compiler_parse_import(compiler, &text, url, ENDPOINT_CSV);
	} else
	if (str_is(&type->value, "application/jsonl", 17))
	{
		// parse JSONL
		compiler_parse_import(compiler, &text, url, ENDPOINT_JSONL);
	} else
	if (str_is(&type->value, "application/json", 16))
	{
		// parse JSON
		compiler_parse_import(compiler, &text, url, ENDPOINT_JSON);
	} else
	{
		error("unsupported API operation");
	}

	// compile
	if (! compiler->parser.stmts.count)
		return;

	// generate bytecode
	compiler_emit(compiler);

	// [EXPLAIN]
	auto program = compiler->program;
	if (program->explain)
	{
		explain_run(&self->explain, program,
		            &self->local,
		            &self->content,
		            false);
		return;
	}

	// take the required program lock
	session_lock(self, program->lock);

	// execute utility, DDL, DML or Query
	if (stmt_is_utility(compiler_stmt(compiler)))
		session_execute_utility(self, program);
	else
		session_execute_distributed(self, program);
}

hot static inline void
session_read(Session* self)
{
	auto client = self->client;
	auto limit = opt_int_of(&config()->limit_recv);
	auto limit_reached =
		http_read_content_limit(&client->request,
		                        &client->readahead,
		                        &client->request.content,
		                         limit);
	if (unlikely(limit_reached))
		error("http request limit reached");
}

hot static inline bool
session_auth(Session* self)
{
	auto client  = self->client;
	auto request = &client->request;

	auto method = &request->options[HTTP_METHOD];
	if (unlikely(! str_is(method, "POST", 4)))
		goto forbidden;

	auto auth_header = http_find(request, "Authorization", 13);
	if (! auth_header)
	{
		// trusted by the server listen configuration
		if (! client->auth)
			return true;
	} else
	{
		auto user = auth(&self->frontend->auth, &auth_header->value);
		if (likely(user))
			return true;
	}

forbidden:
	// 403 Forbidden
	client_403(client);
	return false;
}

hot void
session_main(Session* self)
{
	auto client    = self->client;
	auto readahead = &client->readahead;
	auto request   = &client->request;
	auto content   = &self->content;

	for (;;)
	{
		// read request header
		http_reset(request);
		auto eof = http_read(request, readahead, true);
		if (unlikely(eof))
			break;

		// authenticate
		if (! session_auth(self))
			break;

		// handle backup or primary server connection
		auto service = http_find(request, "Am-Service", 10);
		if (service)
		{
			if (str_is(&service->value, "backup", 6))
				backup(share()->db, client);
			else
			if (str_is(&service->value, "repl", 4))
				session_primary(self);
			break;
		}

		cancel_pause();

		// execute request based on the content-type
		auto on_error = error_catch
		(
			// prepare session state for execution
			session_reset(self);

			// read content
			session_read(self);

			// take shared session lock (for catalog access)
			session_lock(self, LOCK_SHARED);

			// parse and execute request
			session_execute(self);

			// done
			session_unlock(self);
		);

		// reply
		if (on_error)
		{
			content_reset(content);
			content_write_json_error(content, &am_self()->error);

			session_unlock(self);

			// 400 Bad Request
			client_400(client, content->content, content->content_type->mime);
		} else
		{
			// 204 No Content
			// 200 OK
			if (buf_empty(content->content))
				client_204(client);
			else
				client_200(client, content->content, content->content_type->mime);
		}

		// cancellation point
		cancel_resume();
	}
}
