
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
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
#include <amelie_aggr.h>
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

Session*
session_create(Client* client, Frontend* frontend, Share* share)
{
	auto self = (Session*)am_malloc(sizeof(Session));
	self->client    = client;
	self->frontend  = frontend;
	self->lock_type = LOCK_NONE;
	self->lock      = NULL;
	self->lock_ref  = NULL;
	self->share     = share;
	local_init(&self->local, global());
	explain_init(&self->explain);
	vm_init(&self->vm, share->db, NULL,
	        share->executor,
	        &self->dtr,
	        &client->reply.content,
	        share->function_mgr);
	compiler_init(&self->compiler, share->db, share->function_mgr);
	dtr_init(&self->dtr, &share->cluster->router);
	return self;
}

static inline void
session_reset(Session* self)
{
	explain_reset(&self->explain);
	vm_reset(&self->vm);
	compiler_reset(&self->compiler);
	dtr_reset(&self->dtr);
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
	case LOCK:
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
	case LOCK:
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
session_explain(Session* self, Program* program, bool profile)
{
	auto body = &self->client->reply.content;
	auto buf  = explain(&self->explain,
	                    program->code,
	                    program->code_node,
	                    program->code_data,
	                    &self->dtr,
	                    body,
	                    profile);
	guard_buf(buf);
	buf_reset(body);
	body_add_buf(body, buf, self->local.timezone);
}

hot static inline void
session_execute_distributed(Session* self)
{
	auto compiler = &self->compiler;
	auto executor = self->share->executor;
	auto explain  = &self->explain;
	auto dtr      = &self->dtr;

	// generate bytecode
	Program  program_compiled;
	Program* program;
	program = &program_compiled;
	compiler_emit(compiler);
	compiler_program(compiler, &program_compiled);

	// prepare distributed transaction
	dtr_create(dtr, &self->local, program, NULL);

	// explain
	if (compiler->parser.explain == EXPLAIN)
	{
		session_explain(self, program, false);
		return;
	}

	// profile
	auto profile = parser_is_profile(&compiler->parser);
	if (profile)
		explain_start(&explain->time_run_us);

	// execute coordinator
	Exception e;
	if (enter(&e))
	{
		vm_run(&self->vm, &self->local,
		       NULL,
		       program->code,
		       program->code_data,
		       NULL,
		       NULL,
		       &dtr->cte, NULL, 0);
	}

	Buf* error = NULL;
	if (leave(&e))
		error = msg_error(&am_self()->error);

	if (profile)
	{
		explain_end(&explain->time_run_us);
		explain_start(&explain->time_commit_us);
	}

	// coordinate wal write and group commit, handle abort
	executor_commit(executor, dtr, error);

	// explain profile
	if (profile)
	{
		explain_end(&explain->time_commit_us);
		session_explain(self, program, true);
	}
}

hot static void
session_execute_sql(Session* self)
{
	auto compiler = &self->compiler;

	// parse SQL query
	Str text;
	buf_str(&self->client->request.content, &text);
	compiler_parse(compiler, &self->local, NULL, &text);

	if (! compiler->parser.stmt_list.list_count)
	{
		session_unlock(self);
		return;
	}

	// execute utility, DDL, DML or Query
	auto stmt = compiler_stmt(compiler);
	auto utility = stmt &&
	               stmt_is_utility(stmt) &&
	               stmt->id != STMT_EXECUTE;
	if (utility)
		session_execute_utility(self);
	else
		session_execute_distributed(self);
}

static void
session_execute(Session* self)
{
	auto client  = self->client;
	auto request = &client->request;

	// prepare session state for execution
	session_reset(self);

	// set transaction time
	local_update_time(&self->local);

	// take shared session lock (for catalog access)
	session_lock(self, LOCK);

	auto url = &request->options[HTTP_URL];
	if (str_compare_raw(url, "/", 1))
	{
		// POST /
		auto type = http_find(request, "Content-Type", 12);
		if (unlikely(! type))
			error("Content-Type is missing");

		if (! str_compare_raw(&type->value, "text/plain", 10))
			error("unsupported API operation");

		session_execute_sql(self);
	} else
	{
		// POST /schema/table
		session_execute_import(self);
	}

	session_unlock(self);
}

hot static inline void
session_read(Session* self)
{
	auto client = self->client;
	auto limit = var_int_of(&config()->limit_recv);
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

	// POST /schema/table
	// POST /
	auto method = &request->options[HTTP_METHOD];
	if (unlikely(! str_compare_raw(method, "POST", 4)))
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
	auto reply     = &client->reply;
	auto body      = &reply->content;

	for (;;)
	{
		// read request header
		http_reset(reply);
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
			if (str_compare_raw(&service->value, "backup", 6))
				backup(self->share->db, client);
			else
			if (str_compare_raw(&service->value, "repl", 4))
				session_primary(self);
			break;
		}

		cancel_pause();

		// execute request
		Exception e;
		if (enter(&e))
		{
			session_read(self);
			session_execute(self);
		}

		// reply
		if (leave(&e))
		{
			auto error = &am_self()->error;
			buf_reset(body);
			body_error(body, error);

			session_unlock(self);

			// 400 Bad Request
			client_400(client, body);
		} else
		{
			// 204 No Content
			// 200 OK
			if (buf_empty(body))
				client_204(client);
			else
				client_200(client, body);
		}

		// cancellation point
		cancel_resume();
	}
}
