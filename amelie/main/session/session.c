
//
// amelie.
//
// Real-Time SQL Database.
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

	Program* program;
	Program  program_compiled;
	Buf*     args;

	auto stmt = compiler_stmt(compiler);
	if (stmt && stmt->id == STMT_EXECUTE)
	{
		// function call
		auto udf = ast_execute_of(stmt->ast)->udf;
		auto executable = (Executable*)udf->context;
		program = &executable->program;
		args = &compiler->code_data.data;
	} else
	{
		// generate bytecode
		compiler_emit(compiler);
		compiler_program(compiler, &program_compiled);
		program = &program_compiled;
		args = NULL;
	}

	// prepare distributed transaction
	dtr_create(dtr, &self->local, program, args);

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
		       args,
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

static void
session_execute(Session* self)
{
	auto compiler = &self->compiler;

	// prepare session state for execution
	session_reset(self);

	// set transaction time
	local_update_time(&self->local);

	// take shared session lock (catalog access during parsing)
	session_lock(self, LOCK);

	// parse SQL query
	Str text;
	buf_str(&self->client->request.content, &text);
	compiler_parse(compiler, &self->local, NULL, &text);

	if (! compiler->parser.stmt_list.list_count)
	{
		session_unlock(self);
		auto body = &self->client->reply.content;
		body_empty(body);
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
	auto reply   = &client->reply;
	auto auth_header = http_find(request, "Authorization", 13);
	if (auth_header)
	{
		auto user = auth(&self->frontend->auth, &auth_header->value);
		if (! user)
		{
			http_write_reply(reply, 401, "Unauthorized");
			http_write_end(reply);
			tcp_write_buf(&client->tcp, &reply->raw);
			return false;
		}
	}
	return true;
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

		// handle backup
		auto url = &request->options[HTTP_URL];
		if (unlikely(str_compare_raw(url, "/backup", 7)))
		{
			backup(self->share->db, client);
			break;
		}

		// handle primary connection
		if (unlikely(str_compare_raw(url, "/repl", 5)))
		{
			session_primary(self);
			break;
		}

		// execute
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
			http_write_reply(reply, 400, "Bad Request");
			http_write(reply, "Content-Length", "%" PRIu64, buf_size(body));
			http_write(reply, "Content-Type", "application/json");
			http_write_end(reply);

			session_unlock(self);
		} else {
			http_write_reply(reply, 200, "OK");
			http_write(reply, "Content-Length", "%" PRIu64, buf_size(body));
			http_write(reply, "Content-Type", "application/json");
			http_write_end(reply);
		}

		tcp_write_pair(&client->tcp, &reply->raw, body);
	}
}
