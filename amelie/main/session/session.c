
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
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
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
	self->lock_type = SESSION_LOCK_NONE;
	self->lock      = NULL;
	self->share     = share;
	local_init(&self->local, global());
	explain_init(&self->explain);
	vm_init(&self->vm, share->db, NULL,
	        share->executor,
	        &self->plan,
	        &client->reply.content,
	        share->function_mgr);
	compiler_init(&self->compiler, share->db, share->function_mgr);
	plan_init(&self->plan, &share->cluster->router, &frontend->trx_cache,
	          &frontend->req_cache);
	return self;
}

void
session_free(Session *self)
{
	assert(self->lock_type == SESSION_LOCK_NONE);
	vm_free(&self->vm);
	compiler_free(&self->compiler);
	plan_free(&self->plan);
	local_free(&self->local);
	am_free(self);
}

void
session_lock(Session* self, SessionLock type)
{
	// upgrade lock
	if (self->lock_type == type)
		return;
	if (self->lock_type != SESSION_LOCK_NONE)
		session_unlock(self);

	switch (type) {
	case SESSION_LOCK_NONE:
		break;
	case SESSION_LOCK:
		// take shared frontend lock
		self->lock = lock_lock(&self->frontend->lock, true);
		break;
	case SESSION_LOCK_EXCLUSIVE:
		// take global lock
		control_lock();
		break;
	}
	self->lock_type = type;
}

void
session_unlock(Session* self)
{
	switch (self->lock_type) {
	case SESSION_LOCK_NONE:
		break;
	case SESSION_LOCK:
		lock_unlock(self->lock);
		self->lock = NULL;
		break;
	case SESSION_LOCK_EXCLUSIVE:
		control_unlock();
		break;
	}
	self->lock_type = SESSION_LOCK_NONE;
}

static inline void
session_reset(Session* self)
{
	explain_reset(&self->explain);
	vm_reset(&self->vm);
	compiler_reset(&self->compiler);
	plan_reset(&self->plan);
	palloc_truncate(0);
}

hot static inline void
session_explain(Session* self, bool profile)
{
	auto compiler = &self->compiler;
	auto body     = &self->client->reply.content;
	auto buf = explain(&self->explain,
	                   &compiler->code_coordinator,
	                   &compiler->code_node,
	                   &compiler->code_data,
	                   &self->plan,
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
	auto plan     = &self->plan;

	// todo: shared table lock?

	// generate bytecode
	compiler_emit(&self->compiler);

	// prepare plan
	int stmts = compiler->parser.stmt_list.list_count;
	int last  = -1;
	if (compiler->last)
		last = compiler->last->order;
	plan_create(plan, &self->local, &compiler->code_node,
	            &compiler->code_data,
	            stmts, last);

	// mark plan for distributed snapshot case
	if (compiler->snapshot)
		plan_set_snapshot(plan);

	// explain
	if (compiler->parser.explain == EXPLAIN)
	{
		session_explain(self, false);
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
		       &compiler->code_coordinator,
		       &compiler->code_data,
		       NULL,
		       &plan->cte, NULL, 0);
	}
	if (leave(&e))
	{
		plan_shutdown(plan);
		auto buf = msg_error(&am_self()->error);
		plan_set_error(plan, buf);
	}

	// mark plan as completed
	executor_complete(executor, plan);

	if (profile)
	{
		explain_end(&explain->time_run_us);
		explain_start(&explain->time_commit_us);
	}

	// coordinate wal write and group commit, handle abort
	executor_commit(executor, plan);

	// explain profile
	if (profile)
	{
		explain_end(&explain->time_commit_us);
		session_explain(self, true);
	}
}

static void
session_execute(Session* self)
{
	auto compiler = &self->compiler;

	// set transaction time
	local_update_time(&self->local);

	// take shared session lock (catalog access during parsing)
	session_lock(self, SESSION_LOCK);

	// parse SQL query
	Str text;
	buf_str(&self->client->request.content, &text);
	compiler_parse(compiler, &self->local, &text);

	if (! compiler->parser.stmt_list.list_count)
	{
		session_unlock(self);
		return;
	}

	// execute utility or DDL command
	auto stmt = compiler_stmt(compiler);
	if (stmt && stmt_is_utility(stmt))
		session_execute_utility(self);
	else
		// DML or Select
		session_execute_distributed(self);

	session_unlock(self);
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
		// read request
		http_reset(request);
		auto eof = http_read(request, readahead, true);
		if (unlikely(eof))
			break;
		http_read_content(request, readahead, &request->content);
		http_reset(reply);

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
			session_reset(self);
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
