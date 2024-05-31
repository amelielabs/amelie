
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_node.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_semantic.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>
#include <sonata_repl.h>
#include <sonata_shard.h>
#include <sonata_frontend.h>
#include <sonata_session.h>

Session*
session_create(Client* client, Frontend* frontend, Share* share)
{
	auto self = (Session*)so_malloc(sizeof(Session));
	self->client   = client;
	self->frontend = frontend;
	self->lock     = NULL;
	self->share    = share;
	explain_init(&self->explain);
	vm_init(&self->vm, share->db, NULL,
	        share->executor,
	        &self->plan,
	        &client->reply.content,
	        share->function_mgr);
	compiler_init(&self->compiler, share->db, share->function_mgr);
	plan_init(&self->plan, share->router, &frontend->trx_cache,
	          &frontend->req_cache);
	return self;
}

void
session_free(Session *self)
{
	assert(! self->lock);
	vm_free(&self->vm);
	compiler_free(&self->compiler);
	plan_free(&self->plan);
	so_free(self);
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

static inline void
session_lock(Session* self)
{
	// take shared frontend lock
	self->lock = lock_lock(&self->frontend->lock, true);
}

static inline void
session_unlock(Session* self)
{
	if (! self->lock)
		return;
	lock_unlock(self->lock);
	self->lock = NULL;
}

hot static inline void
session_explain(Session* self)
{
	// todo: profile
	(void)self;
	auto buf = explain(&self->explain,
	                   &self->compiler.code_coordinator,
	                   &self->compiler.code_shard,
	                   &self->compiler.code_data,
	                   &self->plan,
	                   false);
	guard_buf(buf);
	body_add_buf(&self->client->reply.content, buf);
}

hot static inline void
session_execute_distributed(Session* self)
{
	auto compiler = &self->compiler;
	auto executor = self->share->executor;
	auto plan = &self->plan;

	// todo: reference lock?

	// generate bytecode
	compiler_emit(&self->compiler);

	// prepare plan
	int stmts = compiler->parser.stmt_list.list_count;
	int last  = -1;
	if (compiler->last)
		last = compiler->last->order;
	plan_create(plan, &compiler->code_shard, &compiler->code_data, stmts, last);

	// mark plan for distributed snapshot case
	if (compiler->snapshot)
		plan_set_snapshot(plan);

	// explain
	if (compiler->parser.explain != EXPLAIN_NONE)
	{
		session_explain(self);
		return;
	}

	// execute coordinator
	Exception e;
	if (enter(&e))
	{
		vm_run(&self->vm, NULL,
		       &compiler->code_coordinator,
		       &compiler->code_data,
		       NULL,
		       &plan->cte, NULL, 0);
	}
	if (leave(&e))
	{
		plan_shutdown(plan);
		auto buf = msg_error(&so_self()->error);
		plan_set_error(plan, buf);
	}

	// mark plan as completed
	executor_complete(executor, plan);

	// coordinate wal write and group commit, handle abort
	executor_commit(executor, plan);
}

static void
session_execute(Session* self)
{
	auto compiler = &self->compiler;

	// take shared session lock (catalog access during parsing)
	session_lock(self);

	// parse SQL query
	Str text;
	buf_str(&self->client->request.content, &text);
	compiler_parse(compiler, &text);

	if (! compiler->parser.stmt_list.list_count)
	{
		session_unlock(self);
		return;
	}

	// execute utility or DDL command by system
	auto stmt = compiler_stmt(compiler);
	if (stmt && stmt_is_utility(stmt))
	{
		session_unlock(self);

		Buf* buf = NULL;
		rpc(global()->control->system, RPC_CTL, 3, self, stmt, &buf);
		if (buf)
		{
			guard_buf(buf);
			body_add_buf(&self->client->reply.content, buf);
		}
		return;
	}

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

		// handle backup request
		auto url = &request->options[HTTP_URL];
		if (unlikely(str_compare_raw(url, "/backup", 7)))
		{
			backup(self->share->db, client);
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
			session_unlock(self);

			auto error = &so_self()->error;
			buf_reset(body);
			body_error(body, error);
			http_write_reply(reply, 400, "Bad Request");
			http_write(reply, "Content-Length", "%" PRIu64, buf_size(body));
			http_write(reply, "Content-Type", "application/json");
			http_write_end(reply);
		} else {
			http_write_reply(reply, 200, "OK");
			http_write(reply, "Content-Length", "%" PRIu64, buf_size(body));
			http_write(reply, "Content-Type", "application/json");
			http_write_end(reply);
		}

		tcp_write_pair(&client->tcp, &reply->raw, body);
	}
}
