
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
#include <amelie_io.h>
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
#include <amelie_system.h>

static void
system_save_state(void* arg)
{
	unused(arg);
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/state.json", state_directory());
	state_save(state(), path);
}

static void
proc_if_prepare(Proc* self)
{
	System* system = self->iface_arg;

	// parse and compile procedure
	Local local;
	local_init(&local, global());
	local_update_time(&local);
	Compiler compiler;
	compiler_init(&compiler, &system->db, &local, &system->function_mgr, NULL);
	defer(compiler_free, &compiler);

	// create parsing scope
	auto scope = scopes_add(&compiler.parser.scopes, NULL, self);

	// create arguments as variables
	list_foreach(&self->config->columns.list)
	{
		auto column = list_at(Column, link);
		auto var = vars_add(&scope->vars, &column->name);
		var->type = column->type;
		var->r = rpin(&compiler, column->type);
	}

	// create variables
	list_foreach(&self->config->vars.list)
	{
		auto column = list_at(Column, link);
		auto var = vars_add(&scope->vars, &column->name);
		var->type = column->type;
		var->r = rpin(&compiler, column->type);
	}

	// parse procedure
	auto parser = &compiler.parser;
	auto lex = &parser->lex;
	lex_start(lex, &self->config->text);
	parse_scope(parser, scope);

	// force last statetement as return
	if (parser->stmt)
		parser->stmt->ret = true;

	// ensure EXPLAIN has command
	if (unlikely(parser->explain))
		error("EXPLAIN cannot be used inside procedure");

	// ensure main stmt is not utility when using CTE
	if (parser->stmts.count_utility > 0)
		error("utility commands cannot be used inside procedure");

	// ensure EXECUTE used only once
	if (parser->execute)
		error("EXECUTE command cannot be used inside procedure");

	// set statements order
	stmts_order(&parser->stmts);

	// emit procedure
	compiler_emit(&compiler);

	self->data = compiler.program;
	compiler.program = NULL;
}

static void
proc_if_free(Proc* self)
{
	Program* program = self->data;
	if (! program)
		return;
	program_free(program);
	self->data = NULL;
}

static bool
proc_if_depend(Proc* self, Str* schema, Str* name)
{
	Program* program = self->data;
	if (name)
		return access_find(&program->access, schema, name) != NULL;
	return access_find_schema(&program->access, schema) != NULL;
}

ProcIf proc_if =
{
	.prepare = proc_if_prepare,
	.free    = proc_if_free,
	.depend  = proc_if_depend
};

static void
system_attach(PartList* list, void* arg)
{
	// redistribute partitions across backends
	System* self = arg;
	auto backend_mgr = &self->backend_mgr;
	if (list->list_count > PARTITION_MAX)
		error("exceeded the maximum number of partitions per table");
	// extend backend pool
	backend_mgr_ensure(backend_mgr, list->list_count);
	auto order = 0;
	list_foreach(&list->list)
	{
		auto part = list_at(Part, link);
		part->core = &backend_mgr->workers[order]->core;
		order++;
	}
}

System*
system_create(void)
{
	System* self = am_malloc(sizeof(System));
	self->lock = false;

	// set control
	auto control = &self->control;
	control->system     = &am_task->channel;
	control->save_state = system_save_state;
	control->arg        = self;
	global()->control   = control;

	// server
	user_mgr_init(&self->user_mgr);
	server_init(&self->server);

	// frontend/backend mgr
	frontend_mgr_init(&self->frontend_mgr);
	backend_mgr_init(&self->backend_mgr, &self->db, &self->executor, &self->function_mgr);
	executor_init(&self->executor, &self->db, &self->backend_mgr.core_mgr);
	rpc_queue_init(&self->lock_queue);

	// vm
	function_mgr_init(&self->function_mgr);

	// db
	db_init(&self->db, system_attach, self, &proc_if, self);

	// replication
	repl_init(&self->repl, &self->db);

	// prepare shared context (shared between frontends)
	auto share = &self->share;
	share->executor     = &self->executor;
	share->repl         = &self->repl;
	share->frontend_mgr = &self->frontend_mgr;
	share->backend_mgr  = &self->backend_mgr;
	share->function_mgr = &self->function_mgr;
	share->user_mgr     = &self->user_mgr;
	share->db           = &self->db;
	return self;
}

void
system_free(System* self)
{
	repl_free(&self->repl);
	executor_free(&self->executor);
	db_free(&self->db);
	function_mgr_free(&self->function_mgr);
	server_free(&self->server);
	user_mgr_free(&self->user_mgr);
	am_free(self);
}

static void
system_on_server_connect(Server* server, Client* client)
{
	auto buf = msg_create(MSG_CLIENT);
	buf_write(buf, &client, sizeof(void**));
	msg_end(buf);
	System* self = server->on_connect_arg;
	frontend_mgr_forward(&self->frontend_mgr, buf);
}

static void
system_on_frontend_connect(Frontend* frontend, Client* client)
{
	System* self = frontend->on_connect_arg;
	auto session = session_create(client, frontend, &self->share);
	defer(session_free, session);
	session_main(session);
}

static void
system_recover(System* self)
{
	// ask each backend to recover last checkpoint partitions in parallel
	int workers = opt_int_of(&config()->backends);
	info("⟶ recovery (checkpoint %" PRIu64 ", using %d backends)",
	     state_checkpoint(), workers);

	Build build;
	build_init(&build, BUILD_RECOVER, &self->backend_mgr, NULL, NULL, NULL, NULL);
	defer(build_free, &build);
	build_run(&build);

	// replay wals
	Recover recover;
	recover_init(&recover, &self->db, false, &build_if, &self->backend_mgr);
	defer(recover_free, &recover);
	recover_wal(&recover);

	info("complete");
}

static void
system_bootstrap(System* self)
{
	state_lsn_set(1);

	// create initial checkpoint
	Checkpoint cp;
	checkpoint_init(&cp, &self->db.checkpoint_mgr);
	defer(checkpoint_free, &cp);
	checkpoint_begin(&cp, 1, 1);
	checkpoint_run(&cp);
	checkpoint_wait(&cp);
	info("");
}

void
system_start(System* self, bool bootstrap)
{
	// hello
	auto version = &state()->version.string;
	info("amelie %.*s", str_size(version), str_of(version));

	// show system options
	info("");
	if (bootstrap || opt_int_of(&config()->log_options))
	{
		opts_print(&config()->opts);
		info("");
	}

	// register builtin functions
	fn_register(&self->function_mgr);

	// open user manager
	user_mgr_open(&self->user_mgr);

	// create system object and objects from last snapshot (including backends)
	db_open(&self->db);

	// do parallel recover of snapshots and wal
	system_recover(self);

	// do initial deploy
	if (bootstrap)
		system_bootstrap(self);

	// start checkpointer service
	checkpointer_start(&self->db.checkpointer);

	// start periodic async wal fsync
	wal_periodic_start(&self->db.wal_mgr.wal_periodic);

	// start frontends
	int workers = opt_int_of(&config()->frontends);
	frontend_mgr_start(&self->frontend_mgr,
	                   system_on_frontend_connect,
	                   self,
	                   workers);

	// synchronize caches
	frontend_mgr_sync_users(&self->frontend_mgr, &self->user_mgr.cache);

	// prepare replication manager
	repl_open(&self->repl);

	// start server
	server_start(&self->server, system_on_server_connect, self);

	// start replication
	if (opt_int_of(&state()->repl))
	{
		opt_int_set(&state()->repl, false);
		repl_start(&self->repl);
	}
}

void
system_stop(System* self)
{
	info("");
	info("received shutdown request");

	// stop server
	server_stop(&self->server);

	// stop replication
	repl_stop(&self->repl);

	// stop frontends
	frontend_mgr_stop(&self->frontend_mgr);

	// stop backends
	backend_mgr_stop(&self->backend_mgr);

	// close db
	db_close(&self->db);
}

static void
system_rpc(Rpc* rpc, void* arg)
{
	System* self = arg;
	switch (rpc->id) {
	case RPC_SHOW_USERS:
	{
		Buf** buf  = rpc_arg_ptr(rpc, 0);
		Str*  name = NULL;
		if (rpc->argc == 2)
			name = rpc_arg_ptr(rpc, 1);
		*buf = user_mgr_list(&self->user_mgr, name);
		break;
	}
	case RPC_SHOW_REPLICAS:
	{
		Buf** buf = rpc_arg_ptr(rpc, 0);
		Uuid* id  = NULL;
		if (rpc->argc == 2)
			id = rpc_arg_ptr(rpc, 1);
		*buf = replica_mgr_list(&self->repl.replica_mgr, id);
		break;
	}
	case RPC_SHOW_REPL:
	{
		Buf** buf = rpc_arg_ptr(rpc, 0);
		*buf = repl_status(&self->repl);
		break;
	}
	case RPC_SHOW_METRICS:
	{
		Buf** buf = rpc_arg_ptr(rpc, 0);
		*buf = system_metrics(self);
		break;
	}
	default:
		break;
	}
}

static void
system_lock(System* self, Rpc* rpc)
{
	if (self->lock)
	{
		rpc_queue_add(&self->lock_queue, rpc);
	} else
	{
		// request exclusive lock for each frontend worker
		frontend_mgr_lock(&self->frontend_mgr);

		// sync to make last operation completed on backends
		//
		// even with exclusive lock, there is a chance that
		// last abort did not not finished yet
		Build build;
		build_init(&build, BUILD_NONE, &self->backend_mgr, NULL, NULL, NULL, NULL);
		defer(build_free, &build);
		build_run(&build);

		rpc_done(rpc);
		self->lock = true;
	}
}

static void
system_unlock(System* self, Rpc* rpc)
{
	assert(self->lock);
	frontend_mgr_unlock(&self->frontend_mgr);

	auto pending = rpc_queue_pop(&self->lock_queue);
	if (pending)
	{
		frontend_mgr_lock(&self->frontend_mgr);
		rpc_done(pending);
	} else {
		self->lock = false;
	}
	rpc_done(rpc);
}

void
system_main(System* self)
{
	for (;;)
	{
		auto buf = channel_read(&am_task->channel, -1);
		auto msg = msg_of(buf);
		defer_buf(buf);

		if (msg->id == RPC_STOP)
			break;
		auto rpc = rpc_of(buf);
		switch (msg->id) {
		case RPC_LOCK:
			system_lock(self, rpc);
			break;
		case RPC_UNLOCK:
			system_unlock(self, rpc);
			break;
		case RPC_CHECKPOINT:
			checkpointer_request(&self->db.checkpointer, rpc);
			break;
		default:
			rpc_execute(rpc, system_rpc, self);
			break;
		}
	}
}
