
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
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_host.h>
#include <amelie_compute.h>
#include <amelie_session.h>
#include <amelie_system.h>

static void
system_save_config(void* arg)
{
	unused(arg);
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/amelie.json", config_directory());
	config_save(global()->config, path);
}

System*
system_create(void)
{
	System* self = am_malloc(sizeof(System));
	self->lock = false;

	// set control
	auto control = &self->control;
	control->system      = &am_task->channel;
	control->save_config = system_save_config;
	control->arg         = self;
	global()->control    = control;

	// server
	user_mgr_init(&self->user_mgr);
	server_init(&self->server);

	// compute_mgr
	host_mgr_init(&self->host_mgr);
	compute_mgr_init(&self->compute_mgr, &self->db, &self->function_mgr);
	executor_init(&self->executor, &self->db, &self->compute_mgr.router);
	rpc_queue_init(&self->lock_queue);

	// vm
	function_mgr_init(&self->function_mgr);

	// db
	db_init(&self->db, (PartMapper)compute_mgr_map, &self->compute_mgr,
	        &compute_mgr_if, &self->compute_mgr);

	// replication
	repl_init(&self->repl, &self->db);

	// prepare shared context (shared between hosts)
	auto share = &self->share;
	share->executor     = &self->executor;
	share->repl         = &self->repl;
	share->host_mgr     = &self->host_mgr;
	share->compute_mgr  = &self->compute_mgr;
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
	compute_mgr_free(&self->compute_mgr);
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
	host_mgr_forward(&self->host_mgr, buf);
}

static void
system_on_host_connect(Host* host, Client* client)
{
	System* self = host->on_connect_arg;
	auto session = session_create(client, host, &self->share);
	defer(session_free, session);
	session_main(session);
}

static void
system_recover(System* self)
{
	// ask each node to recover last checkpoint partitions in parallel
	info("recover: begin (checkpoint %" PRIu64 ")", config_checkpoint());

	Build build;
	build_init(&build, BUILD_RECOVER, &self->compute_mgr, NULL, NULL, NULL, NULL);
	defer(build_free, &build);
	build_run(&build);

	// replay wals
	Recover recover;
	recover_init(&recover, &self->db, &build_if, &self->compute_mgr);
	defer(recover_free, &recover);
	recover_wal(&recover);

	info("recover: complete");
}

static void
system_bootstrap(System* self)
{
	// create compute nodes
	compute_bootstrap(&self->db, var_int_of(&config()->nodes));
	config_lsn_set(1);

	// create initial checkpoint, mostly to ensure that node
	// information is persisted (if wal is disabled)
	Checkpoint cp;
	checkpoint_init(&cp, &self->db.checkpoint_mgr);
	defer(checkpoint_free, &cp);
	checkpoint_begin(&cp, 1, 1);
	checkpoint_run(&cp);
	checkpoint_wait(&cp);
}

void
system_start(System* self, bool bootstrap)
{
	// hello
	auto version = &config()->version.string;
	info("");
	info("amelie. (%.*s)", str_size(version), str_of(version));
	info("");

	// register builtin functions
	fn_register(&self->function_mgr);

	// open user manager
	user_mgr_open(&self->user_mgr);

	// create system object and objects from last snapshot (including nodes)
	db_open(&self->db);

	// do parallel recover of snapshots and wal
	system_recover(self);

	// do initial deploy
	if (bootstrap)
		system_bootstrap(self);

	// start checkpointer service
	checkpointer_start(&self->db.checkpointer);

	// start hosts
	auto workers = var_int_of(&config()->hosts);
	host_mgr_start(&self->host_mgr,
	               system_on_host_connect,
	               self,
	               workers);

	// synchronize caches
	host_mgr_sync_users(&self->host_mgr, &self->user_mgr.cache);

	// prepare replication manager
	repl_open(&self->repl);

	// show system options
	info("");
	if (bootstrap || var_int_of(&config()->log_options))
	{
		vars_print(&config()->vars);
		info("");
	}

	// start server
	server_start(&self->server, system_on_server_connect, self);

	// start replication
	if (var_int_of(&config()->repl))
	{
		var_int_set(&config()->repl, false);
		repl_start(&self->repl);
	}
}

void
system_stop(System* self)
{
	// stop server
	server_stop(&self->server);

	// stop replication
	repl_stop(&self->repl);

	// stop hosts
	host_mgr_stop(&self->host_mgr);

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
	case RPC_SHOW_STATUS:
	{
		Buf** buf = rpc_arg_ptr(rpc, 0);
		*buf = system_status(self);
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
		// request exclusive lock for each host worker
		host_mgr_lock(&self->host_mgr);

		// sync to make last operation completed on nodes
		//
		// even with exclusive lock, there is a chance that
		// last abort did not not finished yet on nodes
		compute_mgr_sync(&self->compute_mgr);

		rpc_done(rpc);
		self->lock = true;
	}
}

static void
system_unlock(System* self, Rpc* rpc)
{
	assert(self->lock);
	host_mgr_unlock(&self->host_mgr);

	auto pending = rpc_queue_pop(&self->lock_queue);
	if (pending)
	{
		host_mgr_lock(&self->host_mgr);
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
