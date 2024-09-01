
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
#include <amelie_func.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_cluster.h>
#include <amelie_frontend.h>
#include <amelie_session.h>
#include <amelie_system.h>

static void
system_save_config(void* arg)
{
	unused(arg);
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/config.json", config_directory());
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

	// cluster
	frontend_mgr_init(&self->frontend_mgr);
	cluster_init(&self->cluster, &self->db, &self->function_mgr);
	executor_init(&self->executor, &self->db, &self->cluster.router);
	rpc_queue_init(&self->lock_queue);

	// db
	db_init(&self->db, (PartMapper)cluster_map, &self->cluster);

	// replication
	repl_init(&self->repl, &self->db);

	// vm
	function_mgr_init(&self->function_mgr);

	// prepare shared context (shared between frontends)
	auto share = &self->share;
	share->executor     = &self->executor;
	share->repl         = &self->repl;
	share->cluster      = &self->cluster;
	share->frontend_mgr = &self->frontend_mgr;
	share->function_mgr = &self->function_mgr;
	share->user_mgr     = &self->user_mgr;
	share->db           = &self->db;
	return self;
}

void
system_free(System* self)
{
	repl_free(&self->repl);
	cluster_free(&self->cluster);
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
	auto buf = msg_begin(MSG_CLIENT);
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
	guard(session_free, session);
	session_main(session);
}

static void
system_recover(System* self)
{
	// ask each node to recover last checkpoint partitions in parallel
	info("recover: checkpoint %" PRIu64, config_checkpoint());

	Build build;
	build_init(&build, BUILD_RECOVER, &self->cluster, NULL, NULL, NULL, NULL);
	guard(build_free, &build);
	build_run(&build);

	// replay wals
	Recover recover;
	recover_init(&recover, &self->db, &build_if, &self->cluster);
	guard(recover_free, &recover);
	recover_wal(&recover);
}

void
system_start(System* self, bool bootstrap)
{
	// hello
	auto tz = &config()->timezone_default.string;
	info("");
	info("amelie.");
	info("");
	info("time: default timezone is '%.*s'", str_size(tz), str_of(tz));
	info("");

	// register builtin functions
	fn_register(&self->function_mgr);

	// open user manager
	user_mgr_open(&self->user_mgr);

	// prepare cluster
	cluster_open(&self->cluster, bootstrap);

	info("cluster: system has %d compute nodes", self->cluster.list_count);
	info("");

	// create system object and objects from last snapshot
	db_open(&self->db);

	// start backend
	cluster_start(&self->cluster);

	// do parallel recover of snapshots and wal
	system_recover(self);

	// start checkpointer service
	checkpointer_start(&self->db.checkpointer);

	// start frontends
	auto workers = var_int_of(&config()->frontends);
	frontend_mgr_start(&self->frontend_mgr,
	                   system_on_frontend_connect,
	                   self,
	                   workers);

	// synchronize caches
	frontend_mgr_sync_users(&self->frontend_mgr, &self->user_mgr.cache);

	// prepare replication manager
	repl_open(&self->repl);

	info("");
	vars_print(&config()->vars);
	info("");

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

	// stop frontends
	frontend_mgr_stop(&self->frontend_mgr);

	// stop cluster
	cluster_stop(&self->cluster);

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
		Buf** buf = rpc_arg_ptr(rpc, 0);
		*buf = user_mgr_list(&self->user_mgr);
		break;
	}
	case RPC_SHOW_REPLICAS:
	{
		Buf** buf = rpc_arg_ptr(rpc, 0);
		*buf = replica_mgr_list(&self->repl.replica_mgr);
		break;
	}
	case RPC_SHOW_REPL:
	{
		Buf** buf = rpc_arg_ptr(rpc, 0);
		*buf = repl_show(&self->repl);
		break;
	}
	case RPC_SHOW_NODES:
	{
		Buf** buf = rpc_arg_ptr(rpc, 0);
		*buf = cluster_list(&self->cluster);
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

		// sync to make last operation copleted on nodes
		//
		// even with exclusive lock, there is a chance that
		// last abort did not not finished yet on nodes
		cluster_sync(&self->cluster);

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
		guard(buf_free, buf);
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
