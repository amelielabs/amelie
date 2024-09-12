
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

static void
system_lock(void* arg, bool shared)
{
	System* self = arg;
	rwlock_lock(&self->lock, shared);
}

static void
system_unlock(void* arg)
{
	System* self = arg;
	rwlock_unlock(&self->lock);
}

static Buf*
system_show(void* arg, int type)
{
	System* self = arg;
	Buf* buf = NULL;
	switch (type) {
	case CONTROL_SHOW_USERS:
		buf = user_mgr_list(&self->user_mgr);
		break;
	case CONTROL_SHOW_REPL:
		buf = repl_show(&self->repl);
		break;
	case CONTROL_SHOW_REPLICAS:
		buf = replica_mgr_list(&self->repl.replica_mgr);
		break;
	case CONTROL_SHOW_NODES:
		buf = cluster_list(&self->cluster);
		break;
	default:
		abort();
		break;
	}
	return buf;
}

System*
system_create(void)
{
	System* self = am_malloc(sizeof(System));

	// set control
	auto control = &self->control;
	control->save_config = system_save_config;
	control->lock        = system_lock;
	control->unlock      = system_unlock;
	control->show        = system_show;
	control->arg         = self;
	global()->control    = control;

	// sessions
	session_mgr_init(&self->session_mgr);
	rwlock_init(&self->lock);

	// server
	user_mgr_init(&self->user_mgr);
	server_init(&self->server);

	// cluster
	cluster_init(&self->cluster, &self->db, &self->function_mgr);
	executor_init(&self->executor, &self->db, &self->cluster.router);

	// db
	db_init(&self->db, (PartMapper)cluster_map, &self->cluster);

	// replication
	repl_init(&self->repl, &self->db);

	// vm
	function_mgr_init(&self->function_mgr);

	// prepare shared context (shared between sessions)
	auto share = &self->share;
	share->executor     = &self->executor;
	share->repl         = &self->repl;
	share->cluster      = &self->cluster;
	share->function_mgr = &self->function_mgr;
	share->session_mgr  = &self->session_mgr;
	share->user_mgr     = &self->user_mgr;
	share->db           = &self->db;
	return self;
}

void
system_free(System* self)
{
	session_mgr_free(&self->session_mgr);
	repl_free(&self->repl);
	cluster_free(&self->cluster);
	executor_free(&self->executor);
	db_free(&self->db);
	function_mgr_free(&self->function_mgr);
	server_free(&self->server);
	user_mgr_free(&self->user_mgr);
	rwlock_free(&self->lock);
	am_free(self);
}

static void
system_on_server_connect(Server* server, Client* client)
{
	System* self = server->on_connect_arg;
	auto session = session_create(client, &self->share);
	guard(session_free, session);
	session_mgr_add(&self->session_mgr, session);
	session_start(session);
	unguard();
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
	// checkpointer_start(&self->db.checkpointer);

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

	// stop sessions
	session_mgr_stop(&self->session_mgr);

	// stop replication
	repl_stop(&self->repl);

	// stop cluster
	cluster_stop(&self->cluster);

	// close db
	db_close(&self->db);
}

void
system_main(System* self)
{
	server_main(&self->server);
}
