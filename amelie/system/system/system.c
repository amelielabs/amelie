
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
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
catalog_if_index(Catalog* self, Table* table, IndexConfig* index)
{
	// do parallel indexation per worker
	System* system = self->iface_arg;
	BackendMgr* backend_mgr = &system->backend_mgr;
	Build build;
	build_init(&build, BUILD_INDEX, backend_mgr, table, NULL, NULL, index);
	defer(build_free, &build);
	build_run(&build);
}

static void
catalog_if_column_add(Catalog* self, Table* table, Table* table_new, Column* column)
{
	// rebuild new table with new column in parallel per worker
	System* system = self->iface_arg;
	BackendMgr* backend_mgr = &system->backend_mgr;
	Build build;
	build_init(&build, BUILD_COLUMN_ADD, backend_mgr, table, table_new, column, NULL);
	defer(build_free, &build);
	build_run(&build);
}

static void
catalog_if_column_drop(Catalog* self, Table* table, Table* table_new, Column* column)
{
	// rebuild new table without column in parallel per worker
	System* system = self->iface_arg;
	BackendMgr* backend_mgr = &system->backend_mgr;
	Build build;
	build_init(&build, BUILD_COLUMN_DROP, backend_mgr, table, table_new, column, NULL);
	defer(build_free, &build);
	build_run(&build);
}

static void
catalog_if_udf_compile(Catalog* self, Udf* udf)
{
	unused(self);

	Local local;
	local_init(&local);
	local_update_time(&local);

	auto program = program_allocate();
	errdefer(program_free, program);

	SetCache set_cache;
	set_cache_init(&set_cache);
	defer(set_cache_free, &set_cache);

	Compiler compiler;
	compiler_init(&compiler, &local, &set_cache);
	defer(compiler_free, &compiler);
	compiler_set(&compiler, program);

	// parse SQL statements
	compiler_parse_udf(&compiler, udf);

	// generate bytecode
	compiler_emit(&compiler);

	// assign udf program
	udf->data = program;
}

static void
catalog_if_udf_free(Udf* udf)
{
	Program* program = udf->data;
	if (! program)
		return;

	SetCache set_cache;
	set_cache_init(&set_cache);
	defer(set_cache_free, &set_cache);
	program_reset(program, &set_cache);
	program_free(program);

	udf->data = NULL;
}

static bool
catalog_if_udf_depends(Udf* udf, Str* schema, Str* name)
{
	Program* program = udf->data;
	assert(program);
	if (! name) {
		if (access_find_schema(&program->access, schema))
			return true;
	}
	if (access_find(&program->access, schema, name))
		return true;
	return false;
}

static CatalogIf catalog_if =
{
	.build_index       = catalog_if_index,
	.build_column_add  = catalog_if_column_add,
	.build_column_drop = catalog_if_column_drop,
	.udf_compile       = catalog_if_udf_compile,
	.udf_free          = catalog_if_udf_free,
	.udf_depends       = catalog_if_udf_depends
};

static void*
frontend_if_session_create(Frontend* self, void* arg)
{
	unused(arg);
	return session_create(self);
}

static void
frontend_if_session_free(void* ptr)
{
	return session_free(ptr);
}

static bool
frontend_if_session_execute(void*    ptr,
                            Str*     url,
                            Str*     text,
                            Str*     content_type,
                            Content* output)
{
	return session_execute(ptr, url, text, content_type, output);
}

static void
frontend_if_session_execute_replay(void* ptr, Primary* primary, Buf* data)
{
	session_execute_replay(ptr, primary, data);
}

static FrontendIf frontend_if =
{
	.session_create         = frontend_if_session_create,
	.session_free           = frontend_if_session_free,
	.session_execute        = frontend_if_session_execute,
	.session_execute_replay = frontend_if_session_execute_replay
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

	// set runtime control
	auto control = &self->runtime_if;
	control->save_state = system_save_state;
	control->arg        = self;
	runtime()->iface    = control;

	// prepare shared context
	auto share = &self->share;
	share->executor     = &self->executor;
	share->commit       = &self->commit;
	share->core_mgr     = &self->backend_mgr.core_mgr;
	share->repl         = &self->repl;
	share->function_mgr = &self->function_mgr;
	share->user_mgr     = &self->user_mgr;
	share->db           = &self->db;

	// share shared context globally
	am_share = share;

	// server
	user_mgr_init(&self->user_mgr);
	server_init(&self->server);

	// frontend/backend mgr
	frontend_mgr_init(&self->frontend_mgr);
	backend_mgr_init(&self->backend_mgr);
	executor_init(&self->executor);
	commit_init(&self->commit, &self->db, &self->backend_mgr.core_mgr,
	            &self->executor);
	rpc_queue_init(&self->lock_queue);

	// vm
	function_mgr_init(&self->function_mgr);

	// db
	db_init(&self->db, &catalog_if, self, system_attach, self);

	// replication
	repl_init(&self->repl, &self->db);
	return self;
}

void
system_free(System* self)
{
	repl_free(&self->repl);
	commit_free(&self->commit);
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
	System* self = server->on_connect_arg;
	frontend_mgr_forward(&self->frontend_mgr, &client->msg);
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
	recover_init(&recover, &self->db, false);
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

	// start system in the client mode without repository
	if (str_empty(opt_string_of(&state()->directory)))
	{
		info("client-only mode.");

		// start frontends to handle relay clients
		int workers = opt_int_of(&config()->frontends);
		frontend_mgr_start(&self->frontend_mgr,
		                   &frontend_if,
		                   self,
		                   workers);
		return;
	}

	// register builtin functions
	fn_register(&self->function_mgr);

	// open user manager
	user_mgr_open(&self->user_mgr);

	// start commit worker
	commit_start(&self->commit);

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
	                   &frontend_if,
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

	// stop commit worker
	commit_stop(&self->commit);

	// stop backends
	backend_mgr_stop(&self->backend_mgr);

	// close db
	db_close(&self->db);
}

static void
system_rpc(Rpc* rpc, void* arg)
{
	System* self = arg;
	switch (rpc->msg.id) {
	case MSG_SHOW_METRICS:
	{
		Buf** buf = rpc_arg_ptr(rpc, 0);
		*buf = system_metrics(self);
		break;
	}
	case MSG_SYNC_USERS:
	{
		UserCache* cache = rpc_arg_ptr(rpc, 0);
		frontend_mgr_sync_users(&self->frontend_mgr, cache);
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
		auto msg = task_recv();
		if (msg->id == MSG_STOP)
			break;
		if (msg->id == MSG_NATIVE)
		{
			// amelie_connect()
			frontend_mgr_forward(&self->frontend_mgr, msg);
			continue;
		}
		// rpc
		auto rpc = rpc_of(msg);
		switch (msg->id) {
		case MSG_LOCK:
			system_lock(self, rpc);
			break;
		case MSG_UNLOCK:
			system_unlock(self, rpc);
			break;
		case MSG_CHECKPOINT:
			checkpointer_request(&self->db.checkpointer, rpc);
			break;
		default:
			rpc_execute(rpc, system_rpc, self);
			break;
		}
	}
}
