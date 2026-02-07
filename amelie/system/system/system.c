
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_compiler>
#include <amelie_frontend.h>
#include <amelie_backend.h>
#include <amelie_session.h>
#include <amelie_system.h>

static bool
catalog_if_index_create(Catalog* self, Tr* tr, uint8_t* op, bool if_not_exists)
{
	System* system = self->iface_arg;
	return db_create_index(&system->db, tr, op, if_not_exists);
}

static void
catalog_if_udf_compile(Catalog* self, Udf* udf)
{
	unused(self);

	Local local;
	local_init(&local);
	local.db = udf->config->db;
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

	// generate explain for udf
	explain(&compiler, &udf->config->name);

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
catalog_if_udf_depends(Udf* udf, Str* name)
{
	Program* program = udf->data;
	assert(program);
	if (access_find(&program->access, &udf->config->db, name))
		return true;
	return false;
}

static CatalogIf catalog_if =
{
	.index_create = catalog_if_index_create,
	.udf_compile  = catalog_if_udf_compile,
	.udf_free     = catalog_if_udf_free,
	.udf_depends  = catalog_if_udf_depends
};

static void*
frontend_if_session_create(Frontend* self, void* arg)
{
	unused(self);
	unused(arg);
	return session_create();
}

static void
frontend_if_session_free(void* ptr)
{
	return session_free(ptr);
}

static bool
frontend_if_session_execute(void*     ptr,
                            Endpoint* endpoint,
                            Str*      content,
                            Output*   output)
{
	return session_execute(ptr, endpoint, content, output);
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
system_on_server_connect(Server* server, Client* client)
{
	System* self = server->on_connect_arg;
	frontend_mgr_forward(&self->frontend_mgr, &client->msg);
}

static void
engine_if_attach(Engine* self, Level* level)
{
	System* system = self->iface_arg;
	if (level->list_count > PART_MAPPING_MAX)
		error("exceeded the maximum number of hash partitions per table");

	// create pods on backends
	backend_mgr_deploy_all(&system->backend_mgr, level);
}

static void
engine_if_detach(Engine* self, Level* level)
{
	// drop pods on backends
	System* system = self->iface_arg;
	backend_mgr_undeploy_all(&system->backend_mgr, level);
}

static EngineIf engine_if =
{
	.attach = engine_if_attach,
	.detach = engine_if_detach
};

static void
system_save_state(void* arg)
{
	unused(arg);
	char path[PATH_MAX];
	sfmt(path, sizeof(path), "%s/state.json", state_directory());
	state_save(state(), path);
}

System*
system_create(void)
{
	System* self = am_malloc(sizeof(System));

	// set runtime control
	auto control = &self->runtime_if;
	control->save_state = system_save_state;
	control->arg        = self;
	runtime()->iface    = control;

	// prepare shared context
	auto share = &self->share;
	share->executor     = &self->executor;
	share->commit       = &self->commit;
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
	commit_init(&self->commit, &self->db, &self->executor);

	// vm
	function_mgr_init(&self->function_mgr);

	// db
	db_init(&self->db, &catalog_if, self, &engine_if, self);

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
system_recover(System* self, bool bootstrap)
{
	info("");

	// restore catalog
	db_open(&self->db, bootstrap);

	// replay wals
	Recover recover;
	recover_init(&recover, &self->db, false);
	defer(recover_free, &recover);
	recover_wal(&recover);
	info("");
}

void
system_start(System* self, bool bootstrap)
{
	// hello
	auto version = &state()->version.string;
	info("amelie %.*s", str_size(version), str_of(version));

	// show system options
	if (bootstrap || opt_int_of(&config()->log_options))
	{
		info("");
		opts_print(&config()->opts);
	}

	// start system in the client mode without repository
	if (str_empty(opt_string_of(&state()->directory)))
	{
		info("client-only mode.");

		// start frontends to handle relay clients
		auto workers = opt_int_of(&config()->frontends);
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

	// start backend workers
	auto workers = opt_int_of(&config()->backends);
	backend_mgr_start(&self->backend_mgr, workers);

	// start commit worker
	commit_start(&self->commit);

	// do parallel recover of catalog and wal
	system_recover(self, bootstrap);

	// start checkpointer service
	// checkpointer_start(&self->storage.checkpointer);

	// start periodic async wal fsync
	wal_periodic_start(&self->db.wal_mgr.wal_periodic);

	// start frontends
	workers = opt_int_of(&config()->frontends);
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

	// stop db
	db_close(&self->db);

	// stop backends
	backend_mgr_stop(&self->backend_mgr);
}

static void
system_rpc(Rpc* rpc, void* arg)
{
	System* self = arg;
	switch (rpc->msg.id) {
	case MSG_SHOW_METRICS:
	{
		Buf** buf = rpc->arg;
		*buf = system_metrics(self);
		break;
	}
	case MSG_SYNC_USERS:
	{
		UserCache* cache = rpc->arg;
		frontend_mgr_sync_users(&self->frontend_mgr, cache);
		break;
	}
	default:
		break;
	}
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
		rpc_execute(rpc, system_rpc, self);
	}
}
