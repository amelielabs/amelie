
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
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_compiler>
#include <amelie_frontend.h>
#include <amelie_backend.h>
#include <amelie_session.h>
#include <amelie_system.h>

static void
catalog_if_user_invalidate(Catalog* catalog, User* user)
{
	// note: exclusive catalog lock must be held
	System* self = catalog->iface_arg;
	frontends_invalidate(&self->frontends, user);
}

static void
catalog_if_udf_compile(Catalog* catalog, Udf* udf)
{
	unused(catalog);
	Local local;
	local_init(&local);
	local.user = udf->config->user;
	local.time_us = time_us();

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
	explain(&compiler, &udf->config->user, &udf->config->name);

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
catalog_if_udf_depends(Udf* udf, Str* user, Str* name)
{
	Program* program = udf->data;
	assert(program);
	if (name)
		return access_find(&program->access, user, name);
	return access_find_user(&program->access, user);
}

static CatalogIf catalog_if =
{
	.user_invalidate = catalog_if_user_invalidate,
	.udf_compile     = catalog_if_udf_compile,
	.udf_free        = catalog_if_udf_free,
	.udf_depends     = catalog_if_udf_depends
};

static void*
frontend_if_session_create(Frontend* fe, void* arg)
{
	unused(fe);
	unused(arg);
	return session_create();
}

static void
frontend_if_session_free(void* ptr)
{
	return session_free(ptr);
}

static bool
frontend_if_session_execute(void* ptr, Request* req, Query* query)
{
	return session_execute(ptr, req, query);
}

static void
frontend_if_session_execute_msg(void* ptr, Node* node, NodeMsg* msg, Buf* data)
{
	//session_execute_msg(ptr, node, msg, data);
	(void)ptr;
	(void)node;
	(void)msg;
	(void)data;
}

static FrontendIf frontend_if =
{
	.session_create      = frontend_if_session_create,
	.session_free        = frontend_if_session_free,
	.session_execute     = frontend_if_session_execute,
	.session_execute_msg = frontend_if_session_execute_msg
};

static void
system_on_server_connect(Server* server, Client* client)
{
	System* self = server->on_connect_arg;
	frontends_forward(&self->frontends, &client->msg);
}

static void
parts_if_attach(Parts* self)
{
	System* system = self->iface_arg;
	if (self->list_count > PART_MAPPING_MAX)
		error("exceeded the maximum number of hash partitions per table");

	// create pods on backends
	backends_deploy_all(&system->backends, self);
}

static void
parts_if_detach(Parts* self)
{
	// drop pods on backends
	System* system = self->iface_arg;
	backends_undeploy_all(&system->backends, self);
}

static PartsIf parts_if =
{
	.attach = parts_if_attach,
	.detach = parts_if_detach
};

static void
recover_if_create(Recover* recover)
{
	System* self = recover->iface_arg;
	auto players = players_allocate();
	players_start(players, &self->frontends);
	recover->state = players;
}

static void
recover_if_free(Recover* recover)
{
	if (! recover->state)
		return;
	players_stop(recover->state);
	players_free(recover->state);
	recover->state = NULL;
}

static void
recover_if_execute(Recover* recover, RecordMsg* record)
{
	players_execute(recover->state, record);
}

static void
recover_if_sync(Recover* recover)
{
	players_sync(recover->state);
}

static RecoverIf recover_if =
{
	.create  = recover_if_create,
	.free    = recover_if_free,
	.execute = recover_if_execute,
	.sync    = recover_if_sync
};

static void
system_save_state(void* arg)
{
	unused(arg);
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/state.json", state_directory());
	state_save(state(), path);
}

static void
system_invalidate_auth(void* arg)
{
	// note: exclusive catalog lock must be held
	System* self = arg;
	frontends_invalidate_all(&self->frontends);
}

System*
system_create(void)
{
	System* self = am_malloc(sizeof(System));

	// set runtime control
	auto control = &self->runtime_if;
	control->save_state      = system_save_state;
	control->invalidate_auth = system_invalidate_auth;
	control->arg             = self;
	runtime()->iface         = control;

	// prepare shared context
	auto share = &self->share;
	share->gtrs           = &self->gtrs;
	share->commit         = &self->commit;
	share->cdc            = &self->cdc;
	share->repl           = &self->repl;
	share->functions      = &self->functions;
	share->db             = &self->db;
	share->recover_if     = &recover_if;
	share->recover_if_arg = self;

	// share shared context globally
	am_share = share;

	// server
	servers_init(&self->servers, system_on_server_connect, self);

	// cdc
	cdc_init(&self->cdc);

	// frontends/backends
	frontends_init(&self->frontends);
	backends_init(&self->backends);

	// transactions
	gtrs_init(&self->gtrs);
	commit_init(&self->commit, &self->db, &self->gtrs);

	// vm
	functions_init(&self->functions);

	// db
	db_init(&self->db, &catalog_if, self, &parts_if, self, &self->cdc);

	// replication
	repl_init(&self->repl, &self->db, &recover_if, self);
	return self;
}

void
system_free(System* self)
{
	repl_free(&self->repl);
	commit_free(&self->commit);
	gtrs_free(&self->gtrs);
	db_free(&self->db);
	cdc_free(&self->cdc);
	functions_free(&self->functions);
	servers_free(&self->servers);
	am_free(self);
}

static void
system_recover(System* self, bool bootstrap)
{
	info("");

	// set system recover state, this will lead to the partition heap
	// files loaded during pod deployment
	opt_int_set(&state()->recover, RECOVER_CHECKPOINT);

	// restore catalog
	db_open(&self->db, bootstrap);

	opt_int_set(&state()->recover, RECOVER_WAL);

	// replay wals
	Recover recover;
	recover_init(&recover, &self->db, &recover_if, self);
	defer(recover_free, &recover);
	recover_wal(&recover);
	info("");

	opt_int_set(&state()->recover, RECOVER_OFF);
}

void
system_start(System* self, bool bootstrap)
{
	// hello
	auto version = &state()->version.string;
	info("amelie {str}", version);

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
		frontends_start(&self->frontends, &frontend_if,
		                self,
		                workers);
		return;
	}

	// configure server early
	servers_open(&self->servers);

	// register builtin functions
	fn_register(&self->functions);

	// start frontends
	auto workers = opt_int_of(&config()->frontends);
	frontends_start(&self->frontends, &frontend_if,
	                self,
	                workers);

	// start backend workers
	workers = opt_int_of(&config()->backends);
	backends_start(&self->backends, workers);

	// start commit worker
	commit_start(&self->commit);

	// do parallel recover of catalog and wal
	system_recover(self, bootstrap);

	// start periodic wal syncer
	syncer_start(&self->db.syncer);

	// prepare replication manager
	repl_open(&self->repl);

	// start servers
	servers_start(&self->servers);

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

	// stop servers
	servers_stop(&self->servers);

	// stop replication
	repl_stop(&self->repl);

	// stop frontends
	frontends_stop(&self->frontends);

	// stop commit worker
	commit_stop(&self->commit);

	// stop db
	db_close(&self->db);

	// stop backends
	backends_stop(&self->backends);
}

static void
system_rpc(Rpc* rpc, void* arg)
{
	System* self = arg;
	switch (rpc->msg.id) {
	case MSG_SHOW_METRICS:
	{
		Buf** buf = rpc->arg;
		system_metrics(self, *buf);
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
			frontends_forward(&self->frontends, msg);
			continue;
		}
		// rpc
		auto rpc = rpc_of(msg);
		rpc_execute(rpc, system_rpc, self);
	}
}
