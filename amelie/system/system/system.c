
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
#include <amelie_compute.h>
#include <amelie_session.h>
#include <amelie_system.h>

static void
catalog_if_user_invalidate(Catalog* catalog, User* user)
{
	// note: exclusive catalog lock must be held
	System* self = catalog->iface_arg;
	computes_invalidate(&self->computes, user);
}

static void
catalog_if_udf_compile(Catalog* catalog, Udf* udf)
{
	auto user = catalog_find_user(catalog, &udf->config->user, true);
	Local local;
	local_init(&local);
	local.user    = user->config->name;
	local.time_us = time_us();
	local.limits  = &user->config->limits;

	auto program = program_allocate();
	errdefer(program_free, program);

	SetCache set_cache;
	set_cache_init(&set_cache);
	defer(set_cache_free, &set_cache);

	Compiler compiler;
	compiler_init(&compiler, &set_cache);
	defer(compiler_free, &compiler);
	compiler_set(&compiler, &local, program);

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

static void
catalog_if_user_attach(Catalog* catalog, User* user)
{
	// create pod
	System* system = catalog->iface_arg;
	computes_deploy(&system->computes, user);
}

static void
catalog_if_user_detach(Catalog* catalog, User* user)
{
	// drop pods
	System* system = catalog->iface_arg;
	computes_undeploy(&system->computes, user);
}

static void
catalog_if_user_sync(Catalog* catalog, User* user)
{
	// sync prepared transactions
	System* system = catalog->iface_arg;
	computes_sync(&system->computes, user);
}

static void
catalog_if_table_truncate(Catalog* catalog, Table* table)
{
	// truncate pod
	System* system = catalog->iface_arg;
	computes_truncate(&system->computes, table);
}

static CatalogIf catalog_if =
{
	.user_invalidate = catalog_if_user_invalidate,
	.user_attach     = catalog_if_user_attach,
	.user_detach     = catalog_if_user_detach,
	.user_sync       = catalog_if_user_sync,
	.udf_compile     = catalog_if_udf_compile,
	.udf_free        = catalog_if_udf_free,
	.udf_depends     = catalog_if_udf_depends,
	.table_truncate  = catalog_if_table_truncate
};

static void*
compute_if_session_create(Compute* fe, void* arg)
{
	unused(fe);
	unused(arg);
	return session_create();
}

static void
compute_if_session_free(void* ptr)
{
	return session_free(ptr);
}

static bool
compute_if_session_execute(void* ptr, Portal* portal, Request* req)
{
	return session_execute(ptr, portal, req);
}

static ComputeIf compute_if =
{
	.session_create  = compute_if_session_create,
	.session_free    = compute_if_session_free,
	.session_execute = compute_if_session_execute
};

static void
system_on_server_connect(Server* server, Client* client)
{
	System* self = server->on_connect_arg;
	if (client->repl)
		repl_connect(&self->repl, client);
	else
		computes_forward(&self->computes, &client->msg);
}

static void
recover_if_create(Recover* recover)
{
	System* self = recover->iface_arg;
	auto players = players_allocate();
	players_start(players, &self->computes);
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
eval_if_create(Eval* self)
{
	self->state = system_eval_allocate();
	system_eval_create(self->state);
}

static void
eval_if_free(Eval* self)
{
	system_eval_free(self->state);
	self->state = NULL;
}

static void
eval_if_execute(Eval* self, Str* command)
{
	system_eval(self->state, command);
}

static EvalIf eval_if =
{
	.create  = eval_if_create,
	.free    = eval_if_free,
	.execute = eval_if_execute
};

static void
system_ctl_state_write(void* arg)
{
	system_state_write(arg);
}

static void
system_ctl_state_read(void* arg, Buf* buf)
{
	system_state_create(arg, buf);
}

static void
system_ctl_invalidate_auth(void* arg)
{
	// note: exclusive catalog lock must be held
	System* self = arg;
	computes_invalidate_all(&self->computes);
}

System*
system_create(void)
{
	System* self = am_malloc(sizeof(System));

	// set runtime control
	auto control = &self->runtime_if;
	control->state_write     = system_ctl_state_write;
	control->state_read      = system_ctl_state_read;
	control->invalidate_auth = system_ctl_invalidate_auth;
	control->arg             = self;
	runtime()->iface         = control;

	// prepare shared context
	auto share = &self->share;
	share->gtrs           = &self->gtrs;
	share->commit         = &self->commit;
	share->repl           = &self->repl;
	share->functions      = &self->functions;
	share->db             = &self->db;
	share->recover_if     = &recover_if;
	share->recover_if_arg = self;

	// share shared context globally
	am_share = share;

	// server
	servers_init(&self->servers, system_on_server_connect, self);

	// computes
	computes_init(&self->computes);

	// transactions
	gtrs_init(&self->gtrs);
	commit_init(&self->commit, &self->db, &self->gtrs);

	// vm
	functions_init(&self->functions);

	// db
	db_init(&self->db, &catalog_if, &eval_if, self);

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
	auto version = &config()->version.string;
	info("amelie {str}", version);

	// save system state on bootstrap
	if (bootstrap)
		control_state_write();

	if (bootstrap || opt_int_of(&config()->log_options))
	{
		info("");
		opts_print(&config()->opts);
	}

	// configure server early
	servers_open(&self->servers);

	// register builtin functions
	fn_register(&self->functions);

	// start computes
	auto workers = opt_int_of(&config()->compute);
	computes_start(&self->computes, &compute_if,
	                self,
	                workers);

	// start commit worker
	commit_start(&self->commit);

	// do parallel recover of catalog and wal
	system_recover(self, bootstrap);

	// start periodic wal syncer
	syncer_start(&self->db.syncer);

	// configure system and replicas
	system_state_read(self);

	// start servers
	servers_start(&self->servers);

	// start repl
	repl_start(&self->repl);
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

	// stop computes
	computes_stop(&self->computes);

	// stop commit worker
	commit_stop(&self->commit);

	// stop db
	db_close(&self->db);
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
		// rpc
		auto rpc = rpc_of(msg);
		rpc_execute(rpc, system_rpc, self);
	}
}
