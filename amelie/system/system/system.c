
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
#include <amelie_heap>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_compiler>
#include <amelie_compute.h>
#include <amelie_session.h>
#include <amelie_system.h>

static void
catalog_if_udf_compile(Catalog* catalog, Udf* udf)
{
	System* system = catalog->iface_arg;

	auto user = catalog_find_user(&system->db.users, catalog->user, true);
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
	compiler_set(&compiler, user, &local, program);

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
catalog_if_udf_depends(Udf* udf, Str* name)
{
	Program* program = udf->data;
	assert(program);
	return access_find(&program->access, name);
}

static void
catalog_if_attach(Catalog* users, User* user)
{
	System* system = users->iface_arg;
	computes_deploy(&system->computes, user);
}

static void
catalog_if_detach(Catalog* users, User* user)
{
	System* system = users->iface_arg;
	computes_undeploy(&system->computes, user);
}

static CatalogIf catalog_if =
{
	.udf_compile = catalog_if_udf_compile,
	.udf_free    = catalog_if_udf_free,
	.udf_depends = catalog_if_udf_depends,
	.attach      = catalog_if_attach,
	.detach      = catalog_if_detach
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
	{
		repl_connect(&self->repl, client);
		return;
	}

	// forward to next compute, client will be migrated if the
	// user does not match
	computes_connect(&self->computes, client);
}

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

System*
system_create(void)
{
	System* self = am_malloc(sizeof(System));

	// set runtime control
	auto control = &self->runtime_if;
	control->state_write     = system_ctl_state_write;
	control->state_read      = system_ctl_state_read;
	control->arg             = self;
	runtime()->iface         = control;

	// prepare shared context
	auto share = &self->share;
	share->commit         = &self->commit;
	share->repl           = &self->repl;
	share->functions      = &self->functions;
	share->db             = &self->db;

	// share shared context globally
	am_share = share;

	// server
	servers_init(&self->servers, system_on_server_connect, self);

	// compute
	computes_init(&self->computes);

	// transactions
	commit_init(&self->commit, &self->db);

	// vm
	functions_init(&self->functions);

	// db
	db_init(&self->db, &catalog_if, &eval_if, self);

	// replication
	repl_init(&self->repl, &self->db);
	return self;
}

void
system_free(System* self)
{
	repl_free(&self->repl);
	commit_free(&self->commit);
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
	recover_init(&recover, &self->db, false);
	defer(recover_free, &recover);
	recover_wal(&recover);
	info("");

	opt_int_set(&state()->recover, RECOVER_NONE);
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
