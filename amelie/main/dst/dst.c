
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

#include <amelie>
#include <amelie_main.h>
#include <amelie_main_dst.h>

static inline int
dst_sh(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char cmd[PATH_MAX];
	formatv(cmd, sizeof(cmd), fmt, args);
	int rc = system(cmd);
	va_end(args);
	return rc;
}

void
dst_init(Dst* self)
{
	self->step        = 0;
	self->users       = NULL;
	self->users_count = 0;
	runtime_init(&self->runtime);
	opts_init(&self->opts);

	OptsDef defs[] =
	{
		{ "dir",   OPT_STRING, OPT_C,       &self->opt_dir,   "_output", 0     },
		{ "seed",  OPT_INT,    OPT_C,       &self->opt_seed,  NULL,      0     },
		{ "users", OPT_INT,    OPT_C|OPT_Z, &self->opt_users, NULL,      3     },
		{ "steps", OPT_INT,    OPT_C|OPT_Z, &self->opt_steps, NULL,      1000  },
		{ "keys",  OPT_INT,    OPT_C|OPT_Z, &self->opt_keys,  NULL,      1000  },
		{ "sync",  OPT_INT,    OPT_C|OPT_Z, &self->opt_sync,  NULL,      1000  },
		{  NULL,   0,          0,            NULL,            NULL,      0     }
	};
	opts_define(&self->opts, defs);
}

void
dst_free(Dst* self)
{
	if (self->users)
	{
		for (auto i = 0; i < self->users_count; i++)
			dst_user_free(&self->users[i]);
		am_free(self->users);
	}
	opts_free(&self->opts);
	runtime_free(&self->runtime);
}

static void
dst_cleanup(Dst* self)
{
	auto dir = opt_string_of(&self->opt_dir);
	if (fs_exists("{str}", dir))
		dst_sh("rm -rf '{str}'", dir);
}

static void
dst_configure(Dst* self)
{
	// create directory
	auto dir = opt_string_of(&self->opt_dir);
	dst_sh("mkdir {str}", dir);

	// set logger
	auto logger = &runtime()->logger;
	logger_set_enable(logger, true);
	logger_set_stdout(logger, true);
	logger_set_stdout_lf(logger, true);
	char path[PATH_MAX];
	format(path, sizeof(path), "{str}/log", dir);
	logger_open(logger, path);

	// prepare users
	self->users_count = (int)self->opt_users.integer;
	self->users = am_malloc(sizeof(DstUser) * self->users_count);
	for (auto i = 0; i < self->users_count; i++)
		dst_user_init(&self->users[i], self, i);
}

static void
dst_open(Dst* self)
{
	// start instance
	char path[PATH_MAX];
	format(path, sizeof(path), "{str}/env", opt_string_of(&self->opt_dir));

	int   argc = 12;
	char* argv[18] =
	{
		"amelie",
		"start",
		path,
		"--log_enable=true",
		"--log_stdout=false",
		"--log_options=true",
		"--timezone=UTC",
		"--wal_sync_create=false",
		"--wal_sync_close=false",
		"--wal_sync_write=false",
		"--wal_service=false",
		"--storage_sync=false"
	};
	int rc = runtime_start(&self->runtime, main_runtime, NULL, argc, argv);
	if (rc == -1)
		error("failed to open instance");
}

static void
dst_close(Dst* self)
{
	runtime_stop(&self->runtime);
	runtime_free(&self->runtime);
	runtime_init(&self->runtime);
}

void
dst_execute(Dst* self, Client* client, bool must_fail, char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char msg[1024];
	auto msg_size = formatv(msg, sizeof(msg), fmt, args);
	va_end(args);

	Str cmd;
	str_set(&cmd, msg, msg_size);
	//info("[{u64}] {str}", self->step, &cmd);
	auto code = client_execute(client, &cmd, NULL);

	if (must_fail)
	{
		if (code >= 400)
			return;
		error("[{u64}] failure expected");
		return;
	}
	if (code < 400)
		return;

	auto reply = &client->reply;
	if (buf_empty(&reply->content))
		error("[{u64}] {str}", self->step, &reply->options[HTTP_MSG]);
	error("[{u64}] {buf}", self->step, &reply->content);
}

static void
dst_bootstrap(Dst* self)
{
	char path[PATH_MAX];
	format(path, sizeof(path), "{str}/env", opt_string_of(&self->opt_dir));

	// connect (superuser)
	Endpoint endpoint;
	endpoint_init(&endpoint);
	defer(endpoint_free, &endpoint);
	opt_string_set_cstr(&endpoint.path, path);

	auto client = client_allocate();
	defer(client_free, client);
	client_set_endpoint(client, &endpoint);
	client_connect(client);

	// create users and relations
	for (auto i = 0; i < self->users_count; i++)
	{
		auto user = &self->users[i];
		dst_execute(self, client, false, "CREATE USER user_{u64}", i);
		dst_user_connect(user);
		dst_user_create(user);
	}
}

void
dst_run(Dst* self)
{
	// cleanup
	dst_cleanup(self);

	// create repo
	dst_configure(self);
	dst_open(self);

	// hello
	info("amelie dst (deterministic simulation)");
	info("");

	// create users and relations
	dst_bootstrap(self);

	// set seed
	auto random = &am_task->random;	
	random->seed[0] = 123;
	random->seed[1] = 123;

	// main loop
	auto sync  = (int)opt_int_of(&self->opt_sync);
	auto steps = (int)opt_int_of(&self->opt_steps);
	while (self->step < steps)
	{
		auto user_id = random_generate(&am_task->random) % self->users_count;
		auto user = &self->users[user_id];
		dst_step(user);

		// validate
		if (self->step > 0 && self->step % sync == 0)
			dst_validate(self);

		self->step++;
	}

	// validate
	dst_validate(self);

	dst_close(self);

	// cleanup
	dst_cleanup(self);
}
