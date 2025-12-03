
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

#include <amelie_core.h>
#include <amelie.h>
#include <amelie_main.h>
#include <amelie_main_slt.h>

static int
slt_sh(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char cmd[PATH_MAX];
	vsnprintf(cmd, sizeof(cmd), fmt, args);
	int rc = system(cmd);
	va_end(args);
	return rc;
}

void
slt_init(Slt* self)
{
	self->dir       = NULL;
	self->threshold = 8;
	self->session   = NULL;
	self->env       = NULL;
	slt_parser_init(&self->parser);
	slt_result_init(&self->result);
	slt_hash_init(&self->hash);
}

static void
slt_open(Slt* self, Str* dir)
{
	self->dir = dir;
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%.*s", str_size(dir),
	         str_of(dir));

	// recreate result directory
	if (fs_exists("%s", path))
		slt_sh("rm -rf %s", path);

	// create repository
	int   argc   = 12;
	char* argv[] =
	{
		"--log_enable=true",
		"--log_to_stdout=false",
		"--log_options=true",
		"--timezone=UTC",
		"--wal_worker=false",
		"--wal_sync_on_create=false",
		"--wal_sync_on_close=false",
		"--wal_sync_on_write=false",
		"--checkpoint_sync=false",
		"--frontends=1",
		"--backends=1",
		"--listen=[]"
	};
	self->env = amelie_init();
	auto rc = amelie_open(self->env, path, argc, argv);
	if (rc == -1)
		error("amelie_open() failed");

	// create session
	self->session = amelie_connect(self->env, "amelie://");
	if (! self->session)
		error("amelie_connect() failed");
}

static void
slt_close(Slt* self)
{
	if (self->session)
	{
		amelie_free(self->session);
		self->session = NULL;
	}
	if (self->env)
	{
		amelie_free(self->env);
		self->env = NULL;
	}
	slt_result_free(&self->result);
	slt_parser_free(&self->parser);
	slt_hash_free(&self->hash);
}

static void
slt_compare(Slt* self, SltCmd* cmd, amelie_arg_t* msg)
{
	// empty result
	Str data;
	str_set(&data, msg->data, msg->data_size);
	if (str_empty(&data))
	{
		if (str_empty(&cmd->result))
			return;
		slt_cmd_error(cmd, "result mismatch (empty result)");
		return;
	}

	// process and compare result
	auto result = &self->result;
	slt_result_reset(result);
	slt_result_create(result, cmd->sort, self->threshold, cmd->columns, &data);

	// compare result
	if (! slt_result_compare(result, &cmd->result))
	{
		slt_cmd_log(cmd);
		info(">>>> (%d values)", result->count);
		info("%.*s", (int)buf_size(&result->result), buf_cstr(&result->result));
		info("result mismatch");
		return;
	}

	// compare previous command result by label
	if (str_empty(&cmd->query_label))
		return;

	if (! slt_hash_add(&self->hash, &cmd->query_label, &result->result_hash))
	{
		slt_cmd_log(cmd);
		info(">>>> (%d values)", result->count);
		info("%.*s", (int)buf_size(&result->result), buf_cstr(&result->result));
		info("hash mismatch with the previous label");
	}
}

static void
slt_execute(Slt* self, SltCmd* cmd)
{
	auto query = buf_create();
	defer_buf(query);
	buf_write_str(query, &cmd->query);
	buf_write(query, "\0", 1);

	// execute query
	auto req = amelie_execute(self->session, buf_cstr(query), 0, NULL, NULL, NULL);
	if (! req)
		error("amelie_execute() failed");
	defer(amelie_free, req);

	// wait for completion
	amelie_arg_t result = {NULL, 0};
	auto status = amelie_wait(req, -1, &result);

	// validate result
	auto is_error = status != 200 &&
	                status != 204;
	switch (cmd->type) {
	case SLT_QUERY:
	{
		if (is_error)
			break;
		slt_compare(self, cmd, &result);
		return;
	}
	case SLT_STATEMENT_OK:
	{
		if (is_error)
			break;
		return;
	}
	case SLT_STATEMENT_ERROR:
	{
		if (! is_error)
			slt_cmd_error(cmd, "command succeeded, but error expected");
		return;
	}
	default:
		abort();
	}

	slt_cmd_log(cmd);
	error("%.*s", (int)result.data_size, result.data);
}

void
slt_start(Slt* self, Str* dir, Str* file)
{
	// create repository and start runtime
	slt_open(self, dir);

	// read file
	auto parser = &self->parser;
	slt_parser_open(parser, file);

	// process commands
	int processed = 0;
	for (;; processed++)
	{
		auto cmd = slt_parser_next(parser);
		if (! cmd)
			break;
		switch (cmd->type) {
		case SLT_QUERY:
		case SLT_STATEMENT_OK:
		case SLT_STATEMENT_ERROR:
			slt_execute(self, cmd);
			break;
		case SLT_HASH_THRESHOLD:
			self->threshold = cmd->value;
			break;	
		case SLT_HALT:
			slt_cmd_log(cmd);
			return;
		default:
			slt_cmd_error_invalid(cmd);
			break;
		}
	}

	info("tests processed: %d", processed);
}

void
slt_stop(Slt* self)
{
	slt_close(self);
	auto dir = self->dir;
	if (!dir)
		return;
	if (fs_exists("%.*s", str_size(dir), str_of(dir)))
		slt_sh("rm -rf %.*s", str_size(dir), str_of(dir));
}
