
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
#include <amelie_cli.h>
#include <amelie_cli_slt.h>
#include <openssl/evp.h>

static inline int
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
	self->line      = 1;
	self->threshold = 8;
	self->session   = NULL;
	self->env       = NULL;
	json_init(&self->json);
}

static void
slt_read(Slt* self, SltCommand* cmd, Str* stream)
{
	// command \n
	// [line] \n | eof
	//  ...
	// [----]
	// [line] \n | eof
	//  ...
	// \n | eof
	//
	slt_command_reset(cmd);

	auto body = &cmd->body;
	str_set(body, stream->pos, 0);

	// command
	cmd->line = self->line;
	if (! str_gets(stream, &cmd->command))
		return;
	self->line++;

	// [query]
	auto query = &cmd->query;
	str_set(query, stream->pos, 0);
	for (;;)
	{
		// eof
		Str line;
		if (! str_gets(stream, &line))
			return;
		self->line++;

		// \n
		if (str_empty(&line))
			return;

		// ----
		if (str_is(&line, "----", 4))
			break;

		// line (including \n)
		auto end = line.end + 1;
		query->end = end;
		body->end = end;
	}

	// [result]
	auto result = &cmd->result;
	str_set(result, stream->pos, 0);
	for (;;)
	{
		// eof
		Str line;
		if (! str_gets(stream, &line))
			return;
		self->line++;

		// \n
		if (str_empty(&line))
			return;

		// line (including \n)
		auto end = line.end + 1;
		result->end = end;
		body->end = end;
	}
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
		"--backends=3",
		"--listen=[]"
	};
	self->env = amelie_init();
	auto rc = amelie_open(self->env, path, argc, argv);
	if (rc == -1)
		error("amelie_open() failed");

	// create session
	self->session = amelie_connect(self->env, NULL);
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
	json_free(&self->json);
}

static void
slt_log(SltCommand* cmd)
{
	info("---- (line %d)", cmd->line);
	info("%.*s", (int)str_size(&cmd->body), str_of(&cmd->body));
}

static void
slt_error(SltCommand* cmd, amelie_arg_t* msg)
{
	slt_log(cmd);
	error("%.*s", (int)msg->data_size, msg->data);
}

static void
slt_export(Buf* buf, uint8_t** pos, int* count)
{
	json_read_array(pos);
	while (! json_read_array_end(pos))
	{
		if (json_is_array(*pos))
		{
			slt_export(buf, pos, count);
			continue;
		}
		if (json_is_bool(*pos))
		{
			buf_printf(buf, "%d\n", **pos == JSON_TRUE);
		} else
		if (json_is_integer(*pos))
		{
			int64_t value;
			json_read_integer(pos, &value);
			buf_printf(buf, "%" PRIi64 "\n", value);
		} else
		if (json_is_real(*pos))
		{
			double value;
			json_read_real(pos, &value);
			buf_printf(buf, "%.3f\n", value);
		} else
		if (json_is_string(*pos))
		{
			// todo: print control chars as @
			Str value;
			json_read_string(pos, &value);
			if (str_empty(&value))
				buf_printf(buf, "(empty)\n");
			else
				buf_printf(buf, "%.*s\n", (int)str_size(&value),
				           str_of(&value));
		} else
		if (json_is_null(*pos))
		{
			buf_printf(buf, "NULL\n");
		} else {
			error("unsupported type in result");
		}
		(*count)++;
	}
}

static inline void
slt_md5(Buf* input, char output[33])
{
	auto ctx = EVP_MD_CTX_new();
	unsigned char digest[EVP_MAX_MD_SIZE];
	unsigned int  digest_len;
	EVP_DigestInit_ex2(ctx, EVP_md5(), NULL);
	EVP_DigestUpdate(ctx, buf_cstr(input), buf_size(input));
	EVP_DigestFinal_ex(ctx, digest, &digest_len);
	EVP_MD_CTX_free(ctx);
    const char* hex = "0123456789abcdef";
	for (unsigned int i = 0, j = 0; i < digest_len; i++)
	{
        output[j++] = hex[(digest[i] >> 4) & 0x0F];
        output[j++] = hex[(digest[i]) & 0x0F];
	}
	output[32] = 0;
}

static void
slt_diff(Slt* self, SltCommand* cmd, amelie_arg_t* msg)
{
	// empty result
	Str content;
	str_set(&content, msg->data, msg->data_size);
	if (str_empty(&content))
	{
		if (str_empty(&cmd->result))
			return;
		slt_log(cmd);
		error("result mismatch (empty result)");
		return;
	}

	// prepare result
	int  result_count = 0;
	auto result = buf_create();
	defer_buf(result);

	// convert json result into text
	auto json = &self->json;
	json_reset(json);
	json_parse(json, &content, NULL);
	auto pos = json->buf->start;
	slt_export(result, &pos, &result_count);

	// convert result into md5 string on the threshold reach
	if (result_count > self->threshold)
	{
		char digest[33];
		slt_md5(result, digest);
		buf_reset(result);
		buf_printf(result, "%d values hashing to %s\n",
		           result_count, digest);
	}

	// compare result
	if (str_is(&cmd->result, buf_cstr(result), buf_size(result)))
		return;

	// error
	slt_log(cmd);

	info(">>>> (%d values)", result_count);
	info("%.*s", (int)buf_size(result), buf_cstr(result));
	error("result mismatch");
}

static void
slt_execute(Slt* self, SltCommand* cmd)
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
		if (is_error)
			slt_error(cmd, &result);
		slt_diff(self, cmd, &result);
		break;
	case SLT_STATEMENT_OK:
		if (is_error)
			slt_error(cmd, &result);
		break;
	case SLT_STATEMENT_ERROR:
		if (! is_error)
		{
			slt_log(cmd);
			error("error expected");
		}
		break;
	default:
		abort();
	}
}

void
slt_start(Slt* self, Str* dir, Str* file)
{
	// create repository and start runtime
	slt_open(self, dir);

	auto buf = file_import("%.*s", (int)str_size(file), str_of(file));
	defer_buf(buf);
	Str stream;
	buf_str(buf, &stream);

	// process commands
	SltCommand cmd;
	slt_command_init(&cmd);

	int processed = 0;
	for (;; processed++)
	{
		// todo: [skipif | onlyif \n]
		slt_read(self, &cmd, &stream);
		if (str_empty(&cmd.command))
			break;

		// find command
		slt_command_match(&cmd);

		// process system commands
		switch (cmd.type) {
		case SLT_HASH_THRESHOLD:
			// todo: set threshold
			break;	
		case SLT_HALT:
			slt_log(&cmd);
			return;
		case SLT_UNDEF:
			slt_log(&cmd);
			error("unknown command");
			break;
		default:
			slt_execute(self, &cmd);
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
