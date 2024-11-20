
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>

void
config_init(Config* self)
{
	memset(self, 0, sizeof(*self));
	vars_init(&self->vars);
}

void
config_free(Config* self)
{
	vars_free(&self->vars);
}

void
config_prepare(Config* self)
{
	auto cpus = get_nprocs();
	auto default_fe = cpus / 3;
	auto default_be = cpus - default_fe;
	if (default_fe == 0)
		default_fe = 1;

	VarDef defs[] =
	{
		{ "version",                 VAR_STRING, VAR_E,                   &self->version,                 "1.0.0",     0                },
		{ "uuid",                    VAR_STRING, VAR_C,                   &self->uuid,                    NULL,        0                },
		{ "directory",               VAR_STRING, VAR_E,                   &self->directory,               NULL,        0                },
		{ "timezone",                VAR_STRING, VAR_E|VAR_R|VAR_L,       &self->timezone,                NULL,        0                },
		{ "timezone_default",        VAR_STRING, VAR_C,                   &self->timezone_default,        NULL,        0                },
		// log
		{ "log_enable",              VAR_BOOL,   VAR_C,                   &self->log_enable,              NULL,        true             },
		{ "log_to_file",             VAR_BOOL,   VAR_C,                   &self->log_to_file,             NULL,        true             },
		{ "log_to_stdout",           VAR_BOOL,   VAR_C,                   &self->log_to_stdout,           NULL,        true             },
		{ "log_connections",         VAR_BOOL,   VAR_C|VAR_R,             &self->log_connections,         NULL,        true             },
		{ "log_options",             VAR_BOOL,   VAR_C|VAR_R,             &self->log_options,             NULL,        true             },
		// server
		{ "tls_capath",              VAR_STRING, VAR_C,                   &self->tls_capath,              NULL,        0                },
		{ "tls_ca",                  VAR_STRING, VAR_C,                   &self->tls_ca,                  NULL,        0                },
		{ "tls_cert",                VAR_STRING, VAR_C,                   &self->tls_cert,                NULL,        0                },
		{ "tls_key",                 VAR_STRING, VAR_C,                   &self->tls_key,                 NULL,        0                },
		{ "listen",                  VAR_DATA,   VAR_C,                   &self->listen,                  NULL,        0                },
		// limits
		{ "limit_send",              VAR_INT,    VAR_C|VAR_R,             &self->limit_send,              NULL,        3 * 1024 * 1024  },
		{ "limit_recv",              VAR_INT,    VAR_C|VAR_R,             &self->limit_recv,              NULL,        1 * 1024 * 1024  },
		{ "limit_write",             VAR_INT,    VAR_C|VAR_R,             &self->limit_write,             NULL,        0                },
		// cluster
		{ "frontends",               VAR_INT,    VAR_C|VAR_Z,             &self->frontends,               NULL,        default_fe       },
		{ "backends",                VAR_INT,    VAR_C|VAR_E,             &self->backends,                NULL,        default_be       },
		// wal
		{ "wal",                     VAR_BOOL,   VAR_C,                   &self->wal,                     NULL,        true             },
		{ "wal_rotate_wm",           VAR_INT,    VAR_C|VAR_R,             &self->wal_rotate_wm,           NULL,        104857600        },
		{ "wal_sync_on_rotate",      VAR_BOOL,   VAR_C,                   &self->wal_sync_on_rotate,      NULL,        true             },
		{ "wal_sync_on_write",       VAR_BOOL,   VAR_C,                   &self->wal_sync_on_write,       NULL,        false            },
		// replication
		{ "repl",                    VAR_BOOL,   VAR_C,                   &self->repl,                    0,           false            },
		{ "repl_primary",            VAR_STRING, VAR_C,                   &self->repl_primary,            NULL,        0                },
		{ "repl_reconnect_ms",       VAR_INT,    VAR_C|VAR_R,             &self->repl_reconnect_ms,       NULL,        3000             },
		// checkpoint
		{ "checkpoint_interval",     VAR_STRING, VAR_C,                   &self->checkpoint_interval,     "5 min",     0                },
		{ "checkpoint_workers",      VAR_INT,    VAR_C,                   &self->checkpoint_workers,      NULL,        3                },
		{ "checkpoint",              VAR_INT,    VAR_E,                   &self->checkpoint,              NULL,        0                },
		// state
		{ "read_only",               VAR_BOOL,   VAR_E,                   &self->read_only,               NULL,        false            },
		{ "lsn",                     VAR_INT,    VAR_E,                   &self->lsn,                     NULL,        0                },
		{ "psn",                     VAR_INT,    VAR_E,                   &self->psn,                     NULL,        0                },
		// state persistent
		{ "replicas",                VAR_DATA,   VAR_Y|VAR_C|VAR_H|VAR_S, &self->replicas,                NULL,        0                },
		{ "users",                   VAR_DATA,   VAR_Y|VAR_C|VAR_H|VAR_S, &self->users,                   NULL,        0                },
		// stats
		{ "connections",             VAR_INT,    VAR_E|VAR_H,             &self->connections,             NULL,        0                },
		{ "sent_bytes",              VAR_INT,    VAR_E|VAR_H,             &self->sent_bytes,              NULL,        0                },
		{ "recv_bytes",              VAR_INT,    VAR_E|VAR_H,             &self->recv_bytes,              NULL,        0                },
		{ "writes",                  VAR_INT,    VAR_E|VAR_H,             &self->writes,                  NULL,        0                },
		{ "writes_bytes",            VAR_INT,    VAR_E|VAR_H,             &self->writes_bytes,            NULL,        0                },
		{ "ops",                     VAR_INT,    VAR_E|VAR_H,             &self->ops,                     NULL,        0                },
		// testing
		{ "test_bool",               VAR_BOOL,   VAR_E|VAR_H|VAR_R,       &self->test_bool,               NULL,        false            },
		{ "test_int",                VAR_INT,    VAR_E|VAR_H|VAR_R,       &self->test_int,                NULL,        0                },
		{ "test_string",             VAR_STRING, VAR_E|VAR_H|VAR_R,       &self->test_string,             NULL,        0                },
		{ "test_data",               VAR_DATA,   VAR_E|VAR_H|VAR_R,       &self->test_data,               NULL,        0                },
		{  NULL,                     0,          0,                       NULL,                           NULL,        0                },
	};

	vars_define(&self->vars, defs);
}

static void
config_save_to(Config* self, const char* path)
{
	// get a list of variables
	auto buf = vars_list_persistent(&self->vars);
	guard_buf(buf);

	// convert to json
	Buf text;
	buf_init(&text);
	guard_buf(&text);
	uint8_t* pos = buf->start;
	json_export_pretty(&text, NULL, &pos);

	// create config file
	File file;
	file_init(&file);
	guard(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0600);
	file_write_buf(&file, &text);

	// sync
	file_sync(&file);
}

void
config_save(Config* self, const char* path)
{
	// remove old saved config, if exists
	Buf buf;
	buf_init(&buf);
	guard_buf(&buf);
	if (fs_exists("%s.old", path))
		fs_unlink("%s.old", path);

	// save existing config as a old
	fs_rename(path,  "%s.old", path);

	// create config file
	config_save_to(self, path);

	// remove old config file
	fs_unlink("%s.old", path);
}

void
config_open(Config* self, const char* path)
{
	if (fs_exists("%s", path))
	{
		Buf buf;
		buf_init(&buf);
		guard_buf(&buf);
		file_import(&buf, "%s", path);
		Str options;
		str_init(&options);
		buf_str(&buf, &options);
		vars_set(&self->vars, &options, true);
		return;
	}

	// create config file
	config_save_to(self, path);
}
