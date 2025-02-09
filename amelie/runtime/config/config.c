
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
	auto default_fe = cpus / 2;
	auto default_be = cpus - default_fe;
	if (default_fe == 0)
		default_fe = 1;

	VarDef defs[] =
	{
		{ "uuid",                    VAR_STRING, VAR_C,                   &self->uuid,                    NULL,          0                   },
		{ "timezone",                VAR_STRING, VAR_C,                   &self->timezone,                NULL,          0                   },
		{ "format",                  VAR_STRING, VAR_C,                   &self->format,                  "json-pretty", 0                   },
		{ "shutdown",                VAR_STRING, VAR_C,                   &self->shutdown,                "fast",        0                   },
		// log
		{ "log_enable",              VAR_BOOL,   VAR_C,                   &self->log_enable,              NULL,          true                },
		{ "log_to_file",             VAR_BOOL,   VAR_C,                   &self->log_to_file,             NULL,          true                },
		{ "log_to_stdout",           VAR_BOOL,   VAR_C,                   &self->log_to_stdout,           NULL,          true                },
		{ "log_connections",         VAR_BOOL,   VAR_C,                   &self->log_connections,         NULL,          true                },
		{ "log_options",             VAR_BOOL,   VAR_C,                   &self->log_options,             NULL,          false               },
		// server
		{ "tls_capath",              VAR_STRING, VAR_C,                   &self->tls_capath,              NULL,          0                   },
		{ "tls_ca",                  VAR_STRING, VAR_C,                   &self->tls_ca,                  NULL,          0                   },
		{ "tls_cert",                VAR_STRING, VAR_C,                   &self->tls_cert,                NULL,          0                   },
		{ "tls_key",                 VAR_STRING, VAR_C,                   &self->tls_key,                 NULL,          0                   },
		{ "listen",                  VAR_JSON,   VAR_C,                   &self->listen,                  NULL,          0                   },
		// limits
		{ "limit_send",              VAR_INT,    VAR_C,                   &self->limit_send,              NULL,          3 * 1024 * 1024     },
		{ "limit_recv",              VAR_INT,    VAR_C,                   &self->limit_recv,              NULL,          1 * 1024 * 1024     },
		{ "limit_write",             VAR_INT,    VAR_C,                   &self->limit_write,             NULL,          0                   },
		// io and compute
		{ "frontends",               VAR_INT,    VAR_C|VAR_Z,             &self->frontends,               NULL,          default_fe          },
		{ "backends",                VAR_INT,    VAR_C|VAR_H|VAR_E,       &self->backends,                NULL,          default_be          },
		// wal
		{ "wal_size",                VAR_INT,    VAR_C,                   &self->wal_size,                NULL,          67108864            },
		{ "wal_sync_on_rotate",      VAR_BOOL,   VAR_C,                   &self->wal_sync_on_rotate,      NULL,          false               },
		{ "wal_sync_on_write",       VAR_BOOL,   VAR_C,                   &self->wal_sync_on_write,       NULL,          false               },
		// replication
		{ "repl_reconnect_ms",       VAR_INT,    VAR_C,                   &self->repl_reconnect_ms,       NULL,          3000                },
		// checkpoint
		{ "checkpoint_interval",     VAR_STRING, VAR_C,                   &self->checkpoint_interval,     "5 min",       0                   },
		{ "checkpoint_workers",      VAR_INT,    VAR_C,                   &self->checkpoint_workers,      NULL,          3                   },
		{  NULL,                     0,          0,                       NULL,                           NULL,          0                   },
	};
	vars_define(&self->vars, defs);
}

void
config_save(Config* self, const char* path)
{
	// get a list of variables
	auto buf = vars_list_persistent(&self->vars);
	defer_buf(buf);

	// convert to json
	Buf text;
	buf_init(&text);
	defer_buf(&text);
	uint8_t* pos = buf->start;
	json_export_pretty(&text, NULL, &pos);

	// create config file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0644);
	file_write_buf(&file, &text);
}

void
config_open(Config* self, const char* path)
{
	auto buf = file_import("%s", path);
	defer_buf(buf);
	Str options;
	str_init(&options);
	buf_str(buf, &options);
	vars_set(&self->vars, &options);
}
