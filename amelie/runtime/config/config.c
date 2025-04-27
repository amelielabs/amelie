
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
	opts_init(&self->opts);
}

void
config_free(Config* self)
{
	opts_free(&self->opts);
}

void
config_prepare(Config* self)
{
	auto cpus = get_nprocs();
	auto default_fe = cpus / 2;
	auto default_be = cpus - default_fe;
	if (default_fe == 0)
		default_fe = 1;
	if (default_be == 0)
		default_be = 1;

	OptsDef defs[] =
	{
		{ "uuid",                    OPT_STRING, OPT_C,                   &self->uuid,                    NULL,          0                   },
		{ "timezone",                OPT_STRING, OPT_C,                   &self->timezone,                NULL,          0                   },
		{ "format",                  OPT_STRING, OPT_C,                   &self->format,                  "json-pretty", 0                   },
		// log
		{ "log_enable",              OPT_BOOL,   OPT_C,                   &self->log_enable,              NULL,          true                },
		{ "log_to_file",             OPT_BOOL,   OPT_C,                   &self->log_to_file,             NULL,          true                },
		{ "log_to_stdout",           OPT_BOOL,   OPT_C,                   &self->log_to_stdout,           NULL,          true                },
		{ "log_connections",         OPT_BOOL,   OPT_C,                   &self->log_connections,         NULL,          true                },
		{ "log_options",             OPT_BOOL,   OPT_C,                   &self->log_options,             NULL,          false               },
		// server
		{ "tls_capath",              OPT_STRING, OPT_C,                   &self->tls_capath,              NULL,          0                   },
		{ "tls_ca",                  OPT_STRING, OPT_C,                   &self->tls_ca,                  NULL,          0                   },
		{ "tls_cert",                OPT_STRING, OPT_C,                   &self->tls_cert,                NULL,          0                   },
		{ "tls_key",                 OPT_STRING, OPT_C,                   &self->tls_key,                 NULL,          0                   },
		{ "listen",                  OPT_JSON,   OPT_C,                   &self->listen,                  NULL,          0                   },
		// limits
		{ "limit_send",              OPT_INT,    OPT_C,                   &self->limit_send,              NULL,          3 * 1024 * 1024     },
		{ "limit_recv",              OPT_INT,    OPT_C,                   &self->limit_recv,              NULL,          1 * 1024 * 1024     },
		{ "limit_write",             OPT_INT,    OPT_C,                   &self->limit_write,             NULL,          0                   },
		// io and compute
		{ "frontends",               OPT_INT,    OPT_C|OPT_Z,             &self->frontends,               NULL,          default_fe          },
		{ "backends",                OPT_INT,    OPT_C|OPT_Z,             &self->backends,                NULL,          default_be          },
		// wal
		{ "wal_worker",              OPT_BOOL,   OPT_C,                   &self->wal_worker,              NULL,          true                },
		{ "wal_crc",                 OPT_BOOL,   OPT_C,                   &self->wal_crc,                 NULL,          true                },
		{ "wal_sync_on_create",      OPT_BOOL,   OPT_C,                   &self->wal_sync_on_create,      NULL,          true                },
		{ "wal_sync_on_close",       OPT_BOOL,   OPT_C,                   &self->wal_sync_on_close,       NULL,          true                },
		{ "wal_sync_on_write",       OPT_BOOL,   OPT_C,                   &self->wal_sync_on_write,       NULL,          false               },
		{ "wal_sync_interval",       OPT_STRING, OPT_C,                   &self->wal_sync_interval,       "1 sec",       0                   },
		{ "wal_size",                OPT_INT,    OPT_C,                   &self->wal_size,                NULL,          67108864            },
		{ "wal_truncate",            OPT_INT,    OPT_C,                   &self->wal_truncate,            NULL,          0                   },
		// replication
		{ "repl_readahead",          OPT_INT,    OPT_C|OPT_Z,             &self->repl_readahead,          NULL,          256 * 1024          },
		{ "repl_reconnect_ms",       OPT_INT,    OPT_C,                   &self->repl_reconnect_ms,       NULL,          3000                },
		// checkpoint
		{ "checkpoint_interval",     OPT_STRING, OPT_C,                   &self->checkpoint_interval,     "5 min",       0                   },
		{ "checkpoint_workers",      OPT_INT,    OPT_C,                   &self->checkpoint_workers,      NULL,          3                   },
		{  NULL,                     0,          0,                       NULL,                           NULL,          0                   },
	};
	opts_define(&self->opts, defs);
}

void
config_save(Config* self, const char* path)
{
	// get a list of optiables
	auto buf = opts_list_persistent(&self->opts);
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
	opts_set(&self->opts, &options);
}
