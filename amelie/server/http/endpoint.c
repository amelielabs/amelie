
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_user.h>
#include <amelie_http.h>

void
endpoint_init(Endpoint* self)
{
	opts_init(&self->opts);
	OptsDef defs[] =
	{
		// protocol
		{ "proto",        OPT_INT,     OPT_C, &self->proto,        NULL, 0    },
		// auth
		{ "user",         OPT_STRING,  OPT_C, &self->user,         NULL, 0    },
		{ "secret",       OPT_STRING,  OPT_C, &self->secret,       NULL, 0    },
		{ "token",        OPT_STRING,  OPT_C, &self->token,        NULL, 0    },
		// host
		{ "host",         OPT_STRING,  OPT_C, &self->host,         NULL, 0    },
		{ "port",         OPT_INT,     OPT_C, &self->port,         NULL, 3485 },
		// repository
		{ "path",         OPT_STRING,  OPT_C, &self->path,         NULL, 0    },
		// tls
		{ "tls_capath",   OPT_STRING,  OPT_C, &self->tls_capath,   NULL, 0    },
		{ "tls_ca",       OPT_STRING,  OPT_C, &self->tls_ca,       NULL, 0    },
		{ "tls_cert",     OPT_STRING,  OPT_C, &self->tls_cert,     NULL, 0    },
		{ "tls_key",      OPT_STRING,  OPT_C, &self->tls_key,      NULL, 0    },
		{ "tls_server",   OPT_STRING,  OPT_C, &self->tls_server,   NULL, 0    },
		// endpoint
		{ "uri",          OPT_STRING,  OPT_C, &self->uri,          NULL, 0    },
		{ "content_type", OPT_STRING,  OPT_C, &self->content_type, NULL, 0    },
		{ "accept",       OPT_STRING,  OPT_C, &self->accept,       NULL, 0    },
		{ "db",           OPT_STRING,  OPT_C, &self->db,           NULL, 0    },
		{ "table",        OPT_STRING,  OPT_C, &self->table,        NULL, 0    },
		{ "function",     OPT_STRING,  OPT_C, &self->function,     NULL, 0    },
		{ "columns",      OPT_STRING,  OPT_C, &self->columns,      NULL, 0    },
		// args
		{ "timezone",     OPT_STRING,  OPT_C, &self->timezone,     NULL, 0    },
		{ "return",       OPT_STRING,  OPT_C, &self->ret,          NULL, 0    },
		// misc
		{ "name",         OPT_STRING,  OPT_C, &self->name,         NULL, 0    },
		{ "debug",        OPT_BOOL,    OPT_C, &self->debug,        NULL, 0    },
		{  NULL,          0,           0,      NULL,               NULL, 0    },
	};
	opts_define(&self->opts, defs);
}

void
endpoint_free(Endpoint* self)
{
	opts_free(&self->opts);
}

void
endpoint_reset(Endpoint* self)
{
	list_foreach(&self->opts.list)
	{
		auto opt = list_at(Opt, link);
		if (opt->type == OPT_STRING || opt->type == OPT_JSON)
			str_free(&opt->string);
		else
			opt->integer = 0;
	}
	self->port.integer = 3485;
}

void
endpoint_copy(Endpoint* self, Endpoint* from)
{
	opts_copy(&self->opts, &from->opts);
}

void
endpoint_read(Endpoint* self, uint8_t** pos)
{
	opts_set_json(&self->opts, pos);
}

void
endpoint_write(Endpoint* self, Buf* buf)
{
	auto opts = opts_list_persistent(&self->opts);
	defer_buf(opts);
	buf_write_buf(buf, opts);
}
