
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
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>

typedef struct OutputType OutputType;

struct OutputType
{
	char*     mime;
	int       mime_size;
	OutputIf* iface;
};

static OutputType output_types[] =
{
	{ "application/json",     16, &output_json    },
	{ "application/json-rpc", 20, &output_jsonrpc },
	{ "text/plain",           10, &output_text    },
	{  NULL,                  0,   NULL           },
};

void
output_init(Output* self)
{
	self->buf      = NULL;
	self->iface    = NULL;
	self->timezone = NULL;
}

void
output_reset(Output* self)
{
	self->iface    = NULL;
	self->timezone = NULL;
	if (self->buf)
		buf_reset(self->buf);
}

void
output_set_buf(Output* self, Buf* buf)
{
	self->buf = buf;
}

static void
output_set_tz(Output* self, Endpoint* endpoint)
{
	self->timezone = runtime()->timezone;

	// set endpoint timezone
	auto timezone = &endpoint->timezone.string;
	if (! str_empty(timezone))
	{
		auto tz = timezone_mgr_find(&runtime()->timezone_mgr, timezone);
		if (tz)
			self->timezone = tz;
	}
}

void
output_set(Output* self, Endpoint* endpoint)
{
	// find and set the output iface
	auto accept = &endpoint->accept.string;
	for (auto i = 0; output_types[i].mime; i++)
	{
		auto type = &output_types[i];
		if (str_is_case(accept, type->mime, type->mime_size))
		{
			self->iface = type->iface;
			break;
		}
	}
	if (! self->iface)
		error("unrecognized Accept '%.*s'", str_size(accept),
		      str_of(accept));

	// set timezone
	output_set_tz(self, endpoint);
}

void
output_set_default(Output* self)
{
	self->iface    = output_types[0].iface;
	self->timezone = runtime()->timezone;
}
