
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
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
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
	{ "application/json", 16, &output_json },
	{  NULL,              0,   NULL        },
};

void
output_init(Output* self)
{
	self->buf            = NULL;
	self->iface          = NULL;
	self->timezone       = NULL;
	self->format_pretty  = true;
	self->format_minimal = true;
	str_init(&self->format);
}

void
output_reset(Output* self)
{
	self->iface          = NULL;
	self->timezone       = NULL;
	self->format_pretty  = true;
	self->format_minimal = true;
	if (self->buf)
		buf_reset(self->buf);
	str_init(&self->format);
}

void
output_set_buf(Output* self, Buf* buf)
{
	self->buf = buf;
}

static inline char*
output_set_format_next(Str* name, char* pos, char* pos_end)
{
	// name[-name ...]
	if (pos == pos_end)
		return NULL;

	auto start = pos;
	for (; pos < pos_end; pos++)
		if (*pos == '-')
			break;
	str_set(name, start, pos - start);
	if (unlikely(str_empty(name)))
		error("invalid format string");

	// -
	if (pos != pos_end)
		pos++;

	return pos;
}

static void
output_set_format(Output* self, Endpoint* endpoint)
{
	// reset
	self->format_pretty  = false;
	self->format_minimal = false;

	// use config or prefered format
	self->format = config()->format.string;
	auto format_prefer = &endpoint->format.string;
	if (! str_empty(format_prefer))
		self->format = *format_prefer;

	// name[-name ...]
	Str  name;
	auto pos     = self->format.pos;
	auto pos_end = self->format.end;
	while ((pos = output_set_format_next(&name, pos, pos_end)))
	{
		if (str_is_case(&name, "minimal", 7)) {
			self->format_minimal = true;
		} else
		if (str_is_case(&name, "full", 4)) {
			self->format_minimal = false;
		} else
		if (str_is_case(&name, "pretty", 6)) {
			self->format_pretty = true;
		} else {
			error("unrecognized format option '%.*s'", str_size(&name),
			      str_of(&name));
		}
	}
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

	// read format and set options
	output_set_format(self, endpoint);

	// set timezone
	output_set_tz(self, endpoint);
}

void
output_set_default(Output* self)
{
	self->iface    = output_types[0].iface;
	self->format   = config()->format.string;
	self->timezone = runtime()->timezone;
}
