
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

void
output_init(Output* self)
{
	self->buf      = NULL;
	self->iface    = NULL;
	self->timezone = runtime()->timezone;
	self->endpoint = NULL;
	print_init(&self->print);
}

void
output_free(Output* self)
{
	print_free(&self->print);
}

void
output_reset(Output* self)
{
	self->iface    = NULL;
	self->timezone = runtime()->timezone;
	self->endpoint = NULL;
	if (self->buf)
		buf_reset(self->buf);
	print_reset(&self->print);
}

void
output_set_buf(Output* self, Buf* buf)
{
	self->buf = buf;
}

void
output_set(Output* self, OutputIf* iface, Endpoint* endpoint)
{
	self->iface    = iface;
	self->endpoint = endpoint;
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
