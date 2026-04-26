#pragma once

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

typedef struct OutputIf OutputIf;
typedef struct Output   Output;

struct OutputIf
{
	void (*write)(Output*, Columns*, Value*);
	void (*write_data)(Output*, Str*, uint8_t*, bool);
	void (*write_error)(Output*, Error*);
	void (*write_none)(Output*);
};

struct Output
{
	Buf*      buf;
	OutputIf* iface;
	Timezone* timezone;
};

hot static inline void
output_ensure_limit(Output* self)
{
	auto limit = opt_int_of(&config()->limit_send);
	if (unlikely((uint64_t)buf_size(self->buf) >= limit))
		error("output limit reached");
}

void output_init(Output*);
void output_reset(Output*);
void output_set_default(Output*);
void output_set(Output*, Endpoint*);
void output_set_buf(Output*, Buf*);

static inline void
output_value(Output* self, Columns* columns, Value* value)
{
	self->iface->write(self, columns, value);
}

static inline void
output_data(Output* self, Str* column, uint8_t* data, bool unwrap)
{
	self->iface->write_data(self, column, data, unwrap);
}

static inline void
output_error(Output* self, Error* error)
{
	self->iface->write_error(self, error);
}

static inline void
output_none(Output* self)
{
	if (self->iface->write_none)
		self->iface->write_none(self);
}
