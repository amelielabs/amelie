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
	void (*write_json)(Output*, Str*, uint8_t*, bool);
	void (*write_error)(Output*, Error*);
};

struct Output
{
	Buf*      buf;
	OutputIf* iface;
	Timezone* timezone;
	bool      format_pretty;
	bool      format_minimal;
	Endpoint* endpoint;
};

void output_init(Output*, Buf*);
void output_reset(Output*);
void output_set(Output*, Endpoint*);

static inline void
output_write(Output* self, Columns* columns, Value* value)
{
	self->iface->write(self, columns, value);
}

static inline void
output_write_json(Output* self, Str* column, uint8_t* data, bool unwrap)
{
	self->iface->write_json(self, column, data, unwrap);
}

static inline void
output_write_error(Output* self, Error* error)
{
	self->iface->write_error(self, error);
}

hot static inline void
output_ensure_limit(Output* self)
{
	auto limit = opt_int_of(&config()->limit_send);
	if (unlikely((uint64_t)buf_size(self->buf) >= limit))
		error("reply limit reached");
}
