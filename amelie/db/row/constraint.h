#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Constraint Constraint;

enum
{
	GENERATED_NONE,
	GENERATED_SERIAL,
	GENERATED_RANDOM
};

struct Constraint
{
	bool    not_null;
	int64_t generated;
	int64_t modulo;
	Buf     value;
};

static inline void
constraint_init(Constraint* self)
{
	self->not_null  = false;
	self->generated = GENERATED_NONE;
	self->modulo    = INT64_MAX;
	buf_init(&self->value);
}

static inline void
constraint_free(Constraint* self)
{
	buf_free(&self->value);
}

static inline void
constraint_set_not_null(Constraint* self, bool not_null)
{
	self->not_null = not_null;
}

static inline void
constraint_set_generated(Constraint* self, int value)
{
	self->generated = value;
}

static inline void
constraint_set_modulo(Constraint* self, int64_t value)
{
	self->modulo = value;
}

static inline void
constraint_set_default(Constraint* self, Buf* buf)
{
	buf_reset(&self->value);
	buf_write(&self->value, buf->start, buf_size(buf));
}

static inline void
constraint_copy(Constraint* self, Constraint* copy)
{
	constraint_set_not_null(copy, self->not_null);
	constraint_set_generated(copy, self->generated);
	constraint_set_modulo(copy, self->modulo);
	constraint_set_default(copy, &self->value);
}

static inline void
constraint_read(Constraint* self, uint8_t** pos)
{
	Decode map[] =
	{
		{ DECODE_BOOL, "not_null",  &self->not_null  },
		{ DECODE_INT,  "generated", &self->generated },
		{ DECODE_INT,  "modulo",    &self->modulo    },
		{ DECODE_DATA, "default",   &self->value     },
		{ 0,            NULL,       NULL             },
	};
	decode_map(map, pos);
}

static inline void
constraint_write(Constraint* self, Buf* buf)
{
	encode_map(buf);

	// not_null
	encode_raw(buf, "not_null", 8);
	encode_bool(buf, self->not_null);

	// generated
	encode_raw(buf, "generated", 9);
	encode_integer(buf, self->generated);

	// modulo
	encode_raw(buf, "modulo", 6);
	encode_integer(buf, self->modulo);

	// default
	encode_raw(buf, "default", 7);
	buf_write(buf, self->value.start, buf_size(&self->value));

	encode_map_end(buf);
}
