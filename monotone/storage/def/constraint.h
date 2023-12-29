#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Constraint Constraint;

enum
{
	GENERATED_NONE,
	GENERATED_SERIAL
};

struct Constraint
{
	bool    not_null;
	int64_t generated;
	Str     expr;
};

static inline void
constraint_init(Constraint* self)
{
	self->not_null  = false;
	self->generated = GENERATED_NONE;
	str_init(&self->expr);
}

static inline void
constraint_free(Constraint* self)
{
	str_free(&self->expr);
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
constraint_set_default(Constraint* self, Str* expr)
{
	str_free(&self->expr);
	str_copy(&self->expr, expr);
}

static inline void
constraint_copy(Constraint* self, Constraint* copy)
{
	constraint_set_not_null(copy, self->not_null);
	constraint_set_generated(copy, self->generated);
	constraint_set_default(copy, &self->expr);
}

static inline void
constraint_read(Constraint* self, uint8_t** pos)
{
	int count;
	data_read_map(pos, &count);

	// not_null
	data_skip(pos);
	data_read_bool(pos, &self->not_null);

	// generated
	data_skip(pos);
	data_read_integer(pos, &self->generated);
	
	// default
	data_skip(pos);
	data_read_string_copy(pos, &self->expr);
}

static inline void
constraint_write(Constraint* self, Buf* buf)
{
	encode_map(buf, 3);

	// not_null
	encode_raw(buf, "not_null", 8);
	encode_bool(buf, self->not_null);

	// generated
	encode_raw(buf, "generated", 9);
	encode_integer(buf, self->generated);

	// default
	encode_raw(buf, "default", 7);
	encode_string(buf, &self->expr);
}
