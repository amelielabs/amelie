#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Mapping Mapping;

enum
{
	MAP_NONE,
	MAP_REFERENCE,
	MAP_RANGE
};

struct Mapping
{
	int64_t type;
	int64_t interval;
};

static inline Mapping*
mapping_allocate(void)
{
	Mapping* self;
	self = mn_malloc(sizeof(Mapping));
	self->type = MAP_NONE;
	self->interval = 0;
	return self;
}

static inline void
mapping_free(Mapping* self)
{
	mn_free(self);
}

static inline void
mapping_set_type(Mapping* self, int type)
{
	self->type = type;
}

static inline void
mapping_set_interval(Mapping* self, int64_t interval)
{
	self->interval = interval;
}

static inline Mapping*
mapping_copy(Mapping* self)
{
	auto copy = mapping_allocate();
	guard(copy_guard, mapping_free, copy);
	mapping_set_type(copy, self->type);
	mapping_set_interval(copy, self->interval);
	return unguard(&copy_guard);
}

static inline Mapping*
mapping_read(uint8_t** pos)
{
	auto self = mapping_allocate();
	guard(self_guard, mapping_free, self);

	// map
	int count;
	data_read_map(pos, &count);

	// type
	data_skip(pos);
	data_read_integer(pos, &self->type);

	// interval
	data_skip(pos);
	data_read_integer(pos, &self->interval);
	return unguard(&self_guard);
}

static inline void
mapping_write(Mapping* self, Buf* buf)
{
	// map
	encode_map(buf, 2);

	// type
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);

	// interval
	encode_raw(buf, "interval", 8);
	encode_integer(buf, self->interval);
}
