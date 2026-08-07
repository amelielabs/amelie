
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
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_rel.h>
#include <amelie_runtime.h>

static const char*
limits_names[LIMIT_MAX] =
{
	// network
	"send",
	"write",
	"connections",

	// db and runtime
	"memory",
	"compute",

	// relations
	"users",
	"tables",
	"indexes",
	"clones",
	"topics",
	"subscriptions",
	"functions",
	"statements",
	"columns",
	"columns_vector",
	"values",
	"args",
	"partitions",
	"vector"
};

static inline int
limits_of(Str* self)
{	
	for (auto i = 0; i < LIMIT_MAX; i++)
		if (str_is_cstr(self, limits_names[i]))
			return i;
	return -1;
}

void
limits_init(Limits* self)
{
	memset(self, 0, sizeof(*self));

	// send
	self->flags |= (1 << LIMIT_SEND);
	self->limits[LIMIT_SEND] = 3 * 1024 * 1024;
}

void
limits_copy(Limits* self, Limits* from)
{
	memcpy(self, from, sizeof(*self));
}

void
limits_read(Limits* self, uint8_t** pos)
{
	// [[name, value], ...]
	unpack_array(pos);
	while (! unpack_array_end(pos))
	{
		unpack_array(pos);

		// name
		Str name;
		unpack_str(pos, &name);
		auto id = limits_of(&name);
		if (unlikely(id == -1))
			error("unrecognized limit name {str}", &name);

		// value
		int64_t value;
		unpack_int(pos, &value);
		self->flags |= (1 << id);
		self->limits[id] = value;

		unpack_array_end(pos);
	}
}

void
limits_write(Limits* self, Buf* buf)
{
	encode_array(buf);

	for (auto i = 0; i < LIMIT_MAX; i++)
	{
		if (! (self->flags & (1 << i)))
			continue;
		encode_array(buf);
		encode_cstr(buf, limits_names[i]);
		encode_int(buf, self->limits[i]);
		encode_array_end(buf);
	}

	encode_array_end(buf);
}
