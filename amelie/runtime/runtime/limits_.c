
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

typedef struct LimitName LimitName;

struct LimitName
{
	char* name;
	int   name_size;
};

static LimitName
limits_names[LIMIT_MAX] =
{
	// network
	{ "send",           4  },
	{ "write",          5  },
	{ "connections",    11 },

	// db and runtime
	{ "memory",         6  },
	{ "compute",        7  },

	// relations
	{ "users",          5  },
	{ "tables",         6  },
	{ "indexes",        7  },
	{ "clones",         6  },
	{ "topics",         6  },
	{ "subscriptions",  13 },
	{ "functions",      9  },
	{ "statements",     10 },
	{ "columns",        7  },
	{ "columns_vector", 14 },
	{ "values",         6  },
	{ "args",           4  },
	{ "partitions",     10 },
	{ "vector",         66 }
};

void
limits_init(Limits* self)
{
	memset(self, 0, sizeof(*self));

	// send
	limits_set(self, LIMIT_SEND, 3 * 1024 * 1024);
}

void
limits_copy(Limits* self, Limits* from)
{
	memcpy(self, from, sizeof(*self));
}

int
limits_find(Str* self)
{
	for (auto i = 0; i < LIMIT_MAX; i++)
	{
		auto name = &limits_names[i];
		if (str_is_case(self, name->name, name->name_size))
			return i;
	}
	return -1;
}

void
limits_read(Limits* self, uint8_t** pos)
{
	// {name, ...}
	unpack_obj(pos);
	while (! unpack_obj_end(pos))
	{
		// name
		Str name;
		unpack_str(pos, &name);
		auto id = limits_find(&name);
		if (unlikely(id == -1))
			error("unrecognized limit name {str}", &name);

		// value
		int64_t value;
		unpack_int(pos, &value);
		limits_set(self, id, value);
	}
}

void
limits_write(Limits* self, Buf* buf)
{
	encode_obj(buf);

	for (auto i = 0; i < LIMIT_MAX; i++)
	{
		if (! limits_is_set(self, i))
			continue;
		encode_cstr(buf, limits_names[i].name);
		encode_int(buf, self->limits[i]);
	}

	encode_obj_end(buf);
}
