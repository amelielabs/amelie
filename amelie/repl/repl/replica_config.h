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

typedef struct ReplicaConfig ReplicaConfig;

struct ReplicaConfig
{
	Uuid id;
	Str  uri;
};

static inline ReplicaConfig*
replica_config_allocate(void)
{
	ReplicaConfig* self;
	self = am_malloc(sizeof(*self));
	uuid_init(&self->id);
	str_init(&self->uri);
	return self;
}

static inline void
replica_config_free(ReplicaConfig* self)
{
	str_free(&self->uri);
	am_free(self);
}

static inline void
replica_config_set_id(ReplicaConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline void
replica_config_set_uri(ReplicaConfig* self, Str* value)
{
	str_free(&self->uri);
	str_copy(&self->uri, value);
}

static inline ReplicaConfig*
replica_config_copy(ReplicaConfig* self)
{
	auto copy = replica_config_allocate();
	replica_config_set_id(copy, &self->id);
	replica_config_set_uri(copy, &self->uri);
	return copy;
}

static inline ReplicaConfig*
replica_config_read(uint8_t** pos)
{
	auto self = replica_config_allocate();
	errdefer(replica_config_free, self);
	Decode obj[] =
	{
		{ DECODE_UUID, "id",  &self->id  },
		{ DECODE_STR,  "uri", &self->uri },
		{ 0,            NULL,  NULL      },
	};
	decode_obj(obj, "replica", pos);
	return self;
}

static inline void
replica_config_write(ReplicaConfig* self, Buf* buf, int flags)
{
	unused(flags);

	// {}
	encode_obj(buf);

	// id
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->id);

	// uri
	encode_raw(buf, "uri", 3);
	encode_str(buf, &self->uri);
	encode_obj_end(buf);
}
