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
	Uuid     id;
	Endpoint endpoint;
};

static inline ReplicaConfig*
replica_config_allocate(void)
{
	ReplicaConfig* self;
	self = am_malloc(sizeof(*self));
	uuid_init(&self->id);
	endpoint_init(&self->endpoint);
	return self;
}

static inline void
replica_config_free(ReplicaConfig* self)
{
	endpoint_free(&self->endpoint);
	am_free(self);
}

static inline void
replica_config_set_id(ReplicaConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline void
replica_config_set_endpoint(ReplicaConfig* self, Endpoint* endpoint)
{
	endpoint_copy(&self->endpoint, endpoint);
}

static inline ReplicaConfig*
replica_config_copy(ReplicaConfig* self)
{
	auto copy = replica_config_allocate();
	replica_config_set_id(copy, &self->id);
	replica_config_set_endpoint(copy, &self->endpoint);
	return copy;
}

static inline ReplicaConfig*
replica_config_read(uint8_t** pos)
{
	auto self = replica_config_allocate();
	errdefer(replica_config_free, self);
	uint8_t* endpoint = NULL;
	Decode obj[] =
	{
		{ DECODE_UUID, "id",       &self->id },
		{ DECODE_OBJ,  "endpoint", &endpoint },
		{ 0,            NULL,      NULL      },
	};
	decode_obj(obj, "replica", pos);
	endpoint_read(&self->endpoint, &endpoint);
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

	// endpoint
	encode_raw(buf, "endpoint", 8);
	endpoint_write(&self->endpoint, buf);
	encode_obj_end(buf);
}
