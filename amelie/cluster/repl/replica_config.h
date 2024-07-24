#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
replica_config_set_uri(ReplicaConfig* self, Str* uri)
{
	str_free(&self->uri);
	str_copy(&self->uri, uri);
}

static inline ReplicaConfig*
replica_config_copy(ReplicaConfig* self)
{
	auto copy = replica_config_allocate();
	guard(replica_config_free, copy);
	replica_config_set_id(copy, &self->id);
	replica_config_set_uri(copy, &self->uri);
	return unguard();
}

static inline ReplicaConfig*
replica_config_read(uint8_t** pos)
{
	auto self = replica_config_allocate();
	guard(replica_config_free, self);
	Decode map[] =
	{
		{ DECODE_UUID,   "id",  &self->id  },
		{ DECODE_STRING, "uri", &self->uri },
		{ 0,              NULL, NULL       },
	};
	decode_map(map, pos);
	return unguard();
}

static inline void
replica_config_write(ReplicaConfig* self, Buf* buf)
{
	encode_map(buf);
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->id);
	encode_raw(buf, "uri", 3);
	encode_string(buf, &self->uri);
	encode_map_end(buf);
}
