#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct NodeConfig NodeConfig;

struct NodeConfig
{
	Uuid id;
};

static inline NodeConfig*
node_config_allocate(void)
{
	NodeConfig* self;
	self = am_malloc(sizeof(*self));
	uuid_init(&self->id);
	return self;
}

static inline void
node_config_free(NodeConfig* self)
{
	am_free(self);
}

static inline void
node_config_set_id(NodeConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline NodeConfig*
node_config_copy(NodeConfig* self)
{
	auto copy = node_config_allocate();
	guard(node_config_free, copy);
	node_config_set_id(copy, &self->id);
	return unguard();
}

static inline NodeConfig*
node_config_read(uint8_t** pos)
{
	auto self = node_config_allocate();
	guard(node_config_free, self);
	Decode map[] =
	{
		{ DECODE_UUID, "id",   &self->id },
		{ 0,            NULL,  NULL      },
	};
	decode_map(map, pos);
	return unguard();
}

static inline void
node_config_write(NodeConfig* self, Buf* buf)
{
	encode_map(buf);
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->id);
	encode_map_end(buf);
}
