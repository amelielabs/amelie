#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct NodeConfig NodeConfig;

struct NodeConfig
{
	Str name;
};

static inline NodeConfig*
node_config_allocate(void)
{
	NodeConfig* self;
	self = am_malloc(sizeof(*self));
	str_init(&self->name);
	return self;
}

static inline void
node_config_free(NodeConfig* self)
{
	str_free(&self->name);
	am_free(self);
}

static inline void
node_config_set_name(NodeConfig* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline NodeConfig*
node_config_copy(NodeConfig* self)
{
	auto copy = node_config_allocate();
	node_config_set_name(copy, &self->name);
	return copy;
}

static inline NodeConfig*
node_config_read(uint8_t** pos)
{
	auto self = node_config_allocate();
	guard(node_config_free, self);
	Decode map[] =
	{
		{ DECODE_STRING, "name", &self->name },
		{ 0,              NULL,  NULL        },
	};
	decode_map(map, "node", pos);
	return unguard();
}

static inline void
node_config_write(NodeConfig* self, Buf* buf)
{
	// map
	encode_map(buf);
	
	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	encode_map_end(buf);
}
