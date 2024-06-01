#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct NodeConfig NodeConfig;

typedef enum
{
	NODE_COMPUTE,
	NODE_REPL
} NodeType;

struct NodeConfig
{
	int64_t type;
	Uuid    id;
	Str     uri;
};

static inline NodeConfig*
node_config_allocate(void)
{
	NodeConfig* self;
	self = so_malloc(sizeof(*self));
	self->type = NODE_COMPUTE;
	uuid_init(&self->id);
	str_init(&self->uri);
	return self;
}

static inline void
node_config_free(NodeConfig* self)
{
	str_free(&self->uri);
	so_free(self);
}

static inline void
node_config_set_type(NodeConfig* self, NodeType type)
{
	self->type = type;
}

static inline void
node_config_set_id(NodeConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline void
node_config_set_uri(NodeConfig* self, Str* uri)
{
	str_copy(&self->uri, uri);
}

static inline NodeConfig*
node_config_copy(NodeConfig* self)
{
	auto copy = node_config_allocate();
	guard(node_config_free, copy);
	node_config_set_type(copy, self->type);
	node_config_set_id(copy, &self->id);
	node_config_set_uri(copy, &self->uri);
	return unguard();
}

static inline NodeConfig*
node_config_read(uint8_t** pos)
{
	auto self = node_config_allocate();
	guard(node_config_free, self);
	Decode map[] =
	{
		{ DECODE_INT,    "type", &self->type },
		{ DECODE_UUID,   "id",   &self->id   },
		{ DECODE_STRING, "uri",  &self->uri  },
		{ 0,              NULL,  NULL        },
	};
	decode_map(map, pos);
	return unguard();
}

static inline void
node_config_write(NodeConfig* self, Buf* buf)
{
	encode_map(buf);
	encode_raw(buf, "type", 4);
	encode_integer(buf, self->type);
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->id);
	encode_raw(buf, "uri", 3);
	encode_string(buf, &self->uri);
	encode_map_end(buf);
}
