#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct NodeConfig NodeConfig;

struct NodeConfig
{
	Uuid id;
	Str  name;
	Str  uri;
};

static inline NodeConfig*
node_config_allocate(void)
{
	NodeConfig* self;
	self = so_malloc(sizeof(*self));
	str_init(&self->name);
	str_init(&self->uri);
	uuid_init(&self->id);
	return self;
}

static inline void
node_config_free(NodeConfig* self)
{
	str_free(&self->name);
	str_free(&self->uri);
	so_free(self);
}

static inline void
node_config_set_id(NodeConfig* self, Uuid* id)
{
	self->id = *id;
}

static inline void
node_config_set_name(NodeConfig* self, Str* name)
{
	str_copy(&self->name, name);
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
	node_config_set_id(copy, &self->id);
	node_config_set_name(copy, &self->name);
	node_config_set_uri(copy, &self->name);
	return unguard();
}

static inline NodeConfig*
node_config_read(uint8_t** pos)
{
	auto self = node_config_allocate();
	guard(node_config_free, self);
	Decode map[] =
	{
		{ DECODE_UUID,   "id",   &self->id   },
		{ DECODE_STRING, "name", &self->name },
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
	encode_raw(buf, "id", 2);
	encode_uuid(buf, &self->id);
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);
	encode_raw(buf, "uri", 3);
	encode_string(buf, &self->uri);
	encode_map_end(buf);
}
