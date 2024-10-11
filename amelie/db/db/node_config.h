#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct NodeConfig NodeConfig;

struct NodeConfig
{
	Str id;
};

static inline NodeConfig*
node_config_allocate(void)
{
	NodeConfig* self;
	self = am_malloc(sizeof(*self));
	str_init(&self->id);
	return self;
}

static inline void
node_config_free(NodeConfig* self)
{
	str_free(&self->id);
	am_free(self);
}

static inline void
node_config_set_id(NodeConfig* self, Str* id)
{
	str_free(&self->id);
	str_copy(&self->id, id);
}

static inline NodeConfig*
node_config_copy(NodeConfig* self)
{
	auto copy = node_config_allocate();
	node_config_set_id(copy, &self->id);
	return copy;
}

static inline NodeConfig*
node_config_read(uint8_t** pos)
{
	auto self = node_config_allocate();
	guard(node_config_free, self);
	Decode obj[] =
	{
		{ DECODE_STRING, "id",  &self->id },
		{ 0,              NULL,  NULL     },
	};
	decode_obj(obj, "node", pos);
	return unguard();
}

static inline void
node_config_write(NodeConfig* self, Buf* buf)
{
	// obj
	encode_obj(buf);
	
	// id
	encode_raw(buf, "id", 2);
	encode_string(buf, &self->id);

	encode_obj_end(buf);
}
