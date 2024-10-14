#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct NodeConfig NodeConfig;

struct NodeConfig
{
	Str  id;
	bool compute;
};

static inline NodeConfig*
node_config_allocate(void)
{
	NodeConfig* self;
	self = am_malloc(sizeof(*self));
	self->compute = true;
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
		{ DECODE_STRING, "id",      &self->id      },
		{ DECODE_BOOL,   "compute", &self->compute },
		{ 0,              NULL,      NULL          },
	};
	decode_obj(obj, "node", pos);
	if (! self->compute)
		error("unsupported node type");
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

	// compute
	encode_raw(buf, "compute", 7);
	encode_bool(buf, self->compute);

	encode_obj_end(buf);
}
