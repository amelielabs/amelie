#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ReqSet ReqSet;

struct ReqSet
{
	Req**     set;
	int       set_size;
	int       sent;
	int       complete;
	int       error;
	Event     parent;
	ReqMap*   map;
	ReqCache* cache;
	CodeData* code_data;
};

static inline void
req_set_init(ReqSet* self, ReqCache* cache, ReqMap* map,
             CodeData* code_data)
{
	self->set       = NULL;
	self->set_size  = 0;
	self->sent      = 0;
	self->complete  = 0;
	self->error     = 0;
	self->map       = map;
	self->cache     = cache;
	self->code_data = code_data;
	event_init(&self->parent);
}

static inline void
req_set_free(ReqSet* self)
{
	if (self->set)
	{
		for (int i = 0; i < self->set_size; i++)
		{
			auto req = self->set[i];
			if (req)
				req_cache_push(self->cache, req);
		}
		mn_free(self->set);
	}
}

static inline Req*
req_set_at(ReqSet* self, int order)
{
	return self->set[order];
}

static inline bool
req_set_created(ReqSet* self)
{
	return self->set != NULL;
}

static inline void
req_set_create(ReqSet* self, int size)
{
	self->set = mn_malloc(sizeof(Req) * size);
	self->set_size = size;
	memset(self->set, 0, sizeof(Req) * size);
}

static inline void
req_set_reset(ReqSet* self)
{
	for (int i = 0; i < self->set_size; i++)
	{
		auto req = self->set[i];
		if (req)
			req_cache_push(self->cache, req);
	}
	self->sent     = 0;
	self->complete = 0;
	self->error    = 0;
}

hot static inline Req*
req_set_add(ReqSet* self, ReqPartition* part)
{
	auto req = self->set[part->order];
	if (! req)
	{
		req = req_create(self->cache);
		req->dst = part->channel;
		req->code_data = self->code_data;
		event_set_parent(&req->src.on_write.event, &self->parent);
		self->set[part->order] = req;
	}
	return req;
}

hot static inline Req*
req_set_map(ReqSet* self, uint32_t hash)
{
	// get shard by hash
	auto part = req_map_get(self->map, hash);

	// add request to the shard
	return req_set_add(self, part);
}

hot static inline void
req_set_copy(ReqSet* self, Code* code)
{
	auto map = self->map;
	for (int i = 0; i < map->map_order_size; i++)
	{
		auto part = req_map_at(map, i);

		// map request to the shard
		auto req = req_set_add(self, part);

		// append code
		code_copy(&req->code, code);
	}
}
