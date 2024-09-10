#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct TrList TrList;

struct TrList
{
	int  list_count;
	List list;
};

static inline void
tr_list_init(TrList* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
tr_list_reset(TrList* self, TrCache* cache)
{
	list_foreach_safe(&self->list)
	{
		auto tr = list_at(Tr, link);
		tr_cache_push(cache, tr);
	}
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
tr_list_add(TrList* self, Tr* tr)
{
	list_append(&self->list, &tr->link);
	self->list_count++;
}
