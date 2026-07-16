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

typedef struct DstUser DstUser;
typedef struct Dst     Dst;

struct DstUser
{
	uint64_t id;
	Client*  client;
	Endpoint endpoint;
	DstLog   log;
	uint64_t rels_seq;
	List     rels;
	int      rels_count;
	Dst*     dst;
	List     link;
};

DstUser* dst_user_allocate(Dst*, uint64_t);
void     dst_user_free(DstUser*);
void     dst_user_connect(DstUser*);
void     dst_user_close(DstUser*);
DstRel*  dst_user_create(DstUser*, int);
DstRel*  dst_user_create_for(DstUser*, DstRel*, int);
void     dst_user_drop(DstUser*, DstRel*);

static inline DstRel*
dst_user_rel(DstUser* self, int order)
{
	auto pos = 0;
	list_foreach(&self->rels)
	{
		auto rel = list_at(DstRel, link);
		if (pos == order)
			return rel;
		pos++;
	}
	return NULL;
}

static inline DstRel*
dst_user_rel_filter(DstUser* self, int order, bool table, bool topic)
{
	auto pos = 0;
	list_foreach(&self->rels)
	{
		auto rel = list_at(DstRel, link);
		if ((table && rel->type == DST_REL_TABLE) ||
		    (topic && rel->type == DST_REL_TOPIC))
		{
			if (order != pos)
			{
				pos++;
				continue;
			}
			return rel;
		}
	}
	return  NULL;
}

static inline int
dst_user_count(DstUser* self, bool table, bool topic)
{
	auto count = 0;
	list_foreach(&self->rels)
	{
		auto rel = list_at(DstRel, link);
		if ((table && rel->type == DST_REL_TABLE) ||
		    (topic && rel->type == DST_REL_TOPIC))
		{
			count++;
			continue;
		}
	}
	return count;
}
