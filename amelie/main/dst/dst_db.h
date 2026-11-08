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

typedef struct DstDb DstDb;
typedef struct Dst   Dst;

struct DstDb
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

DstDb*  dst_db_allocate(Dst*, uint64_t);
void    dst_db_free(DstDb*);
void    dst_db_connect(DstDb*);
void    dst_db_close(DstDb*);
DstRel* dst_db_create(DstDb*, int);
DstRel* dst_db_create_for(DstDb*, DstRel*, int);
void    dst_db_drop(DstDb*, DstRel*);

static inline DstRel*
dst_db_rel(DstDb* self, int order)
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
dst_db_rel_filter(DstDb* self, int order,
                  bool   table,
                  bool   table_vector)
{
	auto pos = 0;
	list_foreach(&self->rels)
	{
		auto rel = list_at(DstRel, link);
		if ((table        && rel->type == DST_REL_TABLE) ||
		    (table_vector && rel->type == DST_REL_TABLE_VECTOR))
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
dst_db_count(DstDb* self,
             bool   table,
             bool   table_vector)
{
	auto count = 0;
	list_foreach(&self->rels)
	{
		auto rel = list_at(DstRel, link);
		if ((table        && rel->type == DST_REL_TABLE) ||
		    (table_vector && rel->type == DST_REL_TABLE_VECTOR))
		{
			count++;
			continue;
		}
	}
	return count;
}
