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
	int      id;
	Client*  client;
	Endpoint endpoint;
	DstLog   log;
	List     rels;
	int      rels_count;
	Dst*     dst;
};

void dst_user_init(DstUser*, Dst*, int);
void dst_user_free(DstUser*);
void dst_user_connect(DstUser*);
void dst_user_close(DstUser*);
void dst_user_create(DstUser*);

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
