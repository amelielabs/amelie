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

typedef struct Service Service;

struct Service
{
	Spinlock     lock;
	ServicePage* current;
	ServicePage* list;
	int          list_count;
	uint64_t     id;
	List         subs;
	int          subs_count;
	Subscriber*  waiters;
	atomic_u64   errors;
};

void    service_init(Service*);
void    service_free(Service*);
void    service_attach(Service*, Subscriber*);
void    service_detach(Service*, Subscriber*);
void    service_add(Service*, ActionType, Uuid*, Complete*, uint32_t, void*);
Action* service_next(Service*, Subscriber*);

static inline void
service_error(Service* self)
{
	atomic_u64_inc(&self->errors);
}

static inline void
service_sync(Service* self)
{
	Complete co;
	complete_init(&co, self->subs_count, NULL);
	service_add(self, ACTION_SYNC, NULL, &co, 0, NULL);
	complete_wait(&co);
}

static inline void
service_checkpoint(Service* self, Uuid* id, uint64_t* lsn)
{
	service_add(self, ACTION_CHECKPOINT, id, NULL, sizeof(uint64_t**), &lsn);
	service_sync(self);
}
