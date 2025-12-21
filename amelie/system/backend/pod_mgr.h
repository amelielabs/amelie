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

typedef struct PodMgr PodMgr;

struct PodMgr
{
	List  list;
	int   list_count;
	Task* task;
};

static inline void
pod_mgr_init(PodMgr* self, Task* task)
{
	self->task       = task;
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
pod_mgr_create(PodMgr* self, Part* part)
{
	auto pod = pod_allocate(part);
	list_append(&self->list, &pod->link);
	self->list_count++;
	pod_start(pod, self->task);
}

static inline void
pod_mgr_drop(PodMgr* self, Pod* pod)
{
	list_unlink(&pod->link);
	self->list_count--;
	pod_stop(pod);
	pod_free(pod);
}

static inline Pod*
pod_mgr_find(PodMgr* self, Part* part)
{
	list_foreach(&self->list)
	{
		auto pod = list_at(Pod, link);
		if (pod->part == part)
			return pod;
	}
	return NULL;
}

static inline void
pod_mgr_drop_by(PodMgr* self, Part* part)
{
	auto pod = pod_mgr_find(self, part);
	if (! pod)
		return;
	list_unlink(&pod->link);
	self->list_count--;
	pod_stop(pod);
	pod_free(pod);
}

static inline void
pod_mgr_shutdown(PodMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto pod = list_at(Pod, link);
		pod_mgr_drop(self, pod);
	}
}
