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

typedef struct Pods Pods;

struct Pods
{
	List  list;
	int   list_count;
	Task* task;
};

static inline void
pods_init(Pods* self, Task* task)
{
	self->task       = task;
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
pods_create(Pods* self, Part* part)
{
	auto pod = pod_allocate(part);
	list_append(&self->list, &pod->link);
	self->list_count++;
	pod_start(pod, self->task);
}

static inline void
pods_drop(Pods* self, Pod* pod)
{
	list_unlink(&pod->link);
	self->list_count--;
	pod_stop(pod);
	pod_free(pod);
}

static inline Pod*
pods_find(Pods* self, Part* part)
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
pods_drop_by(Pods* self, Part* part)
{
	auto pod = pods_find(self, part);
	if (! pod)
		return;
	list_unlink(&pod->link);
	self->list_count--;
	pod_stop(pod);
	pod_free(pod);
}

static inline void
pods_shutdown(Pods* self)
{
	list_foreach_safe(&self->list)
	{
		auto pod = list_at(Pod, link);
		pods_drop(self, pod);
	}
}
