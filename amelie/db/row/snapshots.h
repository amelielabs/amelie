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

typedef struct Snapshots Snapshots;

struct Snapshots
{
	List     list;
	int      list_count;
	Snapshot main;
};

static inline void
snapshots_init(Snapshots* self, Rel* rel)
{
	snapshot_init(&self->main);
	self->main.rel = rel;

	list_init(&self->list);
	list_append(&self->list, &self->main.link);
	self->list_count = 1;
}

static inline void
snapshots_add(Snapshots* self, Snapshot* snapshot)
{
	list_append(&self->list, &snapshot->link);
	self->list_count++;

	snapshot->snapshot_max = snapshot->snapshot;

	// update main snapshot_max (max of children snapshot)
	auto main = &self->main;
	if (snapshot->snapshot > main->snapshot_max)
		main->snapshot_max = snapshot->snapshot;
}

static inline void
snapshots_remove(Snapshots* self, Snapshot* snapshot)
{
	list_unlink(&snapshot->link);
	self->list_count--;

	auto main = &self->main;

	// update main snapshot_max
	int64_t snapshot_max = 0;
	list_foreach(&self->list)
	{
		auto snapshot = list_at(Snapshot, link);
		if (snapshot->snapshot > snapshot_max)
			snapshot_max = snapshot->snapshot;
	}
	main->snapshot_max = snapshot_max;
}

static inline Snapshot*
snapshots_find(Snapshots* self, int64_t id)
{
	list_foreach(&self->list)
	{
		auto ref = list_at(Snapshot, link);
		if (ref->id == id)
			return ref;
	}
	return NULL;
}

static inline Snapshot*
snapshots_first(Snapshots* self)
{
	assert(self->list_count > 0);
	return container_of(list_first(&self->list), Snapshot, link);
}

static inline int
snapshots_max(Snapshots* self)
{
	int max = 0;
	list_foreach(&self->list)
	{
		auto ref = list_at(Snapshot, link);
		if (ref->id > max)
			max = ref->id;
	}
	return max;
}
