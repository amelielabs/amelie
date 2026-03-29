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

typedef struct SnapshotMgr SnapshotMgr;

struct SnapshotMgr
{
	List     list;
	int      list_count;
	Snapshot main;
};

static inline void
snapshot_mgr_init(SnapshotMgr* self)
{
	snapshot_init(&self->main);
	str_set(&self->main.alias, "main", 4);

	list_init(&self->list);
	list_append(&self->list, &self->main.link);
	self->list_count = 1;
}

static inline void
snapshot_mgr_add(SnapshotMgr* self, Snapshot* snapshot)
{
	list_append(&self->list, &snapshot->link);
	self->list_count++;

	// update parent snapshot_max (max of children snapshot)
	assert(snapshot->parent);
	if (snapshot->snapshot > snapshot->parent->snapshot_max)
		snapshot->parent->snapshot_max = snapshot->snapshot;
}

static inline void
snapshot_mgr_remove(SnapshotMgr* self, Snapshot* snapshot)
{
	list_unlink(&snapshot->link);
	self->list_count--;

	auto parent = snapshot->parent;
	assert(parent);

	// update parent snapshot snapshot_max
	int64_t snapshot_max = 0;
	list_foreach(&self->list)
	{
		auto snapshot = list_at(Snapshot, link);
		if (snapshot->parent == parent)
			if (snapshot->snapshot > snapshot_max)
				snapshot_max = snapshot->snapshot;
	}
	parent->snapshot_max = snapshot_max;
}

static inline Snapshot*
snapshot_mgr_find(SnapshotMgr* self, int64_t id)
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
snapshot_mgr_first(SnapshotMgr* self)
{
	assert(self->list_count > 0);
	return container_of(list_first(&self->list), Snapshot, link);
}

static inline int
snapshot_mgr_max(SnapshotMgr* self)
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
