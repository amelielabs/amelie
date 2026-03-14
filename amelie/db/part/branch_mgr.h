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

typedef struct BranchMgr BranchMgr;

struct BranchMgr
{
	List list;
	int  list_count;
};

static inline void
branch_mgr_init(BranchMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
branch_mgr_free(BranchMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto branch = list_at(Branch, link);
		branch_free(branch);
	}
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
branch_mgr_add(BranchMgr* self, Branch* branch)
{
	list_append(&self->list, &branch->link);
	self->list_count++;
}

static inline void
branch_mgr_remove(BranchMgr* self, Branch* branch)
{
	list_unlink(&branch->link);
	self->list_count--;
}

static inline void
branch_mgr_copy(BranchMgr* self, BranchMgr* copy)
{
	list_foreach(&self->list)
	{
		auto branch = list_at(Branch, link);
		auto branch_copy_ref = branch_copy(branch);
		branch_mgr_add(copy, branch_copy_ref);
	}
}

static inline void
branch_mgr_read(BranchMgr* self, uint8_t** pos)
{
	json_read_array(pos);
	while (! json_read_array_end(pos))
	{
		auto branch = branch_read(pos);
		branch_mgr_add(self, branch);
	}
}

static inline void
branch_mgr_write(BranchMgr* self, Buf* buf, int flags)
{
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto branch = list_at(Branch, link);
		branch_write(branch, buf, flags);
	}
	encode_array_end(buf);
}

static inline Branch*
branch_mgr_find(BranchMgr* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto ref = list_at(Branch, link);
		if (str_compare(&ref->name, name))
			return ref;
	}
	return NULL;
}

static inline Branch*
branch_mgr_find_by(BranchMgr* self, int64_t id)
{
	list_foreach(&self->list)
	{
		auto ref = list_at(Branch, link);
		if (ref->id == id)
			return ref;
	}
	return NULL;
}
