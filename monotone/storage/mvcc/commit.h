#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Commit Commit;

struct Commit
{
	Row*     list;
	Row*     list_last;
	int      list_count;
	uint64_t lsn;
};

static inline void
commit_init(Commit* self)
{
	self->lsn        = 0;
	self->list       = NULL;
	self->list_last  = NULL;
	self->list_count = 0;
}

static inline void
commit_free(Commit* self)
{
	auto row = self->list;
	for (; row; row = row->next_commit)
		row_free(row);
	commit_init(self);
}

static inline void
commit_set_lsn(Commit* self, uint64_t lsn)
{
	self->lsn = lsn;
}

static inline void
commit_add(Commit* self, Row* row)
{
	if (unlikely(self->list == NULL))
		self->list = row;
	else
		self->list_last->next_commit = row;
	row->next_commit = NULL;
	row->prev_commit = self->list_last;
	self->list_last = row;
	self->list_count++;
}

static inline void
commit_remove(Commit* self, Row* row)
{
	if (self->list == row)
		self->list = row->next_commit;

	if (self->list_last == row)
		self->list_last = row->prev_commit;

	if (row->prev_commit)
		row->prev_commit->next_commit = row->next_commit;
	if (row->next_commit)
		row->next_commit->prev_commit = row->prev_commit;
	row->prev_commit = NULL;
	row->next_commit = NULL;

	self->list_count--;
}

static inline void
commit_merge(Commit* self, Commit* with)
{
	if (with->list_count == 0)
		return;

	assert(with->lsn > self->lsn);
	self->lsn = with->lsn;

	if (self->list == NULL)
	{
		self->list       = with->list;
		self->list_last  = with->list_last;
		self->list_count = with->list_count;
	} else
	{
		self->list_last->next_commit = with->list;
		with->list->prev_commit      = self->list_last;
		self->list_last              = with->list_last;
		self->list_count            += with->list_count;
	}

	commit_init(with);
}
