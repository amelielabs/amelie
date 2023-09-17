#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct HeapCommit HeapCommit;

struct HeapCommit
{
	Commit* commit;
	Commit  commit_1;
	Commit  commit_2;
};

static inline void
heap_commit_init(HeapCommit* self)
{
	self->commit = &self->commit_1;
	commit_init(&self->commit_1);
	commit_init(&self->commit_2);
}

static inline void
heap_commit_free(HeapCommit* self)
{
	commit_free(&self->commit_1);
	commit_free(&self->commit_2);
}

static inline void
heap_commit_set_1(HeapCommit* self)
{
	self->commit = &self->commit_1;
}

static inline void
heap_commit_set_2(HeapCommit* self)
{
	self->commit = &self->commit_2;
}

static inline void
heap_commit_add(HeapCommit* self, Row* row, uint64_t lsn)
{
	commit_set_lsn(self->commit, lsn);

	// add deletes only for the second stage
	if (row->is_delete)
	{
		if (self->commit == &self->commit_1)
		{
			row_free(row);
			return;
		}
	}

	// add to the commit list 1 or 2
	commit_add(self->commit, row);
}

static inline void
heap_commit_remove(HeapCommit* self, Row* row)
{
	// remove from commit list and free if the row was commited
	// in the first stage
	if (self->commit == &self->commit_2)
		return;

	commit_remove(self->commit, row);
	row_free(row);
}
