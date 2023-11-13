#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct SetMerge SetMerge;

struct SetMerge
{
	SetRow* current;
	SetKey* keys;
	int     keys_count;
	Buf     list;
	int     list_count;
};

static inline void
set_merge_init(SetMerge* self)
{
	self->current    = NULL;
	self->keys       = NULL;
	self->keys_count = 0;
	self->list_count = 0;
	buf_init(&self->list);
}

static inline void
set_merge_free(SetMerge* self)
{
	buf_free(&self->list);
}

static inline void
set_merge_reset(SetMerge* self)
{
	self->current    = NULL;
	self->keys       = NULL;
	self->keys_count = 0;
	self->list_count = 0;
	buf_reset(&self->list);
}

static inline void
set_merge_add(SetMerge* self, Set* set)
{
	// add set iterator
	buf_reserve(&self->list, sizeof(SetIterator));
	auto it = (SetIterator*)self->list.position;
	set_iterator_init(it);
	set_iterator_open(it, set);
	buf_advance(&self->list, sizeof(SetIterator));
	self->list_count++;

	// set keys
	if (self->keys == NULL)
	{
		self->keys = set->keys;
		self->keys_count = set->keys_count;
	}
}

static inline bool
set_merge_has(SetMerge* self)
{
	return self->current != NULL;
}

static inline SetRow*
set_merge_at(SetMerge* self)
{
	return self->current;
}

hot static inline void
set_merge_next(SetMerge* self)
{
	auto list = (SetIterator*)self->list.start;
	int dups  = 0;
	int pos   = 0;
	for (; pos < self->list_count; pos++)
	{
		auto current = &list[pos];
		if (current->advance)
		{
			set_iterator_next(current);
			current->advance = false;
		}
	}
	self->current = NULL;

	SetRow* min = NULL;
	for (pos = 0; pos < self->list_count; pos++)
	{
		auto current = &list[pos];
		auto row = set_iterator_at(current);
		if (row == NULL)
			continue;

		if (min == NULL)
		{
			current->advance = true;
			min = row;
			if (! self->keys)
				break;
			continue;
		}

		int rc;
		rc = set_compare(self->keys, self->keys_count, min, row);
		switch (rc) {
		case 0:
			current->advance = true;
			dups++;
			break;
		case 1:
		{
			int i = 0;
			while (i < self->list_count)
			{
				list[i].advance = false;
				i++;
			}
			dups = 0;
			current->advance = true;
			min = row; 
			break;
		}
		}
	}
	self->current = min;
}

static inline void
set_merge_open(SetMerge* self)
{
	set_merge_next(self);
}
