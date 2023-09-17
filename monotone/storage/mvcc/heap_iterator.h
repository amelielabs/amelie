#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct HeapIterator HeapIterator;

struct HeapIterator
{
	Row*  current;
	int   pos;
	Heap* heap;
};

static inline void
heap_iterator_init(HeapIterator* self)
{
	self->current = NULL;
	self->pos     = 0;
	self->heap    = NULL;
}

hot static inline void
heap_iterator_step(HeapIterator* self)
{
	self->current = NULL;
	for (; self->pos < self->heap->table_size; self->pos++)
	{
		if (self->heap->table[self->pos] == NULL)
			continue;
		self->current = self->heap->table[self->pos];
		break;
	}
}

hot static inline void
heap_iterator_open(HeapIterator* self, Heap* heap)
{
	self->current = NULL;
	self->pos     = 0;
	self->heap    = heap;
	heap_iterator_step(self);
}

hot static inline bool
heap_iterator_has(HeapIterator* self)
{
	return self->current != NULL;
}

hot static inline Row*
heap_iterator_at(HeapIterator* self)
{
	return self->current;
}

hot static inline void
heap_iterator_next(HeapIterator* self)
{
	if (unlikely(self->current == NULL))
		return;
	self->current = self->current->prev;
	if (self->current)
		return;
	self->pos++;
	heap_iterator_step(self);
}
