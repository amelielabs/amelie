#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Heap Heap;

struct Heap
{
	Row**      table;
	int        table_size;
	int        count;
	HeapCommit commit;
	Schema*    schema;
	Uuid*      uuid;
};

void heap_init(Heap*, Uuid*, Schema*);
void heap_free(Heap*);
void heap_create(Heap*, int);
Row* heap_set(Heap*, Row*);
Row* heap_delete(Heap*, Row*);
Row* heap_get(Heap*, Row*);
void heap_abort(Heap*,  Row*, Row*);
void heap_commit(Heap*, Row*, Row*, uint64_t);

static inline bool
heap_is_full(Heap* self)
{
	return self->count > (self->table_size / 2);
}
