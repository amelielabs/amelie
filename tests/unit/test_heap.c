

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

#include <amelie_test>

void
test_heap(void* arg)
{
	unused(arg);

	auto heap = heap_allocate(false);
	defer(heap_free, heap);

	auto ptr = heap_add(heap, 903);
	memset(ptr, 'X', 903);
	heap_remove(heap, ptr);

	ptr = heap_add(heap, 1432);
	memset(ptr, 'X', 1432);
	heap_remove(heap, ptr);
}

void
test_heap_lru0(void* arg)
{
	unused(arg);

	auto heap = heap_allocate(true);
	defer(heap_free, heap);

	// add and iterate forward
	//
	// [2, 1, 0]

	// 0
	auto a = (int*)heap_add(heap, sizeof(int));
	*a = 0;

	int at = heap->header->lru;
	int at_offset = heap->header->lru_offset;
	for (auto it = 0; at; it++)
	{
		auto chunk = heap_chunk_at(heap, at, at_offset);
		test(*(int*)chunk->data == it);
		at = chunk->next;
		at = chunk->next_offset;
	}

	// 1
	auto b = (int*)heap_add(heap, sizeof(int));
	*b = 1;

	at = heap->header->lru;
	at_offset = heap->header->lru_offset;
	for (auto it = 1; at; it--)
	{
		auto chunk = heap_chunk_at(heap, at, at_offset);
		test(*(int*)chunk->data == it);
		at = chunk->next;
		at = chunk->next_offset;
	}

	// 2
	auto c = (int*)heap_add(heap, sizeof(int));
	*c = 2;

	at = heap->header->lru;
	at_offset = heap->header->lru_offset;
	for (auto it = 2; at; it--)
	{
		auto chunk = heap_chunk_at(heap, at, at_offset);
		test(*(int*)chunk->data == it);
		at = chunk->next;
		at = chunk->next_offset;
	}

	// reverse and unlink
	at = heap->header->lru_tail;
	at_offset = heap->header->lru_tail_offset;
	for (auto it = 0; at; it++)
	{
		auto chunk = heap_chunk_at(heap, at, at_offset);
		test(*(int*)chunk->data == it);
		at = chunk->prev;
		at = chunk->prev_offset;
	}

	heap_unlink(heap, heap_chunk_of(c));

	at = heap->header->lru_tail;
	at_offset = heap->header->lru_tail_offset;
	for (auto it = 0; at; it++)
	{
		auto chunk = heap_chunk_at(heap, at, at_offset);
		test(*(int*)chunk->data == it);
		at = chunk->prev;
		at = chunk->prev_offset;
	}

	heap_unlink(heap, heap_chunk_of(b));

	at = heap->header->lru_tail;
	at_offset = heap->header->lru_tail_offset;
	for (auto it = 0; at; it++)
	{
		auto chunk = heap_chunk_at(heap, at, at_offset);
		test(*(int*)chunk->data == it);
		at = chunk->prev;
		at = chunk->prev_offset;
	}

	heap_unlink(heap, heap_chunk_of(a));

	at = heap->header->lru_tail;
	at_offset = heap->header->lru_tail_offset;
	for (auto it = 0; at; it++)
	{
		auto chunk = heap_chunk_at(heap, at, at_offset);
		test(*(int*)chunk->data == it);
		at = chunk->prev;
		at = chunk->prev_offset;
	}
}

void
test_heap_lru1(void* arg)
{
	unused(arg);

	auto heap = heap_allocate(true);
	defer(heap_free, heap);

	// up
	auto a = (int*)heap_add(heap, sizeof(int));
	*a = 0;

	auto b = (int*)heap_add(heap, sizeof(int));
	*b = 1;

	auto c = (int*)heap_add(heap, sizeof(int));
	*c = 2;

	// [2, 1, 0]
	int        at;
	int        at_offset;
	HeapChunk* chunk;

	// 2
	at        = heap->header->lru;
	at_offset = heap->header->lru_offset;
	chunk     = heap_chunk_at(heap, at, at_offset);
	test(*(int*)chunk->data == 2);
	at = chunk->next;
	at_offset = chunk->next_offset;

	// 1
	chunk     = heap_chunk_at(heap, at, at_offset);
	test(*(int*)chunk->data == 1);
	at = chunk->next;
	at_offset = chunk->next_offset;

	// 0
	chunk     = heap_chunk_at(heap, at, at_offset);
	test(*(int*)chunk->data == 0);
	at = chunk->next;
	at_offset = chunk->next_offset;

	test(at == 0);
	test(at_offset == 0);

	// [1, 2, 0]
	heap_up(heap, b);

	// 1
	at        = heap->header->lru;
	at_offset = heap->header->lru_offset;
	chunk     = heap_chunk_at(heap, at, at_offset);
	test(*(int*)chunk->data == 1);
	at = chunk->next;
	at_offset = chunk->next_offset;

	// 2
	chunk     = heap_chunk_at(heap, at, at_offset);
	test(*(int*)chunk->data == 2);
	at = chunk->next;
	at_offset = chunk->next_offset;

	// 0
	chunk     = heap_chunk_at(heap, at, at_offset);
	test(*(int*)chunk->data == 0);
	at = chunk->next;
	at_offset = chunk->next_offset;

	test(at == 0);
	test(at_offset == 0);

	// [0, 1, 2]
	heap_up(heap, a);

	// 0
	at        = heap->header->lru;
	at_offset = heap->header->lru_offset;
	chunk     = heap_chunk_at(heap, at, at_offset);
	test(*(int*)chunk->data == 0);
	at = chunk->next;
	at_offset = chunk->next_offset;

	// 1
	chunk     = heap_chunk_at(heap, at, at_offset);
	test(*(int*)chunk->data == 1);
	at = chunk->next;
	at_offset = chunk->next_offset;

	// 2
	chunk     = heap_chunk_at(heap, at, at_offset);
	test(*(int*)chunk->data == 2);
	at = chunk->next;
	at_offset = chunk->next_offset;

	test(at == 0);
	test(at_offset == 0);
}
