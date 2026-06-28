
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_sync>
#include <amelie_value.h>
#include <amelie_set.h>
#include <immintrin.h>

const auto infinity = 3.40282347e+38F;

static void
matching_free(Store* store)
{
	auto self = (Matching*)store;
	am_free(self);
}

Matching*
matching_create(Columns* columns, Heap* heap, Flat* flat, int k)
{
	auto size = sizeof(Matching) + sizeof(MatchingTop) * k;
	auto self = (Matching*)am_malloc_aligned(size, cache_line);
	store_init(&self->store, STORE_MATCHING);
	self->store.free     = matching_free;
	self->store.iterator = NULL;
	self->columns        = columns;
	self->heap           = heap;
	self->flat           = flat;
	self->top_count      = k;

	auto top = &self->top[0];
	for (auto i = 0; i < k; i++)
	{
		top[i].distance = infinity;
		top[i].row      = 0;
		top[i].flat     = flat;
		top[i].heap     = heap;
	}
	return self;
}

hot void
matching_execute(Matching* self, const float* __restrict query)
{
	auto top_k  =  self->top_count;
	auto top    = &self->top[0];
	auto bottom = infinity;

	auto flat              = self->flat;
	const auto dim         = flat->column->size_flat / sizeof(float);
	const auto page_rows   = flat->page_rows;
	const auto page_chunks = page_rows / 64;
	for (auto i = 0; i < flat->storage.list_count; i++)
	{
		auto page = storage_at(&flat->storage, i);
		auto page_vectors = flat_vector(flat, i, 0);

		// calculcate the number of active chunks (64 vectors) on the page
		uint32_t chunks = (page->used + 63) >> 6;
		if (chunks > page_chunks)
			chunks = page_chunks;

		auto bitmap = (uint64_t*)(page->data);
		for (uint32_t chunk_id = 0; chunk_id < chunks; chunk_id++)
		{
			// iterate over the block of 64 vectors
			uint64_t mask = bitmap[chunk_id];
			if (! mask)
				continue;

			// chunk_id * 64
			uint32_t row_base = chunk_id << 6;
			while (mask > 0)
			{
				uint32_t bit = __builtin_ctzll(mask);
				uint32_t page_row = row_base + bit;

				if (unlikely(page_row >= page->used))
					break;

				auto vector = page_vectors + (page_row * dim);

				// calculate distance
				auto dist = vector_distance(dim, vector, query);
				if (dist < bottom)
				{
					auto pos = top_k - 1;
					while (pos > 0 && top[pos - 1].distance > dist)
					{
						top[pos].distance = top[pos - 1].distance;
						top[pos].row      = top[pos - 1].row;
						pos--;
					}

					top[pos].distance = dist;
					top[pos].row      = i * page_rows + page_row;

					bottom = top[top_k - 1].distance;
				}

				mask &= mask - 1;
			}
		}
	}
}

hot void
matching_merge(Value* result, Value** values, int count)
{
	// merge results into the first one
	auto first = (Matching*)values[0]->store;
	auto top   = &first->top[0];
	auto top_k = first->top_count;
	for (auto i = 1; i < count; i++)
	{
		auto next = (Matching*)values[i]->store;
		for (auto j = 0; j < first->top_count; j++)
		{
			auto next_top = &next->top[j];
			if (next_top->distance == infinity)
				continue;

			auto pos = top_k - 1;
			while (pos > 0 && top[pos - 1].distance > next_top->distance)
			{
				top[pos] = top[pos - 1];
				pos--;
			}

			top[pos] = *next_top;
		}
	}

	// create result set of matched rows
	auto dim = first->flat->column->size_flat / sizeof(float);
	auto columns = first->columns;
	auto set = set_create();
	set_prepare(set, columns->count, 0, NULL);
	value_set_store(result, &set->store);

	for (auto i = 0; i < top_k; i++)
	{
		auto at = &top[i];
		if (at->distance == infinity)
			continue;

		auto value = set_reserve(set);
		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);

			auto row_ref   = flat_row_at(at->flat, at->row);
			auto row_chunk = heap_chunk_at(at->heap, row_ref->row_page, row_ref->row_offset);
			auto row = (Row*)row_chunk->data;

			auto data = row_column(row, column);
			if (! data)
			{
				value_set_null(&value[column->order]);
				continue;
			}

			if (column->type == TYPE_VECTOR)
			{
				auto vector = (float*)flat_vector_at(at->flat, *(uint32_t*)data);
				value_set_vector(&value[column->order], dim, vector, NULL);
			} else
			{
				auto size = column->size;
				if (! size)
				{
					// json/string
					uint8_t* start = data;
					uint8_t* pos = start;
					data_skip(&pos);
					size = pos - start;
				}
				value_data_decode(&value[column->order], column, data, size);
			}
		}
	}
}
