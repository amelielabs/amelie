
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
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <immintrin.h>

#if 0
__attribute__((target("avx512f,avx512vnni")))
static inline int32_t
sq8_dot_product_avx512(const uint8_t* query_u8, const int8_t* vector_i8, uint32_t dim)
{
	// AVX-512 VNNI
	//
	// Compute sum(query_u8[i] * vector_i8[i]) across 64-byte chunks
	//
	__m512i acc = _mm512_setzero_si512();

	for (uint32_t i = 0; i < dim; i += 64)
	{
		__m512i q_vec = _mm512_loadu_si512((const __m512i*)(query_u8 + i));
		__m512i v_vec = _mm512_loadu_si512((const __m512i*)(vector_i8 + i));

		// VPDPBUSD: Accumulate 64x (uint8 * int8) into 16x int32 lanes
		acc = _mm512_dpbusd_epi32(acc, q_vec, v_vec);
	}

	return _mm512_reduce_add_epi32(acc);
}
#endif

__attribute__((target("avx2,avxvnni")))
always_inline static inline int32_t
sq8_dot_product_avx2(const uint8_t* query_u8, const int8_t* vector_i8, uint32_t dim)
{
	__m256i acc = _mm256_setzero_si256();

	// process 32 dimensions (32 bytes) per iteration
	for (uint32_t i = 0; i < dim; i += 32)
	{
		__m256i q_vec = _mm256_loadu_si256((const __m256i*)(query_u8 + i));
		__m256i v_vec = _mm256_loadu_si256((const __m256i*)(vector_i8 + i));

		// VPDPBUSD: Multiplies unsigned uint8 query by signed int8 vector
		//
		// Accumulates 32x (uint8 * int8) into 8x int32 lanes
		acc = _mm256_dpbusd_epi32(acc, q_vec, v_vec);
	}

	__m128i low  = _mm256_castsi256_si128(acc);
	__m128i high = _mm256_extracti128_si256(acc, 1);
	__m128i sum  = _mm_add_epi32(low, high);
	sum = _mm_add_epi32(sum, _mm_srli_si128(sum, 8));
	sum = _mm_add_epi32(sum, _mm_srli_si128(sum, 4));
	return _mm_cvtsi128_si32(sum);
}

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
		top[i].score = INT32_MIN;
		top[i].row   = 0;
		top[i].flat  = flat;
		top[i].heap  = heap;
	}
	return self;
}

hot void
matching_execute(Matching* self, const float* __restrict query)
{
	auto top_k = self->top_count;
	auto top   = &self->top[0];

	auto flat              = self->flat;
	const auto dim         = flat->dim;
	const auto page_rows   = flat->page_rows;
	const auto page_chunks = page_rows / 64;

	// Quantize Query Vector
	//
	// (Assumes L2-normalized query bounded [-1.0, 1.0] mapped to [0, 255])
	//
	uint8_t query_u8[dim] cache_line_aligned;
	for (uint32_t i = 0; i < dim; i++)
	{
		float value = (query[i] + 1.0f) * 127.5f;
		if (value < 0.0f)
			query_u8[i] = 0;
		else
		if (value > 255.0f)
			query_u8[i] = 255;
		else
			query_u8[i] = (uint8_t)value;
	}

	int32_t bottom = INT32_MIN;
	for (auto i = 0; i < flat->storage.list_count; i++)
	{
		auto page = storage_at(&flat->storage, i);

		// Direct pointer to contiguous SQ8 byte array
		auto page_sq8 = (const int8_t*)(page->data + flat->page_offset_i8);

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

			uint32_t row_base = chunk_id << 6;
			while (mask > 0)
			{
				uint32_t bit = __builtin_ctzll(mask);
				uint32_t page_row = row_base + bit;

				if (unlikely(page_row >= page->used))
					break;

				// SIMD Dot Product (AVX2)
				auto    vector_sq8 = page_sq8 + (page_row * dim);
				int32_t score = sq8_dot_product_avx2(query_u8, vector_sq8, dim);

				// update top
				if (score > bottom)
				{
					auto pos = top_k - 1;
					while (pos > 0 && top[pos - 1].score < score)
					{
						top[pos].score = top[pos - 1].score;
						top[pos].row   = top[pos - 1].row;
						pos--;
					}

					top[pos].score = score;
					top[pos].row   = i * page_rows + page_row;

					bottom = top[top_k - 1].score;
				}

				mask &= mask - 1;
			}
		}
	}
}

hot void
matching_merge(Value* result, Value** values, int count)
{
	auto first = (Matching*)values[0]->store;
	auto top   = &first->top[0];
	auto top_k = first->top_count;

	// merge results into the first one
	for (auto i = 1; i < count; i++)
	{
		auto next = (Matching*)values[i]->store;
		for (auto j = 0; j < first->top_count; j++)
		{
			auto next_top = &next->top[j];
			if (next_top->score == INT32_MIN)
				continue;

			auto pos = top_k - 1;
			while (pos > 0 && top[pos - 1].score < next_top->score)
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
		if (at->score == INT32_MIN)
			continue;

		auto value = set_reserve(set);
		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);

			auto row_ref = flat_row_at(at->flat, at->row);
			auto row = heap_at(at->heap, row_ref->row_page, row_ref->row_offset);

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
			}
			else
			{
				auto size = column->size;
				if (! size)
				{
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
