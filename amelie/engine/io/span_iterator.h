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

typedef struct SpanIterator SpanIterator;

struct SpanIterator
{
	Span*       span;
	Buf*        span_data;
	SpanRegion* current;
	int         pos;
	Keys*       keys;
};

static inline void
span_iterator_init(SpanIterator* self)
{
	self->span      = NULL;
	self->span_data = NULL;
	self->current   = NULL;
	self->pos       = 0;
	self->keys      = NULL;
}

hot static inline int
span_iterator_search(SpanIterator* self, Row* row)
{
	int begin = 0;
	int end   = self->span->regions - 1;
	while (begin != end)
	{
		auto mid = begin + (end - begin) / 2;
		auto at = span_region_max(self->span, self->span_data, mid);
		if (compare(self->keys, at, row) < 0)
			begin = mid + 1;
		else
			end = mid;
	}
	if (unlikely(end >= (int)self->span->regions))
		end = self->span->regions - 1;
	return end;
}

hot static inline void
span_iterator_open(SpanIterator* self,
                     Span*       span,
                     Buf*        span_data,
                     Row*        row)
{
	self->span      = span;
	self->span_data = span_data;
	self->current   = NULL;
	self->pos       = 0;
	if (unlikely(span == NULL || span->regions == 0))
		return;

	if (row == NULL)
	{
		self->current = span_region(span, span_data, 0);
		return;
	}
	self->pos = span_iterator_search(self, row);

	auto at = span_region_max(span, span_data, self->pos);
	if (compare(self->keys, at, row) < 0)
		self->pos++;
	if (unlikely(self->pos >= (int)span->regions))
		return;

	self->current = span_region(span, span_data, self->pos);
}

static inline bool
span_iterator_has(SpanIterator* self)
{
	return self->current != NULL;
}

static inline SpanRegion*
span_iterator_at(SpanIterator* self)
{
	return self->current;
}

static inline void
span_iterator_next(SpanIterator* self)
{
	self->pos++;
	self->current = NULL;
	if (unlikely(self->pos >= (int)self->span->regions))
		return;
	self->current = span_region(self->span, self->span_data, self->pos);
}
