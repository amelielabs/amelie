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

typedef struct SltResult      SltResult;
typedef struct SltResultValue SltResultValue;

struct SltResult
{
	SltSort sort;
	int     columns;
	Buf     result;
	Buf     result_index;
	int     count;
	int     threshold;
	Json    json;
};

struct SltResultValue
{
	int offset;
	int size;
};

static inline void
slt_result_init(SltResult* self)
{
	self->sort      = SLT_SORT_NONE;
	self->columns   = 0;
	self->count     = 0;
	self->threshold = 0;
	buf_init(&self->result);
	buf_init(&self->result_index);
	json_init(&self->json);
}

static inline void
slt_result_free(SltResult* self)
{
	buf_free(&self->result);
	buf_free(&self->result_index);
	json_free(&self->json);
}

static inline void
slt_result_reset(SltResult* self)
{
	self->sort      = SLT_SORT_NONE;
	self->columns   = 0;
	self->count     = 0;
	self->threshold = 0;
	buf_reset(&self->result);
	buf_reset(&self->result_index);
	json_reset(&self->json);
}

static inline bool
slt_result_compare(SltResult* self, Str* with)
{
	return str_is(with, buf_cstr(&self->result), buf_size(&self->result));
}

void slt_result_create(SltResult*, SltSort, int, int, Str*);
