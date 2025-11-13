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

typedef struct SltValue  SltValue;
typedef struct SltResult SltResult;

struct SltValue
{
	int offset;
	int size;
};

struct SltResult
{
	SltSort sort;
	int     columns;
	Buf     output;
	Buf     output_index;
	Buf     result;
	int     count;
	int     threshold;
	Json    json;
};

static inline void
slt_result_init(SltResult* self)
{
	self->sort      = SLT_SORT_NONE;
	self->columns   = 0;
	self->count     = 0;
	self->threshold = 0;
	buf_init(&self->output);
	buf_init(&self->output_index);
	buf_init(&self->result);
	json_init(&self->json);
}

static inline void
slt_result_free(SltResult* self)
{
	buf_free(&self->output);
	buf_free(&self->output_index);
	buf_free(&self->result);
	json_free(&self->json);
}

static inline void
slt_result_reset(SltResult* self)
{
	self->sort      = SLT_SORT_NONE;
	self->columns   = 0;
	self->count     = 0;
	self->threshold = 0;
	buf_reset(&self->output);
	buf_reset(&self->output_index);
	buf_reset(&self->result);
	json_reset(&self->json);
}

static inline bool
slt_result_compare(SltResult* self, Str* with)
{
	return str_is(with, buf_cstr(&self->result), buf_size(&self->result));
}

void slt_result_create(SltResult*, SltSort, int, int, Str*);
