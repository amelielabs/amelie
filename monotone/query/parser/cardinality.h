#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Cardinality Cardinality;

struct Cardinality
{
	Buf buf;
};

static inline void
cardinality_init(Cardinality* self)
{
	buf_init(&self->buf);
}

static inline void
cardinality_free(Cardinality* self)
{
	buf_free(&self->buf);
}

static inline void
cardinality_reset(Cardinality* self)
{
	buf_reset(&self->buf);
}
static inline void
cardinality_push(Cardinality* self)
{
	int count = 0;
	buf_write(&self->buf, &count, sizeof(count));
}

static inline void
cardinality_push_of(Cardinality* self, int count)
{
	buf_write(&self->buf, &count, sizeof(count));
}

static inline int
cardinality_pop(Cardinality* self)
{
	int count = *(int*)(self->buf.position - sizeof(int));
	buf_truncate(&self->buf, sizeof(int));
	return count;
}

static inline void
cardinality_inc(Cardinality* self)
{
	int* count = (int*)(self->buf.position - sizeof(int));
	(*count)++;
}
