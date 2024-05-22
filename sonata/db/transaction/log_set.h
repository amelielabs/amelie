#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct LogSet LogSet;

struct LogSet
{
	Buf data;
	Iov iov;
};

static inline void
log_set_init(LogSet* self)
{
	buf_init(&self->data);
	iov_init(&self->iov);
}

static inline void
log_set_free(LogSet* self)
{
	buf_free(&self->data);
	iov_free(&self->iov);
}

static inline void
log_set_reset(LogSet* self)
{
	buf_reset(&self->data);
	iov_reset(&self->iov);
}

static inline void
log_set_prepare(LogSet* self)
{
	// reserve place for header and data
	iov_add(&self->iov, NULL, 0);
	iov_add(&self->iov, NULL, 0);
}

hot static inline void
log_set_add(LogSet* self, int cmd, uint64_t storage, Def* def, Row* row)
{
	// [cmd, storage]
	encode_array(&self->data, 2);
	encode_integer(&self->data, cmd);
	encode_integer(&self->data, storage);

	// row
	iov_add(&self->iov, row_data(row, def), row_data_size(row, def));
}
