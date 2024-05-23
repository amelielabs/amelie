#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct LogSet LogSet;

struct LogSet
{
	Buf  meta;
	Iov  iov;
	List link;
};

static inline void
log_set_init(LogSet* self)
{
	buf_init(&self->meta);
	iov_init(&self->iov);
	list_init(&self->link);
}

static inline void
log_set_free(LogSet* self)
{
	buf_free(&self->meta);
	iov_free(&self->iov);
}

static inline void
log_set_reset(LogSet* self)
{
	buf_reset(&self->meta);
	iov_reset(&self->iov);
	list_init(&self->link);
}

hot static inline void
log_set_add(LogSet* self, int cmd, uint64_t storage, Def* def, Row* row)
{
	// [cmd, storage]
	encode_integer(&self->meta, cmd);
	encode_integer(&self->meta, storage);

	// row
	iov_add(&self->iov, row_data(row, def), row_data_size(row, def));
}

hot static inline void
log_set_add_op(LogSet* self, int cmd, Buf* op)
{
	// [cmd]
	encode_integer(&self->meta, cmd);

	// op
	iov_add_buf(&self->iov, op);
}
