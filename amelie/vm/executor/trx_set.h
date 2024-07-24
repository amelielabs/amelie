#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct TrxSet TrxSet;

struct TrxSet
{
	Trx** set;
	int   set_size;
	int   set_allocated;
	Buf   buf;
};

static inline void
trx_set_init(TrxSet* self)
{
	self->set           = NULL;
	self->set_size      = 0;
	self->set_allocated = 0;
	buf_init(&self->buf);
}

static inline void
trx_set_reset(TrxSet* self)
{
	self->set           = NULL;
	self->set_size      = 0;
	self->set_allocated = 0;
	buf_reset(&self->buf);
}

static inline void
trx_set_free(TrxSet* self)
{
	buf_free(&self->buf);
}

static inline void
trx_set_create(TrxSet* self, int set_size)
{
	self->set_size      = set_size;
	self->set_allocated = sizeof(Trx*) * set_size;
	buf_reserve(&self->buf, self->set_allocated);
	memset(self->buf.start, 0, self->set_allocated);
	self->set = (Trx**)self->buf.start;
}

hot static inline void
trx_set_resolve(TrxSet* self, TrxSet* set)
{
	for (int i = 0; i < self->set_size; i++)
	{
		if (self->set[i] == NULL)
			continue;
		set->set[i] = self->set[i];
	}
}

always_inline static inline void
trx_set_set(TrxSet* self, int order, Trx* trx)
{
	self->set[order] = trx;
}

always_inline static inline Trx*
trx_set_get(TrxSet* self, int order)
{
	return self->set[order];
}

hot static inline void
trx_set_send(TrxSet* self, int id)
{
	for (int i = 0; i < self->set_size; i++)
	{
		auto trx = trx_set_get(self, i);
		if (trx == NULL)
			continue;
		auto buf = msg_begin(id);
		buf_write(buf, &trx, sizeof(void**));
		msg_end(buf);
		channel_write(trx->route->channel, buf);
	}
}

hot static inline void
trx_set_begin(TrxSet* self)
{
	trx_set_send(self, RPC_BEGIN);
}

hot static inline void
trx_set_commit(TrxSet* self)
{
	trx_set_send(self, RPC_COMMIT);
}

hot static inline void
trx_set_abort(TrxSet* self)
{
	trx_set_send(self, RPC_ABORT);
}
