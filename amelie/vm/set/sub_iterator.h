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

typedef struct SubIterator SubIterator;

struct SubIterator
{
	StoreIterator it;
	CdcCursor     cursor;
	Value         value[4];
	Sub*          sub;
};

always_inline static inline SubIterator*
sub_store_iterator_of(StoreIterator* self)
{
	return (SubIterator*)self;
}

hot static inline void
sub_store_iterator_set(SubIterator* self)
{
	auto at = cdc_cursor_at(&self->cursor);
	if (! at)
		return;
	self->it.current = &self->value[0];

	// lsn
	value_set_int(&self->value[0], at->lsn);

	// lsn_op
	value_set_int(&self->value[1], at->lsn_op);

	// cmd
	Str cmd;
	switch (at->cmd) {
	case CMD_REPLACE:
		str_set(&cmd, "write", 5);
		break;
	case CMD_DELETE:
		str_set(&cmd, "delete", 6);
		break;
	case CMD_PUBLISH:
		str_set(&cmd, "publish", 7);
		break;
	}
	value_set_string(&self->value[2], &cmd, NULL);

	// row
	value_set_json(&self->value[3], at->data, at->data_size, NULL);
}

hot static inline void
sub_store_iterator_next(StoreIterator* arg)
{
	auto self = sub_store_iterator_of(arg);
	arg->current = NULL;
	if (cdc_cursor_next(&self->cursor))
		sub_store_iterator_set(self);
}

static inline void
sub_store_iterator_close(StoreIterator* arg)
{
	auto self = sub_store_iterator_of(arg);
	unused(self);
	am_free(arg);
}

static inline StoreIterator*
sub_store_iterator_allocate(Sub* sub, Cdc* cdc)
{
	SubIterator* self = am_malloc(sizeof(*self));
	self->it.next    = sub_store_iterator_next;
	self->it.close   = sub_store_iterator_close;
	self->it.current = NULL;
	self->sub        = sub;

	value_init(&self->value[0]);
	value_init(&self->value[1]);
	value_init(&self->value[2]);
	value_init(&self->value[3]);

	cdc_cursor_init(&self->cursor);
	cdc_cursor_open(&self->cursor, cdc, &sub->on_id, sub->slot.lsn, sub->slot.op);
	sub_store_iterator_set(self);
	return &self->it;
}
