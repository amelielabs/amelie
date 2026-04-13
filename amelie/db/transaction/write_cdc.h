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

typedef struct WriteCdcRecord WriteCdcRecord;
typedef struct WriteCdc       WriteCdc;

struct WriteCdcRecord
{
	Cmd      cmd;
	Uuid*    id;
	uint32_t data_size;
	uint8_t* data;
};

struct WriteCdc
{
	Buf  data;
	List link;
};

static inline void
write_cdc_init(WriteCdc* self)
{
	buf_init(&self->data);
	list_init(&self->link);
}

static inline void
write_cdc_free(WriteCdc* self)
{
	buf_free(&self->data);
}

static inline void
write_cdc_reset(WriteCdc* self)
{
	buf_reset(&self->data);
	list_init(&self->link);
}

static inline bool
write_cdc_empty(WriteCdc* self)
{
	return buf_empty(&self->data);
}

hot static inline void
write_cdc_add(WriteCdc* self, Cmd cmd, Uuid* id, uint8_t* data, uint32_t data_size)
{
	auto record = (WriteCdcRecord*)buf_emplace(&self->data, sizeof(WriteCdcRecord));
	record->cmd       = cmd;
	record->id        = id;
	record->data_size = data_size;
	record->data      = data;
}
