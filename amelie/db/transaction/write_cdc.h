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
	uint8_t  cmd;
	Uuid*    id;
	uint32_t data_size;
	uint8_t  data[];
} packed;

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
write_cdc_add_row(WriteCdc* self, Cmd cmd, Uuid* id, Row* row,
                  Columns*  columns,
                  Timezone* tz)
{
	auto offset = buf_size(&self->data);
	auto record = (WriteCdcRecord*)buf_emplace(&self->data, sizeof(WriteCdcRecord));
	record->cmd       = cmd;
	record->id        = id;
	record->data_size = 0;
	row_encode(row, columns, tz, &self->data);

	record = (WriteCdcRecord*)(self->data.start + offset);
	record->data_size = buf_size(&self->data) - offset - sizeof(WriteCdcRecord);
}

hot static inline void
write_cdc_add(WriteCdc* self, Cmd cmd, Uuid* id, uint8_t* data, int data_size)
{
	auto record = (WriteCdcRecord*)buf_emplace(&self->data, sizeof(WriteCdcRecord) + data_size);
	record->cmd       = cmd;
	record->id        = id;
	record->data_size = data_size;
	memcpy(record->data, data, data_size);
}
