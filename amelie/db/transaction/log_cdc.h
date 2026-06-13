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

typedef struct LogCdcRecord LogCdcRecord;
typedef struct LogCdc       LogCdc;

struct LogCdcRecord
{
	uint8_t  cmd;
	Uuid*    id;
	uint32_t data_size;
	uint8_t  data[];
} packed;

struct LogCdc
{
	Buf  data;
	List link;
};

static inline void
log_cdc_init(LogCdc* self)
{
	buf_init(&self->data);
	list_init(&self->link);
}

static inline void
log_cdc_free(LogCdc* self)
{
	buf_free(&self->data);
}

static inline void
log_cdc_reset(LogCdc* self)
{
	buf_reset(&self->data);
	list_init(&self->link);
}

static inline bool
log_cdc_empty(LogCdc* self)
{
	return buf_empty(&self->data);
}

hot static inline void
log_cdc_add_row(LogCdc*   self, int cmd, Uuid* id, Row* row,
                Columns*  columns,
                Timezone* tz)
{
	auto offset = buf_size(&self->data);
	auto record = (LogCdcRecord*)buf_emplace(&self->data, sizeof(LogCdcRecord));
	record->cmd       = cmd;
	record->id        = id;
	record->data_size = 0;
	row_encode(row, columns, tz, &self->data);

	record = (LogCdcRecord*)(self->data.start + offset);
	record->data_size = buf_size(&self->data) - offset - sizeof(LogCdcRecord);
}

hot static inline void
log_cdc_add(LogCdc* self, int cmd, Uuid* id, uint8_t* data, int data_size)
{
	auto record = (LogCdcRecord*)buf_emplace(&self->data, sizeof(LogCdcRecord) + data_size);
	record->cmd       = cmd;
	record->id        = id;
	record->data_size = data_size;
	memcpy(record->data, data, data_size);
}
