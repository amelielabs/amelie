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

typedef struct CdcLogRecord CdcLogRecord;
typedef struct CdcLog       CdcLog;

struct CdcLogRecord
{
	uint8_t  cmd;
	Uuid*    id;
	uint32_t data_size;
	uint8_t  data[];
} packed;

struct CdcLog
{
	Buf  data;
	List link;
};

static inline void
cdc_log_init(CdcLog* self)
{
	buf_init(&self->data);
	list_init(&self->link);
}

static inline void
cdc_log_free(CdcLog* self)
{
	buf_free(&self->data);
}

static inline void
cdc_log_reset(CdcLog* self)
{
	buf_reset(&self->data);
	list_init(&self->link);
}

static inline bool
cdc_log_empty(CdcLog* self)
{
	return buf_empty(&self->data);
}

hot static inline void
cdc_log_add_row(CdcLog*   self, int cmd, Uuid* id, Row* row,
                Columns*  columns,
                Timezone* tz)
{
	auto offset = buf_size(&self->data);
	auto record = (CdcLogRecord*)buf_emplace(&self->data, sizeof(CdcLogRecord));
	record->cmd       = cmd;
	record->id        = id;
	record->data_size = 0;
	row_encode(row, columns, tz, &self->data);

	record = (CdcLogRecord*)(self->data.start + offset);
	record->data_size = buf_size(&self->data) - offset - sizeof(CdcLogRecord);
}

hot static inline void
cdc_log_add(CdcLog* self, int cmd, Uuid* id, uint8_t* data, int data_size)
{
	auto record = (CdcLogRecord*)buf_emplace(&self->data, sizeof(CdcLogRecord) + data_size);
	record->cmd       = cmd;
	record->id        = id;
	record->data_size = data_size;
	memcpy(record->data, data, data_size);
}
