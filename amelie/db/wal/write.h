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

typedef struct Write Write;

struct Write
{
	Record     record;
	Buf        record_data;
	RecordMsg* recover;
	List       link;
};

static inline void
write_init(Write* self)
{
	self->recover = NULL;
	memset(&self->record, 0, sizeof(self->record));
	buf_init(&self->record_data);
	list_init(&self->link);
}

static inline void
write_free(Write* self)
{
	buf_free(&self->record_data);
}

static inline void
write_reset(Write* self)
{
	self->recover = NULL;
	memset(&self->record, 0, sizeof(self->record));
	buf_reset(&self->record_data);
	list_init(&self->link);
}

static inline void
write_set_recover(Write* self, RecordMsg* msg)
{
	self->recover = msg;
}

static inline void
write_set_flags(Write* self, uint8_t flags)
{
	self->record.flags = flags;
}

static inline void
write_set_lsn(Write* self, uint64_t lsn)
{
	self->record.lsn = lsn;
}

static inline void
write_seal(Write* self)
{
	auto record = &self->record;
	auto record_data = &self->record_data;
	record->crc  = 0;
	record->size = sizeof(Record) + buf_size(record_data);

	// calculate header crc (header + commands)
	if (opt_int_of(&config()->wal_crc))
	{
		uint32_t crc;
		crc = runtime()->crc(0, &record->size, sizeof(Record) - sizeof(uint32_t));
		crc = runtime()->crc(crc, record_data->start, buf_size(record_data));
		record->crc = crc;
	}
}

static inline void
write_cdc_prepare(Write* self, CdcBatch* batch, Rel* user, List* list)
{
	if (self->recover)
	{
		auto record = self->recover->record;
		batch->lsn          = record->lsn;
		batch->request_size = record_data_size(record);
		batch->request      = record_data(record);
	} else
	{
		batch->lsn          = self->record.lsn;
		batch->request_size = buf_size(&self->record_data);
		batch->request      = self->record_data.start;
	}
	batch->list = list;
	batch->user = user;
}
