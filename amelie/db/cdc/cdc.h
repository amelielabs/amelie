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

typedef struct CdcEvent  CdcEvent;
typedef struct CdcHeader CdcHeader;
typedef struct Cdc       Cdc;

struct CdcEvent
{
	uint64_t lsn;
	uint32_t lsn_op;
	uint8_t  cmd;
	Uuid     id;
	uint32_t data_size;
	uint8_t  data[];
} packed;

struct CdcHeader
{
	uint64_t lsn;
} packed;

struct Cdc
{
	Spinlock lock;
	uint64_t lsn;
	List     subs;
	int      subs_count;
	List     slots;
	int      slots_count;
	Storage  storage;
};

void   cdc_init(Cdc*);
void   cdc_free(Cdc*);
size_t cdc_create(Cdc*, char*);
size_t cdc_open(Cdc*, char*);

// slots and subscriptions
void   cdc_attach(Cdc*, CdcSlot*);
void   cdc_detach(Cdc*, CdcSlot*);
void   cdc_subscribe(Cdc*, CdcSub*);
void   cdc_unsubscribe(Cdc*, CdcSub*);
void   cdc_min(Cdc*, uint64_t*);

// operations
void   cdc_gc(Cdc*);
void   cdc_write(Cdc*, uint64_t, LogCdc*);
void   cdc_write_batch(Cdc*, uint64_t, List*);
