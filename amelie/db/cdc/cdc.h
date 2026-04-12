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

typedef struct CdcPage  CdcPage;
typedef struct CdcEvent CdcEvent;
typedef struct Cdc      Cdc;

struct CdcPage
{
	List     link;
	uint32_t pos;
	uint32_t end;
	uint8_t  data[];
};

struct CdcEvent
{
	uint64_t lsn;
	Uuid     id;
	uint32_t data_size;
	uint8_t  data[];
};

struct Cdc
{
	Spinlock lock;
	uint64_t lsn;
	bool     shutdown;
	CdcPage* current;
	List     waiters;
	int      waiters_count;
	List     pages;
	int      pages_count;
	List     slots;
	int      slots_count;
	List     link;
};

void cdc_init(Cdc*);
void cdc_free(Cdc*);
void cdc_attach(Cdc*, CdcSlot*);
void cdc_detach(Cdc*, CdcSlot*);
void cdc_shutdown(Cdc*);
void cdc_gc(Cdc*);
void cdc_write(Cdc*, uint64_t, Uuid*, uint8_t*, uint32_t);
