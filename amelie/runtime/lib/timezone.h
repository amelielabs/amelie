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

typedef struct TimezoneHeader TimezoneHeader;
typedef struct TimezoneTime   TimezoneTime;
typedef struct Timezone       Timezone;

struct TimezoneHeader
{
	uint8_t  magic[4];
	uint8_t  version;
	uint8_t  unused[15];
	uint32_t isutcnt;
	uint32_t isstdcnt;
	uint32_t leapcnt;
	uint32_t timecnt;
	uint32_t typecnt;
	uint32_t charcnt;
} packed;

struct TimezoneTime
{
	int32_t utoff;
	uint8_t is_dst;
	uint8_t idx;
} packed;

struct Timezone
{
	Str            name;
	TimezoneHeader header;
	uint32_t*      transition_times;
	uint8_t*       transition_types;
	TimezoneTime*  times;
	HashtableNode  node;
};

Timezone*     timezone_create(Str*, char*);
void          timezone_free(Timezone*);
TimezoneTime* timezone_match(Timezone*, time_t);
