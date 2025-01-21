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

typedef struct Timestamp Timestamp;

// 1970-01-01 00:00:00.000000
#define TIMESTAMP_MIN (0LL)

// 9999-12-31 23:59:59.999999
#define TIMESTAMP_MAX (253402300799999999LL)

struct Timestamp
{
	struct tm time;
	int32_t   us;
	bool      zone_set;
	int       zone;
};

static inline void
timestamp_init(Timestamp* self)
{
	memset(self, 0, sizeof(*self));
};

void    timestamp_set(Timestamp*, Str*);
void    timestamp_set_date(Timestamp*, int64_t);
void    timestamp_set_unixtime(Timestamp*, int64_t);
int64_t timestamp_get_unixtime(Timestamp*, Timezone*);
int     timestamp_get(int64_t, Timezone*, char*, int);
void    timestamp_add(Timestamp*, Interval*);
void    timestamp_sub(Timestamp*, Interval*);
void    timestamp_trunc(Timestamp*, Str*);
int64_t timestamp_extract(int64_t, Timezone*, Str*);
int     timestamp_date(int64_t);
