#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Timestamp Timestamp;

struct Timestamp
{
	struct tm time;
	int       us;
	bool      zone_set;
	int       zone;
};

static inline void
timestamp_init(Timestamp* self)
{
	memset(self, 0, sizeof(*self));
};

void     timestamp_read_value(Timestamp*, uint64_t);
void     timestamp_read(Timestamp*, Str*);
uint64_t timestamp_of(Timestamp*, Timezone*);
int      timestamp_write(uint64_t, Timezone*, char*, int);
void     timestamp_add(Timestamp*, Interval*);
void     timestamp_sub(Timestamp*, Interval*);
void     timestamp_trunc(Timestamp*, Str*);
uint64_t timestamp_extract(uint64_t, Timezone*, Str*);
