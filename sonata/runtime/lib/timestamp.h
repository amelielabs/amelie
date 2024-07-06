#pragma once

//
// sonata.
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

void     timestamp_read(Timestamp*, Str*);
void     timestamp_read_value(Timestamp*, uint64_t);
uint64_t timestamp_of(Timestamp*, bool);
int      timestamp_write(uint64_t, char*, int);
void     timestamp_add(Timestamp*, Interval*);
void     timestamp_sub(Timestamp*, Interval*);
