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

void timestamp_read(Timestamp*, Str*);
int  timestamp_write(uint64_t, char*, int);
uint64_t
timestamp_of(Timestamp*);
