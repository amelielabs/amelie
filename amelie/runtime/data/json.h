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

typedef struct Json Json;

typedef enum
{
	JSON_HINT_NONE,
	JSON_HINT_TIMESTAMP,
	JSON_HINT_INTERVAL,
	JSON_HINT_VECTOR
} JsonHint;

struct Json
{
	char*     pos;
	char*     end;
	Buf       stack;
	Buf*      buf;
	Buf       buf_data;
	Timezone* tz;
	uint64_t  time_us;
};

void json_init(Json*);
void json_free(Json*);
void json_reset(Json*);
void json_set_time(Json*, Timezone*, uint64_t);
void json_parse(Json*, Str*, Buf*);
void json_parse_hint(Json*, Str*, Buf*, JsonHint);
