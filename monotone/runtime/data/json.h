#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Json Json;

struct Json
{
	const char* json;
	int         json_size;
	int         pos;
	Buf*        buf;
	Buf         buf_data;
};

void json_init(Json*);
void json_free(Json*);
void json_reset(Json*);
void json_parse(Json*, Str*);
