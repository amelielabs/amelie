#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Json Json;

struct Json
{
	char* pos;
	char* end;
	Buf   stack;
	Buf*  buf;
	Buf   buf_data;
};

void json_init(Json*);
void json_free(Json*);
void json_reset(Json*);
void json_parse(Json*, Str*, Buf*);
