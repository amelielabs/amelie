#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Auth Auth;

struct Auth
{
	UserCache user_cache;
};

void auth_init(Auth*);
void auth_free(Auth*);
void auth_sync(Auth*, UserCache*);
bool auth(Auth*, Str*);
