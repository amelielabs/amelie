#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Auth Auth;

struct Auth
{
	JwtDecode jwt;
	AuthCache cache;
	UserCache user_cache;
};

void  auth_init(Auth*);
void  auth_free(Auth*);
void  auth_prepare(Auth*);
void  auth_sync(Auth*, UserCache*);
User* auth(Auth*, Str*);
