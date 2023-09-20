#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Auth Auth;

enum
{
	AUTH_ACCESS,
	AUTH_VERSION,
	AUTH_UUID,
	AUTH_MODE,
	AUTH_USER,
	AUTH_SECRET,
	AUTH_SALT,
	AUTH_TOKEN,
	AUTH_MAX
};

struct Auth
{
	bool ro;
	bool complete;
	Str  vars[AUTH_MAX];
};

void auth_init(Auth*);
void auth_free(Auth*);
void auth_reset(Auth*);
void auth_set(Auth*, int, Str*);
Str* auth_get(Auth*, int);
void auth_server(Auth*, Tcp*, UserCache*);
void auth_client(Auth*, Tcp*);
