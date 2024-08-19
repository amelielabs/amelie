#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct AuthState AuthState;

struct AuthState
{
	Str  header;
	Str  payload;
	Str  digest;
	Str  digest_origin;
	Buf* data;
	Json json;
};

void auth_state_init(AuthState*);
void auth_state_free(AuthState*);
void auth_state_reset(AuthState*);
void auth_state_read(AuthState*, Str*);
void auth_state_read_header(AuthState*);
void auth_state_read_payload(AuthState*, Str*);
void auth_state_validate(AuthState*, Str*);
