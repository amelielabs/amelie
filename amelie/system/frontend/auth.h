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

typedef struct Auth Auth;

struct Auth
{
	JwtDecode jwt;
	AuthCache cache;
};

void  auth_init(Auth*);
void  auth_free(Auth*);
User* auth(Auth*, Str*, Str*, bool);
