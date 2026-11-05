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

typedef struct UsersIf UsersIf;
typedef struct Users   Users;

struct UsersIf
{
	void (*attach)(Users*, User*);
	void (*detach)(Users*, User*);
};

struct Users
{
	Rels       users;
	UsersIf*   iface;
	CatalogIf* iface_catalog;
	EvalIf*    iface_eval;
	void*      iface_arg;
};

void users_init(Users*, UsersIf*, CatalogIf*, EvalIf*, void*);
void users_free(Users*);
void users_create(Users*);
void users_status(Users*, Buf*);
bool users_execute(Users*, Tr*, uint8_t*, int);

static inline User*
users_find(Users* self, Str* name, bool error_if_not_exists)
{
	return user_of(rels_find(&self->users, REL_USER, name, error_if_not_exists));
}
