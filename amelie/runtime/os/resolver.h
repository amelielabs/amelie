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

typedef struct Resolver Resolver;

struct Resolver
{
	Task task;
};

void resolver_init(Resolver*);
void resolver_start(Resolver*);
void resolver_stop(Resolver*);
void resolve(Resolver*, const char*, int, struct addrinfo**);
