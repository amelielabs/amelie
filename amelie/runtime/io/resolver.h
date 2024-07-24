#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
