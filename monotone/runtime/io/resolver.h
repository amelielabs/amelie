#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Resolver Resolver;

struct Resolver
{
	Task task;
};

void resolver_init(Resolver*, BufCache*);
void resolver_start(Resolver*);
void resolver_stop(Resolver*);
void resolve(Resolver*, const char*, int, struct addrinfo**);
