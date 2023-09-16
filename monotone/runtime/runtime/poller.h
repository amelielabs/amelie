#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Poller Poller;

struct Poller
{
	int   fd;
	int   list_count;
	int   list_max;
	void* list;
};

void poller_init(Poller*);
void poller_free(Poller*);
int  poller_create(Poller*);
int  poller_step(Poller*, int);
int  poller_add(Poller*, Fd*);
int  poller_del(Poller*, Fd*);
int  poller_read(Poller*, Fd*, FdFunction, void*);
int  poller_write(Poller*, Fd*, FdFunction, void*);
