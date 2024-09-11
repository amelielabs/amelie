#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
int  poller_step(Poller*, List*, List*, int);
int  poller_add(Poller*, Fd*);
int  poller_del(Poller*, Fd*);
int  poller_start_read(Poller*, Fd*);
int  poller_start_write(Poller*, Fd*);
int  poller_stop_read(Poller*, Fd*);
int  poller_stop_write(Poller*, Fd*);
