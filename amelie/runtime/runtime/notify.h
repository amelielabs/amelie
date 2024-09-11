#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Notify Notify;

struct Notify
{
	volatile int signaled;
	Fd           fd;
	Poller*      poller;
};

void notify_init(Notify*);
int  notify_open(Notify*, Poller*);
void notify_close(Notify*);
void notify_read(Notify*);
void notify_write(Notify*);
