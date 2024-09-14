#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Channel Channel;

struct Channel
{
	Spinlock  lock;
	BufList   list;
	Condition on_write;
};

void channel_init(Channel*);
void channel_free(Channel*);
void channel_reset(Channel*);
int  channel_attach_to(Channel*, Poller*);
void channel_attach(Channel*);
void channel_detach(Channel*);
void channel_write(Channel*, Buf*);
Buf* channel_read(Channel*, int);
