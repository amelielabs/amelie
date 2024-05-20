#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Channel Channel;

struct Channel
{
	Mutex     lock;
	CondVar   cond_var;
	BufPool   pool;
	Condition on_write;
};

void channel_init(Channel*);
void channel_free(Channel*);
int  channel_attach_to(Channel*, Poller*);
void channel_attach(Channel*);
void channel_detach(Channel*);
void channel_write(Channel*, Buf*);
Buf* channel_read(Channel*, int);
