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

typedef struct Channel Channel;

struct Channel
{
	Spinlock lock;
	BufList  list;
	Event    on_write;
};

void channel_init(Channel*);
void channel_free(Channel*);
void channel_reset(Channel*);
void channel_attach_to(Channel*, Bus*);
void channel_attach(Channel*);
void channel_detach(Channel*);
void channel_write(Channel*, Buf*);
Buf* channel_read(Channel*, int);
