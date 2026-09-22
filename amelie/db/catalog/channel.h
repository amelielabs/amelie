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
	Rel            rel;
	ChannelConfig* config;
};

bool channel_create(Catalog*, Tr*, ChannelConfig*, bool);

always_inline static inline Channel*
channel_of(Rel* self)
{
	return (Channel*)self;
}
