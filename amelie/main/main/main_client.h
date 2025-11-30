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

typedef struct MainClientIf MainClientIf;
typedef struct MainClient   MainClient;

struct MainClientIf
{
	MainClient* (*create)(Main*);
	void        (*free)(MainClient*);
	void        (*connect)(MainClient*);
	void        (*send)(MainClient*, Str*);
	int         (*recv)(MainClient*, Str*);
};

struct MainClient
{
	MainClientIf* iface;
	int           pending;
	Histogram*    histogram;
	Main*         main;
	List          link;
};

extern MainClientIf main_remote;
extern MainClientIf main_local;

static inline MainClient*
main_client_create(Main* main)
{
	switch (main->access) {
	case MAIN_REMOTE:
	case MAIN_REMOTE_PATH:
		return main_remote.create(main);
	case MAIN_LOCAL:
		break;
	}
	return main_local.create(main);
}

static inline void
main_client_free(MainClient* self)
{
	self->iface->free(self);
}

static inline void
main_client_connect(MainClient* self)
{
	self->iface->connect(self);
}

static inline void
main_client_set(MainClient* self, Histogram* histogram)
{
	self->histogram = histogram;
}

static inline int
main_client_execute(MainClient* self, Str* cmd, Str* reply)
{
	uint64_t time_us;
	if (self->histogram)
		time_start(&time_us);
	self->iface->send(self, cmd);
	auto code = self->iface->recv(self, reply);
	if (self->histogram)
	{
		time_end(&time_us);
		histogram_add(self->histogram, time_us / 1000);
	}
	return code;
}

/*
static inline int
main_client_execute_import(MainClient* self, Str* path, Str* content_type, Str* content)
{
	uint64_t time_us;
	if (self->histogram)
		time_start(&time_us);
	self->iface->send_import(self, path, content_type, content);
	auto code = self->iface->recv(self, NULL);
	if (self->histogram)
	{
		time_end(&time_us);
		histogram_add(self->histogram, time_us / 1000);
	}
	return code;
}
*/
