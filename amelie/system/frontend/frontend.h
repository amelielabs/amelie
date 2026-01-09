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

typedef struct FrontendIf FrontendIf;
typedef struct Frontend   Frontend;

struct FrontendIf
{
	void* (*session_create)(Frontend*, void*);
	void  (*session_free)(void*);
	bool  (*session_execute)(void*, Endpoint*, Str*, Output*);
	void  (*session_execute_replay)(void*, Primary*, Buf*);
};

struct Frontend
{
	ClientMgr   client_mgr;
	Auth        auth;
	FrontendIf* iface;
	void*       iface_arg;
	Task        task;
};

void frontend_init(Frontend*, FrontendIf*, void*);
void frontend_free(Frontend*);
void frontend_start(Frontend*);
void frontend_stop(Frontend*);

static inline void
frontend_add(Frontend* self, Msg* msg)
{
	task_send(&self->task, msg);
}
