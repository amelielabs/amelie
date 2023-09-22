#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Session Session;

struct Session
{
	Portal* portal;
};

void session_init(Session*, Portal*);
void session_free(Session*);
bool session_execute(Session*, Buf*);
