#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Server Server;

typedef void (*ServerEvent)(Server*, Client*);

struct Server
{
	List        listen;
	int         listen_count;
	List        config;
	int         config_count;
	ServerEvent on_connect;
	void*       on_connect_arg;
};

void server_init(Server*);
void server_free(Server*);
void server_start(Server*, ServerEvent, void*);
void server_stop(Server*);
void server_main(Server*);
