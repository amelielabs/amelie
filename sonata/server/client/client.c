
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>

Client*
client_create(void)
{
	Client* self;
	self = so_malloc(sizeof(Client));
	self->tls_context  = NULL;
	self->coroutine_id = UINT64_MAX;
	self->arg          = NULL;
	tcp_init(&self->tcp);
	list_init(&self->link);
	return self;
}

void
client_free(Client* self)
{
	client_close(self);
	if (self->tls_context)
		tls_context_free(self->tls_context);
	tcp_free(&self->tcp);
	so_free(self);
}

void
client_set_coroutine_name(Client* self)
{
	char addr[128];
	tcp_getpeername(&self->tcp, addr, sizeof(addr));
	coroutine_set_name(so_self(), "client %s", addr);
}

void
client_attach(Client* self)
{
	assert(self->tcp.fd.fd != -1);
	tcp_attach(&self->tcp);
	self->coroutine_id = so_self()->id;
}

void
client_detach(Client* self)
{
	if (tcp_connected(&self->tcp))
		tcp_detach(&self->tcp);
	self->coroutine_id = UINT64_MAX;
}

void
client_accept(Client* self)
{
	// new client
	client_set_coroutine_name(self);

	// hello
	bool log_connections = var_int_of(&config()->log_connections);
	if (log_connections)
		log("connected");

	// handshake
	tcp_connect_fd(&self->tcp);
}

void
client_close(Client* self)
{
	bool log_connections = var_int_of(&config()->log_connections);
	if (log_connections && tcp_connected(&self->tcp))
		log("disconnected");
	tcp_close(&self->tcp);
}
