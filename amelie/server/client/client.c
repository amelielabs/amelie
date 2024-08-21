
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>

Client*
client_create(void)
{
	Client* self;
	self = am_malloc(sizeof(Client));
	self->tls_context  = NULL;
	self->coroutine_id = UINT64_MAX;
	self->host         = NULL;
	self->arg          = NULL;
	http_init(&self->request);
	http_init(&self->reply);
	readahead_init(&self->readahead, &self->tcp, 16 * 1024);
	uri_init(&self->uri);
	tcp_init(&self->tcp);
	list_init(&self->link);
	return self;
}

void
client_free(Client* self)
{
	http_free(&self->request);
	http_free(&self->reply);
	readahead_free(&self->readahead);
	client_close(self);
	if (self->tls_context)
		tls_context_free(self->tls_context);
	uri_free(&self->uri);
	tcp_free(&self->tcp);
	am_free(self);
}

void
client_set_coroutine_name(Client* self)
{
	char addr[128];
	tcp_getpeername(&self->tcp, addr, sizeof(addr));
	coroutine_set_name(am_self(), "client %s", addr);
}

void
client_set_remote(Client* self, Remote* remote)
{
	self->remote = remote;
	auto uri = remote_get(remote, REMOTE_URI);
	uri_set(&self->uri, uri);
}

void
client_attach(Client* self)
{
	assert(self->tcp.fd.fd != -1);
	tcp_attach(&self->tcp);
	self->coroutine_id = am_self()->id;
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

	// handshake
	tcp_connect_fd(&self->tcp);

	// hello
	bool log_connections = var_int_of(&config()->log_connections);
	if (log_connections)
	{
		if (tls_is_set(&self->tcp.tls))
		{
			char tls_string[128];
			tls_explain(&self->tcp.tls, tls_string, sizeof(tls_string));
			info("connected (%s)", tls_string);
		} else {
			info("connected");
		}
	}
}

#if 0
static inline void
in_connection_create_tls_context(in_connection_t *conn)
{
	/* tls_cert */
	in_uri_arg_t *tls_cert;
	tls_cert = in_uri_find(&conn->uri, "tls_cert", 8);
	if (tls_cert == NULL)
		return;

	in_tls_context_t *context;
	context = in_tls_context_allocate(true);
	in_try
	{
		/* tls_cert */
		in_tls_context_set_path(context, IN_TLS_FILE_CERT,
		                        ".",
		                        (char*)tls_cert->value.start,
		                        in_buf_size(&tls_cert->value));
		/* tls_key */
		in_uri_arg_t *tls_key;
		tls_key = in_uri_find(&conn->uri, "tls_key", 7);
		if (! tls_key)
			in_error("%s", "<tls_key> uri option is not defined"); 
		in_tls_context_set_path(context, IN_TLS_FILE_KEY,
		                        ".",
		                        (char*)tls_key->value.start,
		                        in_buf_size(&tls_key->value));

		/* tls_ca */
		in_uri_arg_t *tls_ca;
		tls_ca = in_uri_find(&conn->uri, "tls_ca", 6);
		if (tls_ca)
			in_tls_context_set_path(context, IN_TLS_FILE_CA,
			                        ".",
			                        (char*)tls_ca->value.start,
			                        in_buf_size(&tls_ca->value));

		/* create tls context */
		in_tls_context_create(context);

	} in_catch
	{
		in_tls_context_free(context);
		in_rethrow();
	}

	conn->tls_context = context;
}

static inline void
in_connection_connect_to(in_connection_t *conn, in_uri_host_t *host)
{
	in_auth_t *auth = &conn->auth;
	in_assert(conn->tcp.fd.fd == -1);

	bool log_connections;
	log_connections = in_var_int_of(in_global()->config->log_connections);
	if (log_connections)
		in_log("connecting to %s:%d", host->host, host->port);

	/* resolve host address */
	char port[16];
	in_snprintf(port, sizeof(port), "%d", host->port);

	struct addrinfo *addr = NULL;
	struct addrinfo  hints;
	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family   = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags    = AI_PASSIVE;
	hints.ai_protocol = IPPROTO_TCP;
	int rc;
	rc = in_resolve(host->host, port, &hints, &addr);
	if (rc != 0 || addr == NULL)
		in_error("failed to resolve %s:%d", host->host, host->port);

	/* create tls context */
	in_connection_create_tls_context(conn);
	if (conn->tls_context)
		in_tcp_set_tls(&conn->tcp, conn->tls_context);

	/* connect */
	in_try
	{
		in_tcp_connect(&conn->tcp, addr->ai_addr);
	} in_catch
	{
		freeaddrinfo(addr);
		in_rethrow();
	}
	freeaddrinfo(addr);

	/* set authentication options */
	in_assert(conn->access != IN_ACCESS_UNDEF);
	char *access = in_access_string(conn->access);
	char *mode   = in_access_mode_string(conn->mode);
	in_auth_set(auth, IN_AUTH_ACCESS, access, strlen(access));
	in_auth_set(auth, IN_AUTH_MODE, mode, strlen(mode));
	if (conn->uri.user)
		in_auth_set(auth, IN_AUTH_USER, conn->uri.user, conn->uri.user_size);
	if (conn->uri.password)
		in_auth_set(auth, IN_AUTH_SECRET, conn->uri.password,
		            conn->uri.password_size);

	/* authenticate */
	in_auth_by_client(auth, &conn->tcp);
	if (in_unlikely(! auth->complete))
		in_error("%s", "authentication failed");

	/* check connection access */
	if (auth->ro)
	{
		if (conn->mode == IN_ACCESS_MODE_RW)
			in_error("host %s:%d is read-only, closing", host->host, host->port);
	}

	/* connected */
	conn->host = host;

	if (log_connections)
		in_log("connected to %s:%d", host->host, host->port);
}
#endif

static void
client_connect_to(Client* self, UriHost* host)
{
	// resolve host address
	struct addrinfo* addr = NULL;
	resolve(global()->resolver, str_of(&host->host), host->port, &addr);
	guard(freeaddrinfo, addr);

	// todo: create tls context

	// connect
	tcp_connect(&self->tcp, addr->ai_addr);

	// connected
	self->host = host;
	bool log_connections = var_int_of(&config()->log_connections);
	if (log_connections)
		info("connected to %s:%d", str_of(&host->host), host->port);
}

void
client_connect(Client* self)
{
	list_foreach(&self->uri.hosts)
	{
		auto host = list_at(UriHost, link);

		Exception e;
		if (enter(&e))
			client_connect_to(self, host);

		if (leave(&e))
		{
			// reset and try next host
			client_close(self);

			// generate only one error for a single host
			if (self->uri.hosts_count == 1)
				rethrow();
		}
		if (self->host)
			break;
	}

	if (self->host == NULL)
		error("failed to connect to any host");
}

void
client_close(Client* self)
{
	bool log_connections = var_int_of(&config()->log_connections);
	if (log_connections && tcp_connected(&self->tcp))
	{
		if (self->host != NULL)
			info("disconnected from %s:%d", str_of(&self->host->host),
			     self->host->port);
		else
			info("disconnected");
	}
	self->host = NULL;
	tcp_close(&self->tcp);
	http_reset(&self->request);
	http_reset(&self->reply);
	readahead_reset(&self->readahead);
}
