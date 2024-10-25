
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

#include <amelie_runtime.h>
#include <amelie_io.h>

#include <openssl/opensslv.h>
#include <openssl/ssl.h>
#include <openssl/crypto.h>
#include <openssl/engine.h>
#include <openssl/conf.h>
#include <openssl/bio.h>
#include <openssl/err.h>

void
tls_context_init(TlsContext* self)
{
	self->client = false;
	self->ctx    = NULL;
	self->remote = NULL;
}

void
tls_context_free(TlsContext* self)
{
	if (self->ctx)
		SSL_CTX_free(self->ctx);
}

bool
tls_context_created(TlsContext* self)
{
	return self->ctx != NULL;
}

void
tls_context_create(TlsContext* self, bool client, Remote* remote)
{
	self->client = client;
	self->remote = remote;

	SSL_CTX* ctx = NULL;
	SSL_METHOD* method = NULL;
	if (client)
		method = (SSL_METHOD *)SSLv23_client_method();
	else
		method = (SSL_METHOD *)SSLv23_server_method();
	ctx = SSL_CTX_new(method);
	if (ctx == NULL)
		error("%s", "SSL_CTX_NEW() failed");
	self->ctx = ctx;

	SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv2);
	SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv3);
	SSL_CTX_set_mode(ctx, SSL_MODE_ENABLE_PARTIAL_WRITE);
	SSL_CTX_set_mode(ctx, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
	SSL_CTX_set_mode(ctx, SSL_MODE_RELEASE_BUFFERS);

	// certficate file
	auto cert = remote_get_cstr(remote, REMOTE_FILE_CERT);
	int rc;
	if (cert)
	{
		rc = SSL_CTX_use_certificate_chain_file(ctx, cert);
		if (! rc)
			tls_lib_error(0, "SSL_CTX_use_certificate_chafile()");
	}

	// key file
	auto key = remote_get_cstr(remote, REMOTE_FILE_KEY);
	if (key)
	{
		rc = SSL_CTX_use_PrivateKey_file(ctx, key, SSL_FILETYPE_PEM);
		if (rc != 1)
			tls_lib_error(0, "SSL_CTX_use_PrivateKey_file()");
	}
	if (cert && key)
	{
		rc = SSL_CTX_check_private_key(ctx);
		if (rc != 1)
			tls_lib_error(0, "SSL_CTX_check_private_key()");
	}

	// ca file and ca_path
	auto ca_path = remote_get_cstr(remote, REMOTE_PATH_CA);
	auto ca_file = remote_get_cstr(remote, REMOTE_FILE_CA);
	if (ca_file || ca_path)
	{
		rc = SSL_CTX_load_verify_locations(ctx, ca_file, ca_path);
		if (rc != 1)
			tls_lib_error(0, "SSL_CTX_load_verify_locations()");
		int verify;
		verify = SSL_VERIFY_PEER|SSL_VERIFY_CLIENT_ONCE;
		SSL_CTX_set_verify(ctx, verify, NULL);
	}

	// set ciphers
	const char cipher_list[] = "ALL:!aNULL:!eNULL";
	rc = SSL_CTX_set_cipher_list(ctx, cipher_list);
	if (rc != 1)
		tls_lib_error(0, "SSL_CTX_set_cipher_list()");
}
