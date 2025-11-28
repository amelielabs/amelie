
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

#include <amelie_base.h>
#include <amelie_os.h>

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
	self->ctx       = NULL;
	self->client    = false;
	self->file_cert = NULL;
	self->file_key  = NULL;
	self->file_ca   = NULL;
	self->path_ca   = NULL;
	self->server    = NULL;
}

void
tls_context_free(TlsContext* self)
{
	if (self->ctx)
		SSL_CTX_free(self->ctx);
}

void
tls_context_set(TlsContext* self,
                bool        client,
                Str*        file_cert,
                Str*        file_key,
                Str*        file_ca,
                Str*        path_ca,
                Str*        server)
{
	self->client    = client;
	self->file_cert = str_nullif(file_cert);
	self->file_key  = str_nullif(file_key);
	self->file_ca   = str_nullif(file_ca);
	self->path_ca   = str_nullif(path_ca);
	self->server    = str_nullif(server);
}

bool
tls_context_created(TlsContext* self)
{
	return self->ctx != NULL;
}

void
tls_context_create(TlsContext* self)
{
	SSL_CTX* ctx = NULL;
	SSL_METHOD* method = NULL;
	if (self->client)
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
	int rc;
	if (self->file_cert)
	{
		rc = SSL_CTX_use_certificate_chain_file(ctx, str_of(self->file_cert));
		if (! rc)
			tls_lib_error(0, "SSL_CTX_use_certificate_chafile()");
	}

	// key file
	if (self->file_key)
	{
		rc = SSL_CTX_use_PrivateKey_file(ctx, str_of(self->file_key), SSL_FILETYPE_PEM);
		if (rc != 1)
			tls_lib_error(0, "SSL_CTX_use_PrivateKey_file()");
	}
	if (self->file_cert && self->file_key)
	{
		rc = SSL_CTX_check_private_key(ctx);
		if (rc != 1)
			tls_lib_error(0, "SSL_CTX_check_private_key()");
	}

	// ca file and ca_path
	if (self->file_ca || self->path_ca)
	{
		char* file_ca = NULL;
		char* path_ca = NULL;
		if (self->file_ca)
			file_ca = str_of(self->file_ca);
		if (self->path_ca)
			path_ca = str_of(self->path_ca);
		rc = SSL_CTX_load_verify_locations(ctx, file_ca, path_ca);
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
