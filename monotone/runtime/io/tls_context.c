
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>

#include <openssl/opensslv.h>
#include <openssl/ssl.h>
#include <openssl/crypto.h>
#include <openssl/engine.h>
#include <openssl/conf.h>
#include <openssl/bio.h>
#include <openssl/err.h>

void
tls_context_init(TlsContext* self, bool client)
{
	self->client = client;
	self->ctx    = NULL;
	memset(self->options, 0, sizeof(self->options));
}

void
tls_context_free(TlsContext* self)
{
	int i = 0;
	while (i < TLS_MAX)
	{
		str_free(&self->options[i]);
		i++;
	}
	if (self->ctx)
		SSL_CTX_free(self->ctx);
}

void
tls_context_set(TlsContext* self, int id, Str* value)
{
	str_free(&self->options[id]);
	str_copy(&self->options[id], value);
}

void
tls_context_set_path(TlsContext* self, int id, const char* directory, Str* name)
{
	// absolute file path
	if (*str_of(name) == '/')
	{
		tls_context_set(self, id, name);
		return;
	}

	// relative to the directory
	char path[PATH_MAX];
	int  path_size;
	path_size = snprintf(path, sizeof(path), "%s/%.*s", directory,
	                     str_size(name), str_of(name));
	Str dir_path;
	str_set(&dir_path, path, path_size);
	tls_context_set(self, id, &dir_path);
}

char*
tls_context_get(TlsContext* self, int id)
{
	return str_of(&self->options[id]);
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
	auto cert = tls_context_get(self, TLS_FILE_CERT);
	int rc;
	if (cert)
	{
		rc = SSL_CTX_use_certificate_chain_file(ctx, cert);
		if (! rc)
			tls_lib_error(0, "SSL_CTX_use_certificate_chafile()");
	}

	// key file
	auto key = tls_context_get(self, TLS_FILE_KEY);
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
	auto ca_path = tls_context_get(self, TLS_PATH_CA);
	auto ca_file = tls_context_get(self, TLS_FILE_CA);
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

	if (! self->client)
	{
		unsigned char sid[SSL_MAX_SSL_SESSION_ID_LENGTH];
		if (! RAND_bytes(sid, sizeof(sid)))
			tls_lib_error(0, "failed to generate session id");

		if (! SSL_CTX_set_session_id_context(ctx, sid, sizeof(sid)))
			tls_lib_error(0, "SSL_CTX_set_session_id_context()");

		SSL_CTX_set_options(ctx, SSL_OP_CIPHER_SERVER_PREFERENCE);
	}
}
