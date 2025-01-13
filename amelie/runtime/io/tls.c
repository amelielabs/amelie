
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
tls_init(Tls* self)
{
	self->fd      = NULL;
	self->ssl     = NULL;
	self->context = NULL;
	buf_init(&self->write_buf);
}

void
tls_free(Tls* self)
{
	if (self->ssl)
		SSL_free(self->ssl);
	self->fd      = NULL;
	self->ssl     = NULL;
	self->context = NULL;
	buf_free(&self->write_buf);
}

void
tls_error(Tls* self, int ssl_rc, const char* fmt, ...)
{
	int ssl_error;
	ssl_error = SSL_get_error(self->ssl, ssl_rc);
	va_list args;
	va_start(args, fmt);
	char msg[256];
	vsnprintf(msg, sizeof(msg), fmt, args);
	va_end(args);
	tls_lib_error(ssl_error, msg);
}

bool
tls_is_set(Tls* self)
{
	return self->context != NULL;
}

void
tls_set(Tls* self, TlsContext* context)
{
	assert(self->context == NULL);
	self->context = context;
}

void
tls_create(Tls* self, Fd* fd)
{
	assert(self->context);
	self->fd = fd;

	// create ssl object
	SSL* ssl = SSL_new(self->context->ctx);
	if (ssl == NULL)
		tls_lib_error(0, "SSL_new()");
	self->ssl = ssl;

	SSL_set_options(ssl, SSL_OP_IGNORE_UNEXPECTED_EOF);

	// set server name
	auto server = remote_get_cstr(self->context->remote, REMOTE_SERVER);
	if (server)
	{
		int rc;
		rc = SSL_set_tlsext_host_name(ssl, server);
		if (rc != 1)
			tls_lib_error(0, "SSL_setlsext_host_name()");
	}

	// set socket
	int rc;
	rc = SSL_set_rfd(ssl, fd->fd);
	if (rc == -1)
		tls_lib_error(0, "SSL_set_rfd()");

	rc = SSL_set_wfd(ssl, fd->fd);
	if (rc == -1)
		tls_lib_error(0, "SSL_set_wfd()");
}

static inline void
tls_connect(Tls* self)
{
	for (;;)
	{
		int rc = SSL_connect(self->ssl);
		if (rc <= 0)
		{
			int error = SSL_get_error(self->ssl, rc);
			if (error == SSL_ERROR_WANT_READ)
				poll_read(self->fd, -1);
			else
			if (error == SSL_ERROR_WANT_WRITE)
				poll_write(self->fd, -1);
			else
				tls_error(self, rc, "SSL_connect()");
			continue;
		}
		break;
	}
}

static inline void
tls_accept(Tls* self)
{
	for (;;)
	{
		int rc = SSL_accept(self->ssl);
		if (rc <= 0)
		{
			int error = SSL_get_error(self->ssl, rc);
			if (error == SSL_ERROR_WANT_READ)
				poll_read(self->fd, -1);
			else
			if (error == SSL_ERROR_WANT_WRITE)
				poll_write(self->fd, -1);
			else
				tls_error(self, rc, "SSL_accept()");
			continue;
		}
		break;
	}
}

static inline bool
tls_verify_name(const char* cert_name, const char* name)
{
	const char* cert_domain;
	const char* domain;
	const char* next_dot;
	if (strcasecmp(cert_name, name) == 0)
		return true;

	// wildcard match
	if (cert_name[0] != '*')
		return false;

	//
	// valid wildcards:
	// - "*.domain.tld"
	// - "*.sub.domain.tld"
	// - etc.
	// reject "*.tld".
	// no attempt to prevent the use of eg. "*.co.uk".
	//
	cert_domain = &cert_name[1];

	// disallow "*"
	if (cert_domain[0] == '\0')
		return false;

	// disallow "*foo"
	if (cert_domain[0] != '.')
		return false;

	// disallow "*.."
	if (cert_domain[1] == '.')
		return false;

	// disallow "*.bar"
	next_dot = strchr(&cert_domain[1], '.');
	if (next_dot == NULL)
		return false;

	// disallow "*.bar.."
	if (next_dot[1] == '.')
		return false;

	// no wildcard match against a name with no host part.
	domain = strchr(name, '.');
	if (name[0] == '.')
		return false;

	// no wildcard match against a name with no domain part.
	if (domain == NULL || strlen(domain) == 1)
		return false;

	if (strcasecmp(cert_domain, domain) == 0)
		return true;

	return false;
}

static void
tls_verify_common_name(Tls* self, const char* name)
{
	X509* cert = SSL_get_peer_certificate(self->ssl);
	if (cert == NULL)
		tls_error(self, 0, "SSL_get_peer_certificate()");

	auto on_error = error_catch
	(
		X509_NAME* subject_name;
		subject_name = X509_get_subject_name(cert);
		if (subject_name == NULL)
			tls_error(self, 0, "X509_get_subject_name()");

		char common_name[256];
		X509_NAME_get_text_by_NID(subject_name, NID_commonName, common_name, sizeof(common_name));
		if (! tls_verify_name(common_name, name))
			tls_error(self, 0, "bad common name: %s (expected %s)",
			          common_name, name);
	);
	X509_free(cert);

	if (on_error)
		rethrow();
}

void
tls_handshake(Tls* self)
{
	// initial handshake
	if (self->context->client)
		tls_connect(self);
	else
		tls_accept(self);

	// verify server name against certficiate common name
	if (self->context->client)
	{
		auto server = remote_get_cstr(self->context->remote, REMOTE_SERVER);
		if (server)
			tls_verify_common_name(self, server);
	}

	// verify certificate
	int rc = SSL_get_verify_result(self->ssl);
	if (rc != X509_V_OK)
	{
		// ignore self-signed certificate error
		if (rc == X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT ||
		    rc == X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN)
			return;
		auto error = X509_verify_cert_error_string(rc);
		tls_error(self, 0, "SSL_get_verify_result(): %s", error);
	}
}

bool
tls_read_pending(Tls* self)
{
	return SSL_has_pending(self->ssl);
}

int
tls_read(Tls* self, void* buf, int size)
{
	errno = 0;
	int rc = SSL_read(self->ssl, buf, size);
	if (rc > 0) {
		// might have data buffered
		return rc;
	}
	int error = SSL_get_error(self->ssl, rc);
	if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE)
	{
		errno = EAGAIN;
		return -1;
	}
	if (error == SSL_ERROR_ZERO_RETURN)
		return 0;
	// treat socket errors as eof
	if (error == SSL_ERROR_SYSCALL)
		return 0;
	tls_error(self, rc, "SSL_read()");
	return -1;
}

int
tls_write(Tls* self, void* buf, int size)
{
	errno = 0;
	int rc = SSL_write(self->ssl, buf, size);
	if (rc > 0)
		return rc;
	int error = SSL_get_error(self->ssl, rc);
	if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE)
	{
		errno = EAGAIN;
		return -1;
	}
	if (error == SSL_ERROR_ZERO_RETURN)
		return 0;
	tls_error(self, rc, "SSL_write()");
	return -1;
}

hot int
tls_writev(Tls* self, struct iovec* iov, int count)
{
	buf_reset(&self->write_buf);
	while (count > 0)
	{
		buf_write(&self->write_buf, iov->iov_base, iov->iov_len);
		iov++;
		count--;
	}
	return tls_write(self, self->write_buf.start, buf_size(&self->write_buf));
}

int
tls_explain(Tls* self, char* buf, int size)
{
	if (! tls_is_set(self))
		return 0;
	auto version = SSL_get_version(self->ssl);
	auto cipher  = SSL_get_cipher_name(self->ssl);
	return snprintf(buf, size, "%s, %s", version, cipher);
}
