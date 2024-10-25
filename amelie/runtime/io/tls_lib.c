
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

#if OPENSSL_VERSION_NUMBER < 0x10100000L
#error OpenSSL version is not supported
#endif

void
tls_lib_init(void)
{
	SSL_library_init();
	SSL_load_error_strings();
}

void
tls_lib_free(void)
{
	/*
	SSL_COMP_free_compression_methods();
	*/
	/*FIPS_mode_set(0);*/
	ENGINE_cleanup();
	CONF_modules_unload(1);
	EVP_cleanup();
	CRYPTO_cleanup_all_ex_data();
	ERR_free_strings();
}

static inline char*
tls_lib_strerror(int error)
{
	switch (error) {
	case SSL_ERROR_NONE:
		return "SSL_ERROR_NONE";
	case SSL_ERROR_SSL:
		return "SSL_ERROR_SSL";
	case SSL_ERROR_WANT_CONNECT:
		return "SSL_ERROR_CONNECT";
	case SSL_ERROR_WANT_ACCEPT:
		return "SSL_ERROR_ACCEPT";
	case SSL_ERROR_WANT_READ:
		return "SSL_ERROR_WANT_READ";
	case SSL_ERROR_WANT_WRITE:
		return "SSL_ERROR_WANT_WRITE";
	case SSL_ERROR_WANT_X509_LOOKUP:
		return "SSL_ERROR_WANT_X509_LOOKUP";
	case SSL_ERROR_SYSCALL:
		return "SSL_ERROR_SYSCALL";
	case SSL_ERROR_ZERO_RETURN:
		return "SSL_ERROR_ZERO_RETURN";
	}
	return "SSL_ERROR unknown";
}

void
tls_lib_error(int ssl_error, char* msg)
{
	char* error_str = NULL;
	int   error = ERR_get_error();
	if (error != 0)
		error_str = ERR_error_string(error, NULL);

	if (error > 0 && ssl_error > 0)
		error("%s: %s (%s)", msg, tls_lib_strerror(ssl_error), error_str);
	else
	if (error > 0)
		error("%s: %s", msg, error_str);
	else
	if (ssl_error > 0)
		error("%s: %s", msg, tls_lib_strerror(ssl_error));
	else
		error("%s: %s", msg, strerror(errno));
}
