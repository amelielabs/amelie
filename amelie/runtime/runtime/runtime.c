
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
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>

void
runtime_init(Runtime* self)
{
	self->timezone = NULL;
	self->crc      = crc32_sse_supported() ? crc32_sse : crc32;
	self->iface    = NULL;
	buf_mgr_init(&self->buf_mgr);
	config_init(&self->config);
	state_init(&self->state);
	timezone_mgr_init(&self->timezone_mgr);
	random_init(&self->random);
	resolver_init(&self->resolver);
	logger_init(&self->logger);
}

void
runtime_free(Runtime* self)
{
	config_free(&self->config);
	state_free(&self->state);
	timezone_mgr_free(&self->timezone_mgr);
	buf_mgr_free(&self->buf_mgr);
	logger_close(&self->logger);
	tls_lib_free();
}

void
runtime_start(Runtime* self)
{
	// prepare default logger settings
	auto logger = &self->logger;
	logger_set_enable(logger, true);
	logger_set_cli(logger, true, true);
	logger_set_to_stdout(logger, true);

	// init ssl library
	tls_lib_init();

	// set UTC time by default (for posix time api)
	setenv("TZ", "UTC", true);

	// read time zones
	timezone_mgr_open(&self->timezone_mgr);

	// set default timezone as system timezone
	self->timezone = self->timezone_mgr.system;
	logger_set_timezone(logger, self->timezone);

	// init uuid manager
	random_open(&self->random);

	// start resolver
	resolver_start(&self->resolver);

	// prepare default configuration
	config_prepare(config());

	// prepare state
	state_prepare(state());
}

void
runtime_stop(Runtime* self)
{
	// stop resolver
	resolver_stop(&self->resolver);
}
