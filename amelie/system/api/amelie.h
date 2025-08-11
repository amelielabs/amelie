#pragma once

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

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <stdbool.h>

#define AMELIE_API __attribute__((visibility("default")))

typedef struct amelie         amelie_t;
typedef struct amelie_session amelie_session_t;
typedef struct amelie_request amelie_request_t;

typedef struct
{
	char*  data;
	size_t data_size;
} amelie_arg_t;

AMELIE_API amelie_t*
amelie_init(void);

AMELIE_API void
amelie_free(void*);

AMELIE_API int
amelie_open(amelie_t*, const char* path, int argc, char** argv);

AMELIE_API amelie_session_t*
amelie_connect(amelie_t*);

AMELIE_API void
amelie_set_notify(amelie_session_t*, void (*)(void*), void*);

AMELIE_API amelie_request_t*
amelie_execute(amelie_session_t*, const char* command, int argc, amelie_arg_t*);

AMELIE_API int
amelie_wait(amelie_request_t*, uint32_t time_ms, amelie_arg_t*);

#ifdef __cplusplus
}
#endif
