#ifndef MONOTONE_H
#define MONOTONE_H

//
// monotone
//	
// SQL OLTP database
//

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <stdbool.h>

#define MONO_API __attribute__((visibility("default")))

typedef struct mono         mono_t;
typedef struct mono_session mono_session_t;
typedef struct mono_object  mono_object_t;

typedef enum
{
	MONO_OK,
	MONO_OBJECT,
	MONO_CONNECT,
	MONO_DISCONNECT,
	MONO_TIMEDOUT,
	MONO_ERROR
} mono_event_t;

typedef enum
{
	MONO_ERROR_QUERY,
	MONO_ERROR_CANCEL,
	MONO_ERROR_CONFLICT,
	MONO_ERROR_RO
} mono_error_t;

typedef enum
{
	MONO_MAP,
	MONO_ARRAY,
	MONO_STRING,
	MONO_INT,
	MONO_BOOL,
	MONO_REAL,
	MONO_NULL
} mono_type_t;

typedef struct
{
	void* data;
	int   data_size;
} mono_arg_t;

MONO_API mono_t*
mono_init(void);

MONO_API void
mono_free(void*);

MONO_API int
mono_open(mono_t*, const char* directory, const char* options);

MONO_API mono_session_t*
mono_connect(mono_t*, const char* uri);

MONO_API int
mono_execute(mono_session_t*, const char* command, int argc, mono_arg_t*);

MONO_API mono_event_t
mono_read(mono_session_t*, int timeout_ms, mono_object_t**);

MONO_API bool
mono_next(mono_object_t*, mono_arg_t*, mono_type_t*);

MONO_API void
mono_rewind(mono_object_t*);

#ifdef __cplusplus
}
#endif

#endif // MONOTONE_H
