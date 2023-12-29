#ifndef INDIGO_H
#define INDIGO_H

//
// indigo
//	
// SQL OLTP database
//

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <stdbool.h>

#define INDIGO_API __attribute__((visibility("default")))

typedef struct indigo         indigo_t;
typedef struct indigo_session indigo_session_t;
typedef struct indigo_object  indigo_object_t;

typedef enum
{
	INDIGO_OK,
	INDIGO_OBJECT,
	INDIGO_CONNECT,
	INDIGO_DISCONNECT,
	INDIGO_TIMEDOUT,
	INDIGO_ERROR
} indigo_event_t;

typedef enum
{
	INDIGO_ERROR_QUERY,
	INDIGO_ERROR_CANCEL,
	INDIGO_ERROR_CONFLICT,
	INDIGO_ERROR_RO
} indigo_error_t;

typedef enum
{
	INDIGO_MAP,
	INDIGO_ARRAY,
	INDIGO_STRING,
	INDIGO_INT,
	INDIGO_BOOL,
	INDIGO_REAL,
	INDIGO_NULL
} indigo_type_t;

typedef struct
{
	void* data;
	int   data_size;
} indigo_arg_t;

INDIGO_API indigo_t*
indigo_init(void);

INDIGO_API void
indigo_free(void*);

INDIGO_API int
indigo_open(indigo_t*, const char* directory, const char* options);

INDIGO_API indigo_session_t*
indigo_connect(indigo_t*, const char* uri);

INDIGO_API int
indigo_execute(indigo_session_t*, const char* command, int argc, indigo_arg_t*);

INDIGO_API indigo_event_t
indigo_read(indigo_session_t*, int timeout_ms, indigo_object_t**);

INDIGO_API bool
indigo_next(indigo_object_t*, indigo_arg_t*, indigo_type_t*);

INDIGO_API void
indigo_rewind(indigo_object_t*);

#ifdef __cplusplus
}
#endif

#endif // INDIGO_H
