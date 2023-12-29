
//
// indigo
//
// SQL OLTP database
//

#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <inttypes.h>
#include <limits.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <indigo.h>

static inline void
print_error(indigo_object_t* object, int verbose)
{
	// map
	indigo_type_t type;
	indigo_arg_t  data;
	indigo_next(object, &data, &type);
	if (type != INDIGO_MAP && data.data_size != 2)
		goto error;

	// code
	indigo_next(object, &data, &type);
	if (type != INDIGO_STRING)
		goto error;
	indigo_next(object, &data, &type);
	if (type != INDIGO_INT)
		goto error;

	// msg
	indigo_next(object, &data, &type);
	if (type != INDIGO_STRING)
		goto error;
	indigo_next(object, &data, &type);
	if (type != INDIGO_STRING)
		goto error;

	char* msg = data.data;
	int   msg_size = data.data_size;
	printf("error: %.*s\n", msg_size, msg);
	return;

error:
	printf("error: bad error reply\n");
}

static inline void
print_object(indigo_object_t* object, int deep)
{
	indigo_arg_t  data;
	indigo_type_t type;
	if (! indigo_next(object, &data, &type))
		return;

	switch (type) {
	case INDIGO_REAL:
		printf("%f", *(double*)data.data);
		break;
	case INDIGO_BOOL:
		if (*(int8_t*)data.data > 0)
			printf("true");
		else
			printf("false");
		break;
	case INDIGO_INT:
		printf("%" PRIi64, *(int64_t*)data.data);
		break;
	case INDIGO_STRING:
		printf("\"%.*s\"", data.data_size, (char*)data.data);
		break;
	case INDIGO_NULL:
		printf("null");
		break;
	case INDIGO_ARRAY:
	{
		int count = *(int64_t*)data.data;
		printf("[ ");
		while (count-- > 0)
		{
			print_object(object, deep);
			if (count > 0)
				printf(", ");
		}
		printf(" ]");
		break;
	}
	case INDIGO_MAP:
	{
		int i = 0;
		int count = *(int64_t*)data.data;
		if (count == 0) {
			printf("{ }");
			break;
		}
		printf("{\n");
		while (count-- > 0)
		{
			for (i = 0; i < deep + 1; i++)
				printf("  ");

			// key
			print_object(object, deep + 1);
			printf(": ");

			// value
			print_object(object, deep + 1);

			if (count > 0)
				printf(",\n");
			else
				printf("\n");
		}
		for (i = 0; i < deep; i++)
			printf("  ");
		printf("}");
		break;
	}
	}
}

static indigo_event_t
read_event(indigo_session_t* session)
{
	for (;;)
	{
		indigo_object_t* result = NULL;
		indigo_event_t event;
		event = indigo_read(session, -1, &result);
		switch (event) {
		case INDIGO_CONNECT:
			printf("connected\n");
			return event;
		case INDIGO_DISCONNECT:
			printf("disconnected\n");
			return event;
		case INDIGO_OK:
			return event;
		case INDIGO_ERROR:
			if (result)
				print_error(result, 0);
			break;
		case INDIGO_OBJECT:
			print_object(result, 0);
			printf("\n");
			break;
		default:
			assert(0);
		}
		if (result)
			indigo_free(result);
	}

	return INDIGO_OK;
}

static void
init(void)
{
	indigo_t* env = indigo_init();
	if (env == NULL) {
		printf("error: indigo_init() failed\n");
		return;
	}

	char options[] = "{  \"log_to_stdout\": true, \"listen\": [{ \"host\": \"*\", \"port\": 3485 }], \"cluster_shards\": 1 }";
	int rc;
	rc = indigo_open(env, "./t", options);
	if (rc == -1) {
		printf("error: indigo_open() failed\n");
		indigo_free(env);
		return;
	}

	indigo_session_t* session;
	session = indigo_connect(env, NULL);
	if (session == NULL)
	{
		printf("error: indigo_connect() failed\n");
		indigo_free(env);
		return;
	}

	indigo_event_t event = read_event(session);
	if (event != INDIGO_CONNECT)
	{
		indigo_free(session);
		indigo_free(env);
		return;
	}

	for (;;)
	{
		if (isatty(fileno(stdin)))
		{
			printf("> ");
			fflush(stdout);
		}
		char text[1024];
		if (! fgets(text, sizeof(text), stdin))
			break;

		int rc;
		rc = indigo_execute(session, text, 0, NULL);
		if (rc == -1)
		{
			printf("error: indigo_execute() failed\n");
			break;
		}

		event = read_event(session);
		if (event != INDIGO_OK)
			break;
	}

	indigo_free(session);
	indigo_free(env);
}

int
main(int argc, char *argv[])
{
	init();
	return 0;
}
