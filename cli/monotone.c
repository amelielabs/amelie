
//
// monotone
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

#include <monotone.h>

static inline void
print_error(mono_object_t* object, int verbose)
{
	// map
	mono_type_t type;
	mono_arg_t  data;
	mono_next(object, &data, &type);
	if (type != MONO_MAP && data.data_size != 2)
		goto error;

	// code
	mono_next(object, &data, &type);
	if (type != MONO_STRING)
		goto error;
	mono_next(object, &data, &type);
	if (type != MONO_INT)
		goto error;

	// msg
	mono_next(object, &data, &type);
	if (type != MONO_STRING)
		goto error;
	mono_next(object, &data, &type);
	if (type != MONO_STRING)
		goto error;

	char* msg = data.data;
	int   msg_size = data.data_size;
	printf("error: %.*s\n", msg_size, msg);
	return;

error:
	printf("error: bad error reply\n");
}

static inline void
print_object(mono_object_t* object, int deep)
{
	mono_arg_t  data;
	mono_type_t type;
	if (! mono_next(object, &data, &type))
		return;

	switch (type) {
	case MONO_FLOAT:
		printf("%f", *(float*)data.data);
		break;
	case MONO_BOOL:
		if (*(int8_t*)data.data > 0)
			printf("true");
		else
			printf("false");
		break;
	case MONO_INT:
		printf("%" PRIi64, *(int64_t*)data.data);
		break;
	case MONO_STRING:
		printf("\"%.*s\"", data.data_size, (char*)data.data);
		break;
	case MONO_NULL:
		printf("null");
		break;
	case MONO_ARRAY:
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
	case MONO_MAP:
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

static mono_event_t
read_event(mono_session_t* session)
{
	for (;;)
	{
		mono_object_t* result = NULL;
		mono_event_t event;
		event = mono_read(session, -1, &result);
		switch (event) {
		case MONO_CONNECT:
			printf("connected\n");
			return event;
		case MONO_DISCONNECT:
			printf("disconnected\n");
			return event;
		case MONO_OK:
			return event;
		case MONO_ERROR:
			if (result)
				print_error(result, 0);
			break;
		case MONO_OBJECT:
			print_object(result, 0);
			printf("\n");
			break;
		default:
			assert(0);
		}
		if (result)
			mono_free(result);
	}

	return MONO_OK;
}

static void
init(void)
{
	mono_t* env = mono_init();
	if (env == NULL) {
		printf("error: mono_init() failed\n");
		return;
	}

	char options[] = "{  \"log_to_stdout\": true, \"listen\": [{ \"host\": \"*\", \"port\": 3485 }], \"cluster_shards\": 1 }";
	int rc;
	rc = mono_open(env, "./t", options);
	if (rc == -1) {
		printf("error: mono_open() failed\n");
		mono_free(env);
		return;
	}

	mono_session_t* session;
	session = mono_connect(env, NULL);
	if (session == NULL)
	{
		printf("error: mono_connect() failed\n");
		mono_free(env);
		return;
	}

	mono_event_t event = read_event(session);
	if (event != MONO_CONNECT)
	{
		mono_free(session);
		mono_free(env);
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
		rc = mono_execute(session, text, 0, NULL);
		if (rc == -1)
		{
			printf("error: mono_execute() failed\n");
			break;
		}

		event = read_event(session);
		if (event != MONO_OK)
			break;
	}

	mono_free(session);
	mono_free(env);
}

int
main(int argc, char *argv[])
{
	init();
	return 0;
}
