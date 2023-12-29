
//
// indigo
//
// SQL OLTP database
//

#include <indigo_internal.h>
#include <indigo.h>

enum {
	INDIGO_OBJ         = 0x3fb15941,
	INDIGO_OBJ_OBJECT  = 0x14032890,
	INDIGO_OBJ_SESSION = 0x43132510,
	INDIGO_OBJ_FREED   = 0x28101019
};

struct indigo
{
	int  type;
	Main main;
};

struct indigo_session
{
	int       type;
	Native    native;
	indigo_t* indigo;
};

struct indigo_object
{
	int               type;
	uint8_t*          position;
	uint8_t*          end;
	Buf*              buf;
	int64_t           int_value;
	double            real_value;
	indigo_session_t* session;
};

INDIGO_API indigo_t*
indigo_init(void)
{
	indigo_t* indigo = malloc(sizeof(indigo_t));
	if (unlikely(indigo == NULL))
		return NULL;
	indigo->type = INDIGO_OBJ;
	main_init(&indigo->main);
	return indigo;
}

INDIGO_API int
indigo_open(indigo_t* indigo, const char* directory, const char* config)
{
	Str arg_directory;
	Str arg_config;
	str_init(&arg_directory);
	str_init(&arg_config);
	if (directory)
		str_set_cstr(&arg_directory, directory);
	if (config)
		str_set_cstr(&arg_config, config);
	return main_start(&indigo->main, &arg_directory, &arg_config);
}

INDIGO_API void
indigo_free(void* ptr)
{
	switch (*(int*)ptr) {
	case INDIGO_OBJ:
	{
		indigo_t* indigo = ptr;
		main_stop(&indigo->main);
		main_free(&indigo->main);
		break;
	}
	case INDIGO_OBJ_SESSION:
	{
		indigo_session_t* session = ptr;
		auto main = &session->indigo->main;
		native_close(&session->native, &main->buf_cache);
		native_free(&session->native);
		break;
	}
	case INDIGO_OBJ_OBJECT:
	{
		indigo_object_t* object = ptr;
		if (object->buf)
			buf_cache_push(object->buf->cache, object->buf);
		break;
	}
	case INDIGO_OBJ_FREED:
		fprintf(stderr, "\n%s(%p): attempt to use freed object\n",
		        source_function, ptr);
		fflush(stderr);
		abort();
		return;
	default:
		fprintf(stderr, "\n%s(%p): unexpected object type\n",
		        source_function, ptr);
		fflush(stderr);
		abort();
		return;
	}

	*(int*)ptr = INDIGO_OBJ_FREED;
	free(ptr);
}

INDIGO_API indigo_session_t*
indigo_connect(indigo_t* indigo, const char* uri)
{
	auto main = &indigo->main;

	// allocate session object
	indigo_session_t* session;
	session = malloc(sizeof(indigo_session_t));
	if (unlikely(session == NULL))
		return NULL;
	session->type   = INDIGO_OBJ_SESSION;
	session->indigo = indigo;

	// prepare native connection
	auto native = &session->native;
	native_init(native);
	int rc;
	if (uri)
	{
		rc = native_set_uri(native, uri);
		if (unlikely(rc == -1))
			goto error;
	}

	// register session
	rc = main_connect(main, native);
	if (unlikely(rc == -1))
		goto error;

	native_set_connected(&session->native, true);
	return session;

error:
	native_free(native);
	free(session);
	return NULL;
}

INDIGO_API int
indigo_execute(indigo_session_t* session, const char* command,
             int argc, indigo_arg_t* argv)
{
	Str text;
	str_set_cstr(&text, command);
	return native_command(&session->native,
	                      &session->indigo->main.buf_cache,
	                      &text,
	                      argc,
	                      (CommandArgPtr*)argv);
}

static inline indigo_object_t*
indigo_object_create(indigo_session_t* session, Buf* buf)
{
	indigo_object_t* object;
	object = malloc(sizeof(indigo_object_t));
	if (unlikely(object == NULL))
		return NULL;
	object->type      = INDIGO_OBJ_OBJECT;
	object->session   = session;
	object->buf       = buf;
	object->position  = msg_of(buf)->data;
	object->end       = buf->position;
	object->int_value = 0;
	return object;
}

INDIGO_API indigo_event_t
indigo_read(indigo_session_t* session, int timeout_ms, indigo_object_t** result)
{
	auto buf = channel_read(&session->native.src, timeout_ms);
	if (buf == NULL)
		return INDIGO_TIMEDOUT;

	indigo_event_t indigo_event;
	auto msg = msg_of(buf);
	switch (msg->id) {
	case MSG_OK:
		indigo_event = INDIGO_OK;
		break;
	case MSG_OBJECT:
		indigo_event = INDIGO_OBJECT;
		break;
	case MSG_CONNECT:
		indigo_event = INDIGO_CONNECT;
		break;
	case MSG_DISCONNECT:
		native_set_connected(&session->native, false);
		indigo_event = INDIGO_DISCONNECT;
		break;
	case MSG_ERROR:
		indigo_event = INDIGO_ERROR;
		break;
	default:
		assert(0);
		break;
	}

	if (result && (msg->id == MSG_ERROR || msg->id == MSG_OBJECT))
		*result = indigo_object_create(session, buf);
	else
		buf_cache_push(buf->cache, buf);

	return indigo_event;
}

INDIGO_API hot bool
indigo_next(indigo_object_t* object, indigo_arg_t* arg, indigo_type_t* data_type)
{
	if (likely(data_type))
		*data_type = 0;

	if (unlikely(object->position == NULL))
		return false;
	if (unlikely(object->position == object->end))
		return false;

	indigo_type_t type;
	indigo_arg_t  unused_;
	if (unlikely(arg == NULL))
		arg = &unused_;

	switch (*object->position) {
	// null
	case MN_NULL:
		type = INDIGO_NULL;
		object->position += data_size_type();
		arg->data = NULL;
		arg->data_size = 0;
		break;

	// bool
	case MN_TRUE:
		type = INDIGO_BOOL;
		object->int_value = true;
		object->position += data_size_type();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_FALSE:
		type = INDIGO_BOOL;
		object->int_value = false;
		object->position += data_size_type();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;

	// real
	case MN_REAL32:
		type = INDIGO_REAL;
		object->position += data_size_type();
		object->real_value = *(float*)object->position;
		object->position += sizeof(float);
		arg->data = &object->real_value;
		arg->data_size = sizeof(double);
		break;

	case MN_REAL64:
		type = INDIGO_REAL;
		object->position += data_size_type();
		object->real_value = *(double*)object->position;
		object->position += sizeof(double);
		arg->data = &object->real_value;
		arg->data_size = sizeof(double);
		break;

	// int
	case MN_INTV0 ... MN_INTV31:
		type = INDIGO_INT;
		object->int_value = *object->position - MN_INTV0;
		object->position += data_size_type();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_INT8:
		type = INDIGO_INT;
		object->int_value = *(int8_t*)(object->position + data_size_type());
		object->position += data_size8();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_INT16:
		type = INDIGO_INT;
		object->int_value = *(int16_t*)(object->position + data_size_type());
		object->position += data_size16();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_INT32:
		type = INDIGO_INT;
		object->int_value = *(int32_t*)(object->position + data_size_type());
		object->position += data_size32();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_INT64:
		type = INDIGO_INT;
		object->int_value = *(int64_t*)(object->position + data_size_type());
		object->position += data_size64();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;

	// array
	case MN_ARRAYV0 ... MN_ARRAYV31:
		type = INDIGO_ARRAY;
		object->int_value = *object->position - MN_ARRAYV0;
		object->position += data_size_type();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_ARRAY8:
		type = INDIGO_ARRAY;
		object->int_value = *(uint8_t*)(object->position + data_size_type());
		object->position += data_size8();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_ARRAY16:
		type = INDIGO_ARRAY;
		object->int_value = *(uint16_t*)(object->position + data_size_type());
		object->position += data_size16();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_ARRAY32:
		type = INDIGO_ARRAY;
		object->int_value = *(uint32_t*)(object->position + data_size_type());
		object->position += data_size32();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;

	// map
	case MN_MAPV0 ... MN_MAPV31:
		type = INDIGO_MAP;
		object->int_value = *object->position - MN_MAPV0;
		object->position += data_size_type();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_MAP8:
		type = INDIGO_MAP;
		object->int_value = *(uint8_t*)(object->position + data_size_type());
		object->position += data_size8();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_MAP16:
		type = INDIGO_MAP;
		object->int_value = *(uint16_t*)(object->position + data_size_type());
		object->position += data_size16();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_MAP32:
		type = INDIGO_MAP;
		object->int_value = *(uint32_t*)(object->position + data_size_type());
		object->position += data_size32();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;

	// string
	case MN_STRINGV0 ... MN_STRINGV31:
		type = INDIGO_STRING;
		arg->data_size = *object->position - MN_STRINGV0;
		object->position += data_size_type();
		arg->data = object->position;
		object->position += arg->data_size;
		break;
	case MN_STRING8:
		type = INDIGO_STRING;
		arg->data_size = *(uint8_t*)(object->position + data_size_type());
		object->position += data_size8();
		arg->data = object->position;
		object->position += arg->data_size;
		break;
	case MN_STRING16:
		type = INDIGO_STRING;
		arg->data_size = *(uint16_t*)(object->position + data_size_type());
		object->position += data_size16();
		arg->data = object->position;
		object->position += arg->data_size;
		break;
	case MN_STRING32:
		type = INDIGO_STRING;
		arg->data_size = *(uint32_t*)(object->position + data_size_type());
		object->position += data_size32();
		arg->data = object->position;
		object->position += arg->data_size;
		break;

	default:
		assert(0);
		break;
	}

	if (likely(data_type))
		*data_type = type;
	return true;
}

INDIGO_API void
indigo_rewind(indigo_object_t* object)
{
	object->position = msg_of(object->buf)->data;
}
