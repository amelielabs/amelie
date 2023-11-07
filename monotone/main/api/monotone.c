
//
// monotone
//
// SQL OLTP database
//

#include <monotone_internal.h>
#include <monotone.h>

enum {
	MONO_OBJ         = 0x3fb15941,
	MONO_OBJ_OBJECT  = 0x14032890,
	MONO_OBJ_SESSION = 0x43132510,
	MONO_OBJ_FREED   = 0x28101019
};

struct mono
{
	int  type;
	Main main;
};

struct mono_session
{
	int     type;
	Native  native;
	mono_t* mono;
};

struct mono_object
{
	int             type;
	uint8_t*        position;
	uint8_t*        end;
	Buf*            buf;
	int64_t         int_value;
	double          real_value;
	mono_session_t* session;
};

MONO_API mono_t*
mono_init(void)
{
	mono_t* mono = malloc(sizeof(mono_t));
	if (unlikely(mono == NULL))
		return NULL;
	mono->type = MONO_OBJ;
	main_init(&mono->main);
	return mono;
}

MONO_API int
mono_open(mono_t* mono, const char* directory, const char* config)
{
	Str arg_directory;
	Str arg_config;
	str_init(&arg_directory);
	str_init(&arg_config);
	if (directory)
		str_set_cstr(&arg_directory, directory);
	if (config)
		str_set_cstr(&arg_config, config);
	return main_start(&mono->main, &arg_directory, &arg_config);
}

MONO_API void
mono_free(void* ptr)
{
	switch (*(int*)ptr) {
	case MONO_OBJ:
	{
		mono_t* mono = ptr;
		main_stop(&mono->main);
		main_free(&mono->main);
		break;
	}
	case MONO_OBJ_SESSION:
	{
		mono_session_t* session = ptr;
		auto main = &session->mono->main;
		native_close(&session->native, &main->buf_cache);
		native_free(&session->native);
		break;
	}
	case MONO_OBJ_OBJECT:
	{
		mono_object_t* object = ptr;
		if (object->buf)
			buf_cache_push(object->buf->cache, object->buf);
		break;
	}
	case MONO_OBJ_FREED:
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

	*(int*)ptr = MONO_OBJ_FREED;
	free(ptr);
}

MONO_API mono_session_t*
mono_connect(mono_t* mono, const char* uri)
{
	auto main = &mono->main;

	// allocate session object
	mono_session_t* session;
	session = malloc(sizeof(mono_session_t));
	if (unlikely(session == NULL))
		return NULL;
	session->type   = MONO_OBJ_SESSION;
	session->mono = mono;

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

MONO_API int
mono_execute(mono_session_t* session, const char* command,
             int argc, mono_arg_t* argv)
{
	Str text;
	str_set_cstr(&text, command);
	return native_command(&session->native,
	                      &session->mono->main.buf_cache,
	                      &text,
	                      argc,
	                      (CommandArgPtr*)argv);
}

static inline mono_object_t*
mono_object_create(mono_session_t* session, Buf* buf)
{
	mono_object_t* object;
	object = malloc(sizeof(mono_object_t));
	if (unlikely(object == NULL))
		return NULL;
	object->type      = MONO_OBJ_OBJECT;
	object->session   = session;
	object->buf       = buf;
	object->position  = msg_of(buf)->data;
	object->end       = buf->position;
	object->int_value = 0;
	return object;
}

MONO_API mono_event_t
mono_read(mono_session_t* session, int timeout_ms, mono_object_t** result)
{
	auto buf = channel_read(&session->native.src, timeout_ms);
	if (buf == NULL)
		return MONO_TIMEDOUT;

	mono_event_t mono_event;
	auto msg = msg_of(buf);
	switch (msg->id) {
	case MSG_OK:
		mono_event = MONO_OK;
		break;
	case MSG_OBJECT:
		mono_event = MONO_OBJECT;
		break;
	case MSG_CONNECT:
		mono_event = MONO_CONNECT;
		break;
	case MSG_DISCONNECT:
		native_set_connected(&session->native, false);
		mono_event = MONO_DISCONNECT;
		break;
	case MSG_ERROR:
		mono_event = MONO_ERROR;
		break;
	default:
		assert(0);
		break;
	}

	if (result && (msg->id == MSG_ERROR || msg->id == MSG_OBJECT))
		*result = mono_object_create(session, buf);
	else
		buf_cache_push(buf->cache, buf);

	return mono_event;
}

MONO_API hot bool
mono_next(mono_object_t* object, mono_arg_t* arg, mono_type_t* data_type)
{
	if (likely(data_type))
		*data_type = 0;

	if (unlikely(object->position == NULL))
		return false;
	if (unlikely(object->position == object->end))
		return false;

	mono_type_t type;
	mono_arg_t  unused_;
	if (unlikely(arg == NULL))
		arg = &unused_;

	switch (*object->position) {
	// null
	case MN_NULL:
		type = MONO_NULL;
		object->position += data_size_type();
		arg->data = NULL;
		arg->data_size = 0;
		break;

	// bool
	case MN_TRUE:
		type = MONO_BOOL;
		object->int_value = true;
		object->position += data_size_type();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_FALSE:
		type = MONO_BOOL;
		object->int_value = false;
		object->position += data_size_type();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;

	// real
	case MN_REAL32:
		type = MONO_REAL;
		object->position += data_size_type();
		object->real_value = *(float*)object->position;
		object->position += sizeof(float);
		arg->data = &object->real_value;
		arg->data_size = sizeof(double);
		break;

	case MN_REAL64:
		type = MONO_REAL;
		object->position += data_size_type();
		object->real_value = *(double*)object->position;
		object->position += sizeof(double);
		arg->data = &object->real_value;
		arg->data_size = sizeof(double);
		break;

	// int
	case MN_INTV0 ... MN_INTV31:
		type = MONO_INT;
		object->int_value = *object->position - MN_INTV0;
		object->position += data_size_type();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_INT8:
		type = MONO_INT;
		object->int_value = *(int8_t*)(object->position + data_size_type());
		object->position += data_size8();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_INT16:
		type = MONO_INT;
		object->int_value = *(int16_t*)(object->position + data_size_type());
		object->position += data_size16();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_INT32:
		type = MONO_INT;
		object->int_value = *(int32_t*)(object->position + data_size_type());
		object->position += data_size32();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_INT64:
		type = MONO_INT;
		object->int_value = *(int64_t*)(object->position + data_size_type());
		object->position += data_size64();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;

	// array
	case MN_ARRAYV0 ... MN_ARRAYV31:
		type = MONO_ARRAY;
		object->int_value = *object->position - MN_ARRAYV0;
		object->position += data_size_type();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_ARRAY8:
		type = MONO_ARRAY;
		object->int_value = *(uint8_t*)(object->position + data_size_type());
		object->position += data_size8();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_ARRAY16:
		type = MONO_ARRAY;
		object->int_value = *(uint16_t*)(object->position + data_size_type());
		object->position += data_size16();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_ARRAY32:
		type = MONO_ARRAY;
		object->int_value = *(uint32_t*)(object->position + data_size_type());
		object->position += data_size32();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;

	// map
	case MN_MAPV0 ... MN_MAPV31:
		type = MONO_MAP;
		object->int_value = *object->position - MN_MAPV0;
		object->position += data_size_type();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_MAP8:
		type = MONO_MAP;
		object->int_value = *(uint8_t*)(object->position + data_size_type());
		object->position += data_size8();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_MAP16:
		type = MONO_MAP;
		object->int_value = *(uint16_t*)(object->position + data_size_type());
		object->position += data_size16();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;
	case MN_MAP32:
		type = MONO_MAP;
		object->int_value = *(uint32_t*)(object->position + data_size_type());
		object->position += data_size32();
		arg->data = &object->int_value;
		arg->data_size = sizeof(uint64_t);
		break;

	// string
	case MN_STRINGV0 ... MN_STRINGV31:
		type = MONO_STRING;
		arg->data_size = *object->position - MN_STRINGV0;
		object->position += data_size_type();
		arg->data = object->position;
		object->position += arg->data_size;
		break;
	case MN_STRING8:
		type = MONO_STRING;
		arg->data_size = *(uint8_t*)(object->position + data_size_type());
		object->position += data_size8();
		arg->data = object->position;
		object->position += arg->data_size;
		break;
	case MN_STRING16:
		type = MONO_STRING;
		arg->data_size = *(uint16_t*)(object->position + data_size_type());
		object->position += data_size16();
		arg->data = object->position;
		object->position += arg->data_size;
		break;
	case MN_STRING32:
		type = MONO_STRING;
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

MONO_API void
mono_rewind(mono_object_t* object)
{
	object->position = msg_of(object->buf)->data;
}
