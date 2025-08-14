
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

#include <amelie_core.h>
#include <amelie.h>

enum
{
	AMELIE_OBJ         = 0x0323410L,
	AMELIE_OBJ_SESSION = 0x04A3455L,
	AMELIE_OBJ_REQUEST = 0x02BF130L,
	AMELIE_OBJ_FREE    = 0x0f0f0f0L
};

struct amelie
{
	int      type;
	System*  system;
	Instance instance;
	Task     task;
};

struct amelie_session
{
	int       type;
	Native    native;
	List      req_cache;
	int       req_cache_count;
	amelie_t* amelie;
};

struct amelie_request
{
	int               type;
	Request           request;
	amelie_session_t* session;
	List              link;
};

AMELIE_API amelie_t*
amelie_init(void)
{
	auto self = (amelie_t*)am_malloc(sizeof(amelie_t));
	self->type   = AMELIE_OBJ;
	self->system = NULL;
	instance_init(&self->instance);
	task_init(&self->task);
	return self;
}

AMELIE_API void
amelie_free(void* ptr)
{
	switch (*(int*)ptr) {
	case AMELIE_OBJ:
	{
		// all sessions must be closed at this point
		amelie_t* self = ptr;
		if (task_active(&self->task))
		{
			auto buf = msg_create_as(&self->instance.buf_mgr, RPC_STOP, 0);
			channel_write(&self->task.channel, buf);
			task_wait(&self->task);
		}
		task_free(&self->task);
		break;
	}
	case AMELIE_OBJ_SESSION:
	{
		// all requests must be completed at this point
		amelie_session_t* self = ptr;
		Request req;
		request_init(&req);
		req.type = REQUEST_DISCONNECT;
		request_queue_push(&self->native.queue, &req, true);
		request_wait(&req, -1);
		request_free(&req);
		native_free(&self->native);

		// cleanup request cache
		list_foreach_safe(&self->req_cache)
		{
			auto req = list_at(amelie_request_t, link);
			request_free(&req->request);
			am_free(req);
		}
		break;
	}
	case AMELIE_OBJ_REQUEST:
	{
		// place request object to the session request cache
		amelie_request_t* self = ptr;
		assert(self->request.complete);
		auto session = self->session;
		list_append(&session->req_cache, &self->link);
		session->req_cache_count++;
		self->type = AMELIE_OBJ_FREE;
		return;
	}
	case AMELIE_OBJ_FREE:
	{
		fprintf(stderr, "\n%s(%p): attempt to use freed object\n",
		        source_function, ptr);
		fflush(stderr);
		abort();
		break;
	}
	default:
	{
		fprintf(stderr, "\n%s(%p): unrecognized object type\n",
		        source_function, ptr);
		fflush(stderr);
		abort();
		break;
	}
	}
	*(int*)ptr = AMELIE_OBJ_FREE;
	am_free(ptr);
}

typedef struct
{
	amelie_t* self;
	char*     path;
	int       argc;
	char**    argv;
} AmelieArgs;

static void
amelie_main(void* arg)
{
	AmelieArgs* args = arg;
	auto self = args->self;
	auto on_error = error_catch
	(
		// start instance
		instance_start(&self->instance);
		auto bootstrap = instance_open(&self->instance, args->path,
		                               args->argc,
		                               args->argv);
		// create system object
		self->system = system_create();
		system_start(self->system, bootstrap);

		// notify cli_start about start completion
		cond_signal(&self->task.status, 0);

		// handle system requests
		system_main(self->system);
	);

	auto system = self->system;
	if (system)
	{
		system_stop(system);
		system_free(system);
		self->system = NULL;
	}
	instance_stop(&self->instance);
	instance_free(&self->instance);

	// complete
	int rc = 0;
	if (on_error)
		rc = -1;
	cond_signal(&self->task.status, rc);
}

AMELIE_API int
amelie_open(amelie_t* self, const char* path, int argc, char** argv)
{
	if (unlikely(self->system))
	{
		fprintf(stderr, "\n%s(%p): double open\n",
		        source_function, self);
		fflush(stderr);
		abort();
	}

	// start system task
	AmelieArgs args =
	{
		.self = self,
		.path = (char*)path,
		.argc = argc,
		.argv = argv
	};
	int rc;
	rc = task_create_nothrow(&self->task, "main", amelie_main, &args,
	                         &self->instance.global, NULL,
	                         logger_write, &self->instance.logger,
	                         &self->instance.buf_mgr);
	if (unlikely(rc == -1))
		return -1;

	// wait for the start completion
	rc = cond_wait(&self->task.status);
	return rc;
}

AMELIE_API amelie_session_t*
amelie_connect(amelie_t* self, const char* uri)
{
	// reserved as a feature
	unused(uri);

	auto session = (amelie_session_t*)am_malloc(sizeof(amelie_session_t));
	session->type            = AMELIE_OBJ_SESSION;
	session->amelie          = self;
	session->req_cache_count = 0;
	list_init(&session->req_cache);
	native_init(&session->native);

	// prepare connect event before the native registration
	Request req;
	request_init(&req);
	req.type = REQUEST_CONNECT;
	request_queue_push(&session->native.queue, &req, false);

	// register native client in the frontend pool
	auto buf = msg_create_as(&self->instance.buf_mgr, MSG_NATIVE, 0);
	auto native_ptr = &session->native;
	buf_write(buf, &native_ptr, sizeof(void**));
	msg_end(buf);
	frontend_mgr_forward(&self->system->frontend_mgr, buf);

	// wait for completion
	request_wait(&req, -1);
	assert(! req.error);
	request_free(&req);
	return session;
}

hot AMELIE_API amelie_request_t*
amelie_execute(amelie_session_t*    self,
               const char*          command,
               int                  argc,
               amelie_arg_t*        argv,
               amelie_on_complete_t on_complete,
               void*                on_complete_arg)
{
	// allocate request
	amelie_request_t* req;
	if (likely(self->req_cache_count > 0))
	{
		req = container_of(list_pop(&self->req_cache), amelie_request_t, link);
		self->req_cache_count--;
		request_reset(&req->request);
	} else
	{
		req = am_malloc(sizeof(amelie_request_t));
		req->session = self;
		request_init(&req->request);
		list_init(&req->link);
	}

	// prepare request
	req->type = AMELIE_OBJ_REQUEST;
	str_set_cstr(&req->request.cmd, command);
	req->request.on_complete     = (RequestNotify)on_complete;
	req->request.on_complete_arg = on_complete_arg;
	(void)argc;
	(void)argv;

	// schedule for execution
	request_queue_push(&self->native.queue, &req->request, true);
	return req;
}

AMELIE_API int
amelie_wait(amelie_request_t* self, uint32_t time_ms, amelie_arg_t* result)
{
	if (! request_wait(&self->request, time_ms))
		return 1;
	if (result)
	{
		result->data = buf_cstr(&self->request.content);
		result->data_size = buf_size(&self->request.content);
	}
	return self->request.error ? -1 : 0;
}
