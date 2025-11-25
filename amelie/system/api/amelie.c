
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
	int     type;
	Runtime runtime;
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
	self->type = AMELIE_OBJ;
	runtime_init(&self->runtime);
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
		runtime_stop(&self->runtime);
		runtime_free(&self->runtime);
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

static void
amelie_main(void* arg, int argc, char** argv)
{
	Repo repo;
	repo_init(&repo);
	defer(repo_close, &repo);

	System* system = NULL;
	auto on_error = error_catch
	(
		// create or open repository
		auto directory = arg;
		repo_open(&repo, directory, argc, argv);

		// create system object
		system = system_create();
		system_start(system, repo.bootstrap);

		// notify start completion
		cond_signal(&am_task->status, RUNTIME_OK);

		// handle system requests
		system_main(system);
	);
	if (system)
	{
		system_stop(system);
		system_free(system);
	}

	if (on_error)
		rethrow();
}

AMELIE_API int
amelie_open(amelie_t* self, const char* path, int argc, char** argv)
{
	// ensure amelie_open() was not called previously
	if (unlikely(runtime_started(&self->runtime)))
		return -1;
	auto status = runtime_start(&self->runtime, amelie_main, (void*)path,
	                            argc, argv);
	return status == RUNTIME_OK? 0: -1;
}

AMELIE_API amelie_session_t*
amelie_connect(amelie_t* self, const char* uri)
{
	// ensure amelie_open() executed
	if (unlikely(! runtime_started(&self->runtime)))
		return NULL;

	// if amelie started without repository, ensure that the
	// client has the uri argument set
	if (str_empty(opt_string_of(&self->runtime.state.directory)))
		if (! uri)
			return NULL;

	auto session = (amelie_session_t*)am_malloc(sizeof(amelie_session_t));
	session->type            = AMELIE_OBJ_SESSION;
	session->amelie          = self;
	session->req_cache_count = 0;
	list_init(&session->req_cache);
	native_init(&session->native);
	native_set_uri(&session->native, (char*)uri);

	// prepare connect event before the native registration
	Request req;
	request_init(&req);
	req.type = REQUEST_CONNECT;
	request_queue_push(&session->native.queue, &req, false);

	// register native client in the frontend pool
	task_send(&self->runtime.task, &session->native.msg);

	// wait for completion
	request_wait(&req, -1);
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
	unused(argc);
	unused(argv);

	// ensure amelie_open() executed
	if (unlikely(! runtime_started(&self->amelie->runtime)))
		return NULL;

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

	// schedule for execution
	request_queue_push(&self->native.queue, &req->request, true);
	return req;
}

AMELIE_API int
amelie_wait(amelie_request_t* self, uint32_t time_ms, amelie_arg_t* result)
{
	// 408 Request Timeout
	if (! request_wait(&self->request, time_ms))
		return 408;
	if (result)
	{
		result->data = buf_cstr(&self->request.content);
		result->data_size = buf_size(&self->request.content);
	}
	return self->request.code;
}
