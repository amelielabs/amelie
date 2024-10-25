
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

#include <amelie_private.h>
#include <amelie_test.h>

TestSession*
test_session_new(TestSuite* self, TestEnv* env, const char* name)
{
	TestSession* session = malloc(sizeof(*session));
	if (session == NULL)
		return NULL;
	session->name = strdup(name);
	if (session->name == NULL) {
		free(session);
		return NULL;
	}
	session->headers = NULL;
	session->env = env;
	list_init(&session->link);

	session->handle = curl_easy_init();
	if (session->handle == NULL)
	{
		free(session);
		return NULL;
	}
	struct curl_slist* headers = NULL;
	headers = curl_slist_append(headers, "Content-Type: text/plain");
	session->headers = headers;

	list_append(&self->list_session, &session->link);
	env->sessions++;
	return session;
}

void
test_session_free(TestSuite* self, TestSession* session)
{
	unused(self);
	list_unlink(&session->link);
	session->env->sessions--;
	assert(session->env->sessions >= 0);
	if (session->handle)
		curl_easy_cleanup(session->handle);
	if (session->headers)
		curl_slist_free_all(session->headers);
	free(session->name);
	free(session);
}

TestSession*
test_session_find(TestSuite* self, const char* name)
{
	list_foreach(&self->list_session)
	{
		auto session = list_at(TestSession, link);
		if (strcasecmp(name, session->name) == 0)
			return session;
	}
	return NULL;
}

static size_t
test_session_on_write(void* ptr, size_t len, size_t nmemb, void* arg)
{
	TestSuite* self = arg;
	test_log(self, "%.*s\n", (int)(len * nmemb), ptr);
	return len * nmemb;
}

void
test_session_connect(TestSuite* self, TestSession* session,
                     const char* uri,
                     const char* cafile)
{
	auto curl = session->handle;
	curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0);

	CURLcode code;
	code = curl_easy_setopt(curl, CURLOPT_URL, uri);
	if (code != CURLE_OK)
	{
		test_error(self, "line %d: connect: failed to set uri",
		           self->current_line);
		return;
	}
	if (cafile)
	{
		code = curl_easy_setopt(curl, CURLOPT_CAINFO, cafile);
		if (code != CURLE_OK)
		{
			test_error(self, "line %d: connect: failed to set cafile",
			           self->current_line);
			return;
		}
	}

	/*curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);*/
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, session->headers);
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, test_session_on_write);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, self);
}

void
test_session_execute(TestSuite* self, TestSession* session, const char* query)
{
	auto curl = session->handle;
	curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)strlen(query));
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, query);

	CURLcode code;
	code = curl_easy_perform(curl);
	if (code == CURLE_OK)
	{
		/*test_log(self, "\n");*/
		return;
	}

	const char* str = curl_easy_strerror(code);
	if (code == CURLE_HTTP_RETURNED_ERROR)
	{
		long http_code = 0;
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
		test_log(self, "error: HTTP %d (%s)\n", (int)http_code, str);
	}  else
	{
		test_log(self, "error: %s\n", str);
	}
}

void
test_session_import(TestSuite*  self, TestSession* session,
                    const char* content_type,
                    const char* data)
{
	auto curl = session->handle;
	curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)strlen(data));
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data);

	char type[256];
	snprintf(type, sizeof(type), "Content-Type: %s", content_type);
	auto headers = curl_slist_append(NULL, type);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	CURLcode code;
	code = curl_easy_perform(curl);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, session->headers);
	curl_slist_free_all(headers);

	if (code == CURLE_OK)
	{
		/*test_log(self, "\n");*/
		return;
	}

	const char* str = curl_easy_strerror(code);
	if (code == CURLE_HTTP_RETURNED_ERROR)
	{
		long http_code = 0;
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
		test_log(self, "error: HTTP %d (%s)\n", (int)http_code, str);
	}  else
	{
		test_log(self, "error: %s\n", str);
	}
}
