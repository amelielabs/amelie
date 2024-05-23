
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata.h>
#include <curl/curl.h>

static size_t
client_on_write(char* ptr, size_t len, size_t nmemb, void* arg)
{
	printf("%.*s\n", (int)(len * nmemb), ptr);
	return len * nmemb;
}

static int
client(int argc, char** argv)
{
	CURL* curl = curl_easy_init();
	if (curl == NULL)
		return EXIT_FAILURE;

	curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0);

	CURLcode code;
	code = curl_easy_setopt(curl, CURLOPT_URL, "localhost:3485");
	if (code != CURLE_OK)
	{
		curl_easy_cleanup(curl);
		return EXIT_FAILURE;
	}

	char text[1024];
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	/*curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);*/
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, client_on_write);

	struct curl_slist* headers = NULL;
	headers = curl_slist_append(headers, "Content-Type: text/plain");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	for (;;)
	{
		if (isatty(fileno(stdin)))
		{
			printf("> ");
			fflush(stdout);
		}
		if (! fgets(text, sizeof(text), stdin))
			break;

		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, text);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)strlen(text));

		code = curl_easy_perform(curl);
		if (code != CURLE_OK)
		{
			const char* str = curl_easy_strerror(code);
			if (code == CURLE_HTTP_RETURNED_ERROR)
			{
				long http_code = 0;
				curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
				printf("error: HTTP %d (%s)\n", (int)http_code, str);
			}  else
			{
				printf("error: %s\n", str);
				break;
			}
		}
	}

	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);
	return EXIT_SUCCESS;
}

static int
server(int argc, char** argv)
{
	Main main;
	main_init(&main);

	Str directory;
	str_init(&directory);
	str_set_cstr(&directory, "./t");

	Str config;
	str_init(&config);
	char options[] = "{ \"log_to_stdout\": true, \"listen\": [{ \"host\": \"*\", \"port\": 3485 }], \"frontends\": 3, \"shards\": 6 }";
	str_set_cstr(&config, options);

	int rc = main_start(&main, &directory, &config);
	if (rc == -1)
	{
		main_stop(&main);
		main_free(&main);
		return EXIT_FAILURE;
	}

	client(argc, argv);
	/*
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
	}
	*/

	main_stop(&main);
	main_free(&main);
	return EXIT_SUCCESS;
}

int
main(int argc, char* argv[])
{
	if (argc >= 2 && !strcmp(argv[1], "server"))
		return server(argc, argv);

	return client(argc, argv);
}
