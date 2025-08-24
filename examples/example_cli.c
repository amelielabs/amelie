
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Public Domain.
//

#include <amelie.h>
#include <stdio.h>

//
// usage: example_cli ./test [amelie options]
//
// The example below creates a new or opens an existing amelie repository,
// continuous reads and executes SQL commands from stdin
// until EOF.
//

int
main(int argc, char* argv[])
{
	// example_cli <repository> [amelie options]
	if (argc == 1)
	{
		fprintf(stderr, "usage: %s <directory> [options]\n", argv[0]);
		return EXIT_FAILURE;
	}

	// create amelie environment
	amelie_t* env = amelie_init();
	if (! env)
	{
		fprintf(stderr, "amelie_init() failed\n");
		return EXIT_FAILURE;
	}

	// open or create the database repository, passing the arguments
	// from the command line
	int rc = amelie_open(env, argv[1], argc - 2, argv + 2);
	if (rc == -1)
	{
		amelie_free(env);
		fprintf(stderr, "amelie_open() failed\n");
		return EXIT_FAILURE;
	}

	// create session
	amelie_session_t* session = amelie_connect(env, NULL);
	if (! session)
	{
		amelie_free(env);
		fprintf(stderr, "amelie_connect() failed\n");
		return EXIT_FAILURE;
	}

	// read and process SQL commands from stdin until EOF
	char command[256];
	for (;;)
	{
		printf("> ");
		if (! fgets(command, sizeof(command), stdin))
			break;
		
		// cut the \n symbol at the end of input, this is not necessary and
		// used only for better debug output
		for (char* p = command; *p; p++) {
			if (*p == '\n') {
				*p = 0;
				break;
			}
		}

		// create request and schedule the command execution,
		// the command buffer pointer must be valid until the result completion
		amelie_request_t* req = amelie_execute(session, command, 0, NULL, NULL, NULL);
		if (! req)
		{
			amelie_free(session);
			amelie_free(env);
			fprintf(stderr, "amelie_execute() failed\n");
			return EXIT_FAILURE;
		}

		// wait for request completion (the result argument can be NULL)
		amelie_arg_t result = {NULL, 0};
		rc = amelie_wait(req, -1, &result);
		if (rc == -1) {
			// indicates error, the result likely contains the error message
		}
		if (result.data)
			printf("%.*s\n", (int)result.data_size, result.data);

		// free the request
		amelie_free(req);
	}

	// shutdown
	amelie_free(session);
	amelie_free(env);
	return 0;
}
