
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
	//
	// The directory argument can be set to NULL, in such case no repository
	// directory will be used or created and the sessions can be used only
	// for a remote connections.
	//
	int rc = amelie_open(env, argv[1], argc - 2, argv + 2);
	if (rc == -1)
	{
		amelie_free(env);
		fprintf(stderr, "amelie_open() failed\n");
		return EXIT_FAILURE;
	}

	// create session
	//
	// There are two cases to consider for amelie_connect():
	//
	// 1) without URI argument (set to NULL)
	//
	//    Session will be connected to the local embeddable database
	//    using optimized path which avoids network io.
	//
	// 2) with URI argument
	//
	//    Session will be connected to a remote server specified by the URI
	//    acting as a remote client.
	//
	//    In this example it connects to the server created by this
	//    database using http.
	//
	//    Note that the actual connection happens on the first request,
	//    (this also allows it be handled using async API).
	//
	amelie_session_t* session = amelie_connect(env, "http://localhost");
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
		//
		// amelie_wait() returns http code
		//
		amelie_arg_t result = {NULL, 0};
		int status = amelie_wait(req, -1, &result);
		if (status != 200 &&
		    status != 204) {
			// indicates error, the result likely contains the error message
		}

		// the result data pointers are valid until the request
		// object is freed
		if (result.data_size > 0)
			printf("%.*s\n", (int)result.data_size, result.data);

		// free the request
		amelie_free(req);
	}

	// shutdown
	amelie_free(session);
	amelie_free(env);
	return 0;
}
