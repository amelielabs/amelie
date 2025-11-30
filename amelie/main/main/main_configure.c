
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
#include <amelie_main.h>

void
main_configure(Main* self, Opts* opts)
{
	auto endpoint = &self->endpoint;
	auto argc     = self->argc;
	auto argv     = self->argv;

	if (argc == 0)
		error("path, uri or bookmark expected");

	// [path, uri or bookmark]
	int arg = 0;
	if (!strncmp(argv[0], ".", 1) ||
	    !strncmp(argv[0], "/", 1))
	{
		// directory path
		Str str;
		str_set_cstr(&str, argv[0]);
		opt_string_set(&endpoint->path, &str);
		arg = 1;
	} else
	if (!strncmp(argv[0], "http://", 7) ||
	    !strncmp(argv[0], "https://", 8))
	{
		// parse uri
		Str uri;
		str_set_cstr(&uri, argv[0]);
		uri_parse(endpoint, &uri);
		arg = 1;
	} else
	if (strncmp(argv[0], "--", 2) != 0)
	{
		// find bookmark by name
		Str name;
		str_set_cstr(&name, argv[0]);
		auto match = bookmark_mgr_find(&self->bookmark_mgr, &name);
		if (match)
		{
			// use bookmark settings
			endpoint_copy(endpoint, &match->endpoint);
		} else
		{
			// use as directory path
			opt_string_set(&endpoint->path, &name);
		}
		arg = 1;
	}

	// [endpoint options | options] ...

	// --<option>=<value> | name
	for (; arg < argc; arg++)
	{
		// stop of first non -- argument
		if (strncmp(argv[arg], "--", 2) != 0)
			break;

		Str name;
		Str value;
		if (arg_parse(argv[arg], &name, &value) == -1)
			error("invalid argument '%s'", argv[arg]);

		if (str_empty(&value))
			error("argument '%.*s' value is missing", str_size(&name),
			      str_of(&name));

		// --json={options}
		if (str_is_cstr(&name, "json"))
		{
			opts_set(&endpoint->opts, &value);
			continue;
		}

		// find endpoint or supplied options
		auto opt = opts_find(&endpoint->opts, &name);
		if (! opt)
		{
			if (opts)
				opt = opts_find(opts, &name);
		}
		if (opt)
		{
			opt_set(opt, &value);
			continue;
		}

		// stop on first unknown option
		break;
	}

	// validate connection string
	auto uri  = opt_string_of(&endpoint->uri);
	auto path = opt_string_of(&endpoint->path);
	if (str_empty(uri) && str_empty(path))
		error("uri or path is not defined");

	auto db   = opt_string_of(&endpoint->db);
	if (str_empty(db))
		error("database name is not defined");

	// generate auth token
	auto user   = opt_string_of(&endpoint->user);
	auto secret = opt_string_of(&endpoint->secret);
	if (!str_empty(user) || !str_empty(secret))
	{
		if (str_empty(user) || str_empty(secret))
			error("bookmark: both user and secret options are expected");

		// create jwt one-time token

		// set expire timestamp to 1 year
		Timestamp expire;
		timestamp_init(&expire);
		timestamp_set_unixtime(&expire, time_us());
		Interval iv;
		interval_init(&iv);
		Str str;
		str_set_cstr(&str, "1 year");
		interval_set(&iv, &str);
		timestamp_add(&expire, &iv);

		auto jwt = jwt_create(user, secret, &expire);
		defer_buf(jwt);
		Str jwt_str;
		buf_str(jwt, &jwt_str);
		opt_string_set(&endpoint->token, &jwt_str);

		// reset user and secret, keep only token
		str_free(user);
		str_free(secret);
	}

	// point to the db options
	main_advance(self, arg);
}
