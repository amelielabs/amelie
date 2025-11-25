
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

static inline void
main_configure_by_json(Main* self, Str* text)
{
	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, text, NULL);

	auto pos = json.buf->start;
	json_read_obj(&pos);
	while (! json_read_obj_end(&pos))
	{
		// key
		Str name;
		json_read_string(&pos, &name);

		// value
		if (! json_is_string(pos))
			error("json argument '%.*s' string value expected",
			      str_size(&name), str_of(&name));
		Str value;
		json_read_string(&pos, &value);

		// find option
		int id = 0;
		for (; id < REMOTE_MAX; id++)
			if (str_is_cstr(&name, remote_nameof(id)))
				break;
		if (id == REMOTE_MAX)
			error("unknown json argument '%.*s'", str_size(&name),
			      str_of(&name));

		remote_set(&self->remote, id, &value);
	}
}

void
main_configure(Main* self, Opts* opts)
{
	auto remote = &self->remote;
	auto argc   = self->argc;
	auto argv   = self->argv;

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
		remote_set(remote, REMOTE_PATH, &str);
		arg = 1;
	} else
	if (!strncmp(argv[0], "http://", 7) ||
	    !strncmp(argv[0], "https://", 8))
	{
		// parse uri
		Str uri_str;
		str_set_cstr(&uri_str, argv[0]);
		Uri uri;
		uri_init(&uri);
		defer(uri_free, &uri);
		uri_set(&uri, &uri_str, false);

		// convert uri into remote options
		uri_export(&uri, remote);
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
			remote_copy(remote, &match->remote);
		} else
		{
			// use as directory path
			remote_set(remote, REMOTE_PATH, &name);
		}
		arg = 1;
	}

	// [remote options | options] ...

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
			main_configure_by_json(self, &value);
			continue;
		}

		// handle remote options
		auto id = remote_idof(&name);
		if (id != -1)
		{
			remote_set(remote, id, &value);
			continue;
		}

		if (opts)
		{
			auto opt = opts_find(opts, &name);
			if (opt)
			{
				opt_set(opt, &value);
				continue;
			}
		}

		// stop on first unknown option
		break;
	}

	// validate uri
	auto uri  = remote_get(remote, REMOTE_URI);
	auto path = remote_get(remote, REMOTE_PATH);
	if (str_empty(uri) && str_empty(path))
		error("uri or path is not defined");

	// generate auth token
	auto user   = remote_get(remote, REMOTE_USER);
	auto secret = remote_get(remote, REMOTE_SECRET);
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
		remote_set(remote, REMOTE_TOKEN, &jwt_str);

		// reset user and secret, keep only token
		str_free(user);
		str_free(secret);
	}

	// point to the db options
	main_advance(self, arg);
}
