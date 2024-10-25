
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

void
login_mgr_init(LoginMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

void
login_mgr_free(LoginMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto login = list_at(Login, link);
		login_free(login);
	}
}

void
login_mgr_open(LoginMgr* self, const char* path)
{
	if (! fs_exists("%s", path))
		return;

	Buf buf;
	buf_init(&buf);
	guard_buf(&buf);
	file_import(&buf, "%s", path);

	Str text;
	str_init(&text);
	buf_str(&buf, &text);

	Json json;
	json_init(&json);
	guard(json_free, &json);
	json_parse(&json, &text, NULL);

	// {}
	uint8_t* pos = json.buf->start;
	data_read_obj(&pos);
	while (! data_read_obj_end(&pos))
	{
		// name
		data_skip(&pos);

		// value
		auto login = login_read(&pos);
		login_mgr_add(self, login);
	}
}

void
login_mgr_sync(LoginMgr* self, const char* path)
{
	if (fs_exists("%s", path))
		fs_unlink("%s", path);

	// write a list of remote login_mgr
	Buf buf;
	buf_init(&buf);
	guard_buf(&buf);

	// {}
	encode_obj(&buf);
	list_foreach(&self->list)
	{
		auto login = list_at(Login, link);
		encode_string(&buf, remote_get(&login->remote, REMOTE_NAME));
		login_write(login, &buf);
	}
	encode_obj_end(&buf);

	// convert to json
	Buf text;
	buf_init(&text);
	guard_buf(&text);
	uint8_t* pos = buf.start;
	json_export_pretty(&text, NULL, &pos);

	// create file
	File file;
	file_init(&file);
	guard(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0600);
	file_write_buf(&file, &text);

	// sync
	file_sync(&file);
}

void
login_mgr_add(LoginMgr* self, Login* remote)
{
	list_append(&self->list, &remote->link);
	self->list_count++;
}

void
login_mgr_delete(LoginMgr* self, Str* name)
{
	auto remote = login_mgr_find(self, name);
	if (! remote)
		return;
	list_unlink(&remote->link);
	self->list_count--;
	login_free(remote);
}

Login*
login_mgr_find(LoginMgr* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto login = list_at(Login, link);
		if (str_compare(remote_get(&login->remote, REMOTE_NAME), name))
			return login;
	}
	return NULL;
}

static inline void
login_mgr_set_json(Remote* remote, Str* text)
{
	Json json;
	json_init(&json);
	guard(json_free, &json);
	json_parse(&json, text, NULL);

	uint8_t* pos = json.buf->start;

	data_read_obj(&pos);
	while (! data_read_obj_end(&pos))
	{
		// key
		Str name;
		data_read_string(&pos, &name);

		// value
		if (! data_is_string(pos))
			error("login: option '%.*s' string value expected",
			      str_size(&name), str_of(&name));
		Str value;
		data_read_string(&pos, &value);

		// find option
		int id = 0;
		for (; id < REMOTE_MAX; id++)
			if (str_is_cstr(&name, remote_nameof(id)))
				break;
		if (id == REMOTE_MAX)
			error("login: unknown option '%.*s'", str_size(&name),
			      str_of(&name));

		remote_set(remote, id, &value);
	}
}

int
login_mgr_set(LoginMgr* self, Remote* remote, Vars* vars,
              int       argc,
              char**    argv)
{
	// [path or name] [remote options | options] ...
	if (argc == 0)
		return 0;

	int arg = 0;
	if (!strncmp(argv[0], ".", 1) ||
	    !strncmp(argv[0], "/", 1))
	{
		// directory path
		char path[PATH_MAX];
		snprintf(path, sizeof(path), "%s/amelie.sock", argv[0]);
		Str str;
		str_set_cstr(&str, path);
		remote_set(remote, REMOTE_PATH, &str);
		arg = 1;
	} else
	if (strncmp(argv[0], "--", 2) != 0)
	{
		// login name
		Str name;
		str_set_cstr(&name, argv[0]);
		auto match = login_mgr_find(self, &name);
		if (! match)
			error("login: login name '%s' not found", argv[0]);
		remote_copy(remote, &match->remote);
		arg = 1;
	}

	// --<option>=<value> | name
	for (; arg < argc; arg++)
	{
		// stop of first non -- argument
		if (strncmp(argv[arg], "--", 2) != 0)
			break;

		Str name;
		Str value;
		if (arg_parse(argv[arg], &name, &value) == -1)
			error("login: invalid option '%s'", argv[arg]);

		if (str_empty(&value))
			error("login: value is missing for option '%.*s'", str_size(&name),
			      str_of(&name));

		// --json={options}
		if (str_is_cstr(&name, "json"))
		{
			login_mgr_set_json(remote, &value);
			continue;
		}

		int id = 0;
		for (; id < REMOTE_MAX; id++)
			if (str_is_cstr(&name, remote_nameof(id)))
				break;
		if (id != REMOTE_MAX)
		{
			remote_set(remote, id, &value);
			continue;
		}

		if (vars)
		{
			auto var = vars_find(vars, &name);
			if (var)
			{
				var_set(var, &value);
				continue;
			}
		}

		error("login: unknown option '%.*s'", str_size(&name),
		      str_of(&name));
	}

	// validate uri
	auto uri  = remote_get(remote, REMOTE_URI);
	auto path = remote_get(remote, REMOTE_PATH);
	if (str_empty(uri) && str_empty(path))
		error("login: uri or unix socket path is not defined");

	// generate auth token
	auto user   = remote_get(remote, REMOTE_USER);
	auto secret = remote_get(remote, REMOTE_SECRET);
	if (!str_empty(user) || !str_empty(secret))
	{
		if (str_empty(user) || str_empty(secret))
			error("login: both user and secret options are expected");

		// create jwt one-time token

		// set expire timestamp to 1 year
		Timestamp expire;
		timestamp_init(&expire);
		timestamp_read_value(&expire, time_us());
		Interval iv;
		interval_init(&iv);
		Str str;
		str_set_cstr(&str, "1 year");
		interval_read(&iv, &str);
		timestamp_add(&expire, &iv);

		auto jwt = jwt_create(user, secret, &expire);
		guard_buf(jwt);
		Str jwt_str;
		buf_str(jwt, &jwt_str);
		remote_set(remote, REMOTE_TOKEN, &jwt_str);

		// reset user and secret, keep only token
		str_free(user);
		str_free(secret);
	}

	return arg;
}
