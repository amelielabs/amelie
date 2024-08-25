
//
// amelie.
//
// Real-Time SQL Database.
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
	guard(buf_free, &buf);
	file_import(&buf, "%s", path);

	Str text;
	str_init(&text);
	buf_str(&buf, &text);

	Json json;
	json_init(&json);
	guard(json_free, &json);
	json_parse(&json, global()->timezone, &text, NULL);

	// {}
	uint8_t* pos = json.buf->start;
	data_read_map(&pos);
	while (! data_read_map_end(&pos))
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
	guard(buf_free, &buf);

	// {}
	encode_map(&buf);
	list_foreach(&self->list)
	{
		auto login = list_at(Login, link);
		encode_string(&buf, remote_get(&login->remote, REMOTE_NAME));
		login_write(login, &buf);
	}
	encode_map_end(&buf);

	// convert to json
	Buf text;
	buf_init(&text);
	guard(buf_free, &text);
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
	json_parse(&json, NULL, text, NULL);

	uint8_t* pos = json.buf->start;

	data_read_map(&pos);
	while (! data_read_map_end(&pos))
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
			if (str_compare_cstr(&name, remote_nameof(id)))
				break;
		if (id == REMOTE_MAX)
			error("login: unknown option '%.*s'", str_size(&name),
			      str_of(&name));

		remote_set(remote, id, &value);
	}
}

void
login_mgr_set(LoginMgr* self, Remote* remote, int argc, char** argv)
{
	// [name] [remote options]
	if (argc == 0)
		return;

	// name
	if (strncmp(argv[0], "--", 2) != 0)
	{
		Str name;
		str_set_cstr(&name, argv[0]);
		auto match = login_mgr_find(self, &name);
		if (! match)
			error("login: login name '%s' not found", argv[0]);
		remote_copy(remote, &match->remote);
		argc--;
		argv++;
	}

	// --<option>=<value>
	for (auto i = 0; i < argc; i++)
	{
		Str name;
		Str value;
		if (arg_parse(argv[i], &name, &value) == -1)
			error("login: invalid option '%s'", argv[i]);

		if (str_empty(&value))
			error("login: value is missing for option '%.*s'", str_size(&name),
			      str_of(&name));

		// --json={options}
		if (str_compare_cstr(&name, "json"))
		{
			login_mgr_set_json(remote, &value);
			continue;
		}

		int id = 0;
		for (; id < REMOTE_MAX; id++)
			if (str_compare_cstr(&name, remote_nameof(id)))
				break;
		if (id == REMOTE_MAX)
			error("login: unknown option '%.*s'", str_size(&name),
			      str_of(&name));

		remote_set(remote, id, &value);
	}

	// validate uri
	auto uri = remote_get(remote, REMOTE_URI);
	if (str_empty(uri))
		error("login: uri is not defined");

	// generate auth token
	auto user   = remote_get(remote, REMOTE_USER);
	auto secret = remote_get(remote, REMOTE_SECRET);
	if (!str_empty(user) || !str_empty(secret))
	{
		if (str_empty(user) || str_empty(secret))
			error("login: both user and secret options are expected");

		// create jwt token
		auto jwt = jwt_create(user, secret, NULL);
		guard_buf(jwt);
		Str jwt_str;
		buf_str(jwt, &jwt_str);
		remote_set(remote, REMOTE_TOKEN, &jwt_str);

		// reset user and secret, keep only token
		str_free(user);
		str_free(secret);
	}
}
