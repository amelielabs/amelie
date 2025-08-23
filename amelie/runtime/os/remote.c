
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

#include <amelie_base.h>
#include <amelie_os.h>

typedef struct RemoteOpt RemoteOpt;

struct RemoteOpt
{
	int   id;
	char* name;
	int   name_size;
};

static RemoteOpt remote_opts[] =
{
	{ REMOTE_NAME,      "name",       4  },
	{ REMOTE_URI,       "uri",        3  },
	{ REMOTE_USER,      "user",       4  },
	{ REMOTE_SECRET,    "secret",     6  },
	{ REMOTE_TOKEN,     "token",      5  },
	{ REMOTE_PATH,      "path",       4  },
	{ REMOTE_PATH_CA,   "tls_capath", 10 },
	{ REMOTE_FILE_CA,   "tls_ca",     6  },
	{ REMOTE_FILE_CERT, "tls_cert",   8  },
	{ REMOTE_FILE_KEY,  "tls_key",    7  },
	{ REMOTE_SERVER,    "tls_server", 10 },
	{ REMOTE_DEBUG,     "debug",      5  },
	{ REMOTE_MAX,        NULL,        0  }
};

const char*
remote_nameof(int id)
{
	if (id < 0 || id >= REMOTE_MAX)
		return NULL;
	return remote_opts[id].name;
}

int
remote_idof(Str* name)
{
	auto opt = &remote_opts[0];
	for (; opt->name; opt++) {
		if (str_is(name, opt->name, opt->name_size))
			return opt->id;
	}
	return -1;
}
