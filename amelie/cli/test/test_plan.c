
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

#include <amelie.h>
#include <amelie_cli.h>
#include <amelie_cli_test.h>

void
test_plan_read(TestPlan* self, Str* path)
{
	if (! fs_exists("%.*s", str_size(path), str_of(path)))
		error("test plan file '%.*s' not found\n",
		      str_size(path), str_of(path));

	auto data = file_import("%.*s", str_size(path), str_of(path));
	defer_buf(data);

	Str text;
	buf_str(data, &text);

	Str directory;
	str_init(&directory);

	TestGroup* group = NULL;
	auto line = 1;
	for (; !str_empty(&text); line++)
	{
		Str cmd;
		str_split(&text, &cmd, '\n');

		// empty line
		if (str_empty(&cmd))
		{
			str_advance(&text, 1);
			continue;
		}
		str_advance(&text, str_size(&cmd));

		// comment
		if (*cmd.pos == '#')
			continue;

		if (str_is_prefix(&cmd, "directory", 9))
		{
			// directory <name>
			str_advance(&cmd, 9);
			str_arg(&cmd, &directory);
		} else
		if (str_is_prefix(&cmd, "group", 5))
		{
			// group <name>
			str_advance(&cmd, 5);
			if (str_empty(&directory))
				error("plan: %d: directory is not set\n", line);
			Str name;
			str_arg(&cmd, &name);
			if (str_empty(&name))
				error("plan: %d: bad group name\n", line);
			group = test_plan_find_group(self, &name);
			if (group)
				error("plan: %d: group '%.*s' redefined\n", line,
				      str_size(&name), str_of(&name));
			group = test_group_create(&name, &directory);
			test_plan_add_group(self, group);
		} else
		if (str_is_prefix(&cmd, "test", 4))
		{
			// test <name>
			str_advance(&cmd, 4);
			if (! group)
				error("plan: %d: test group is not defined for test\n", line);
			Str name;
			str_arg(&cmd, &name);
			if (str_empty(&name))
				error("plan: %d: bad test name\n", line);
			auto test = test_plan_find(self, &name);
			if (test)
				error("plan: %d: test '%.*s' redefined\n", line,
				      str_size(&name), str_of(&name));
			test = test_create(&name, group);
			test_plan_add(self, test);
		} else {
			error("plan: %d: unknown plan file command\n", line);
		}
	}
}
