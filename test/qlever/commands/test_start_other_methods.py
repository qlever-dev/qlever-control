import argparse
import unittest

from qlever.commands.start import StartCommand


class TestStartCommand(unittest.TestCase):
    def test_description(self):
        self.assertEqual(
            StartCommand().description(),
            "Start the "
            "QLever server (requires that you have built "
            "an index with `qlever index` before)",
        )

    def test_should_have_qleverfile(self):
        assert StartCommand().should_have_qleverfile()

    def test_relevant_qleverfile_arguments(self):
        # Create an instance of IndexCommand
        sc = StartCommand()

        # Call the method
        result = sc.relevant_qleverfile_arguments()

        self.assertEqual(
            result,
            {
                "data": ["name", "description", "text_description"],
                "server": [
                    "server_binary",
                    "host_name",
                    "port",
                    "access_token",
                    "memory_for_queries",
                    "cache_max_size",
                    "cache_max_size_single_entry",
                    "cache_max_num_entries",
                    "num_threads",
                    "timeout",
                    "persist_updates",
                    "only_pso_and_pos_permutations",
                    "use_patterns",
                    "use_text_index",
                    "warmup_cmd",
                ],
                "runtime": ["system", "image", "server_container"],
            },
        )

    def test_additional_arguments(self):
        # Create an instance of StopCommand
        sc = StartCommand()

        # Create a parser and a subparser
        parser = argparse.ArgumentParser()
        subparser = parser.add_argument_group("test")
        # Call the method
        sc.additional_arguments(subparser)
        # Parse an empty argument list to see the default
        args = parser.parse_args([])

        # Test that the default value for cmdline_regex is set correctly
        self.assertEqual(args.kill_existing_with_same_port, False)

        # Test that the help text for
        # --kill-existing-with-same-port is correctly set
        argument_help = subparser._group_actions[-3].help
        self.assertEqual(
            argument_help,
            "If a QLever server is already running "
            "on the same port, kill it before "
            "starting a new server",
        )

        # Test that the default value for --no-warmup is set correctly
        self.assertEqual(args.no_warmup, False)

        # Test that the help text for --no-warmup is correctly set
        argument_help = subparser._group_actions[-2].help
        self.assertEqual(argument_help, "Do not execute the warmup command")
