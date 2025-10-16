import argparse
import unittest

from qlever.commands.stop import StopCommand


class TestStopCommand(unittest.TestCase):
    def test_description(self):
        result = StopCommand().description()
        self.assertEqual(
            result, "Stop QLever server for a given dataset or port"
        )

    def test_should_have_qleverfile(self):
        self.assertTrue(StopCommand().should_have_qleverfile())

    def test_relevant_qleverfile_arguments(self):
        result = StopCommand().relevant_qleverfile_arguments()
        self.assertEqual(
            result,
            {
                "data": ["name"],
                "server": ["port"],
                "runtime": ["server_container"],
            },
        )

    def test_additional_arguments(self):
        # Create an instance of StopCommand
        sc = StopCommand()

        # Create a parser and a subparser
        parser = argparse.ArgumentParser()
        subparser = parser.add_argument_group("test")
        # Call the method
        sc.additional_arguments(subparser)
        # Parse an empty argument list to see the default
        args = parser.parse_args([])

        # Test that the default value for cmdline_regex is set correctly
        self.assertEqual(args.cmdline_regex, "ServerMain.* -i [^ ]*%%NAME%%")

        # Test that the help text for cmdline_regex is correctly set
        argument_help = subparser._group_actions[-2].help
        self.assertEqual(
            argument_help,
            "Show only processes where the command line matches this regex",
        )

        # Test that the default value for no-containers is set correctly
        self.assertEqual(args.no_containers, False)

        # Test that the help text for no-containers is correctly set
        argument_help = subparser._group_actions[-1].help
        self.assertEqual(
            argument_help,
            "Do not look for containers, only for native processes",
        )
