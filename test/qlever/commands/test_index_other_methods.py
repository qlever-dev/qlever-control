import argparse
import unittest
from unittest.mock import MagicMock, patch

from qlever.commands.index import IndexCommand


class TestIndexCommand(unittest.TestCase):
    def setUp(self):
        # Initialize an IndexCommand instance to access the helper function
        self.index_command = IndexCommand()

    def test_description(self):
        # Call the method
        result = self.index_command.description()

        self.assertEqual(result, "Build the index for a given " "RDF dataset")

    def test_should_have_qleverfile(self):
        # Call the method
        result = self.index_command.should_have_qleverfile()

        assert result

    def test_relevant_qleverfile_arguments(self):
        # Call the method
        result = self.index_command.relevant_qleverfile_arguments()

        self.assertEqual(
            result,
            {
                "data": ["name", "format"],
                "index": [
                    "input_files",
                    "cat_input_files",
                    "encode_as_id",
                    "multi_input_json",
                    "parallel_parsing",
                    "settings_json",
                    "vocabulary_type",
                    "index_binary",
                    "only_pso_and_pos_permutations",
                    "ulimit",
                    "use_patterns",
                    "text_index",
                    "stxxl_memory",
                    "parser_buffer_size",
                ],
                "runtime": ["system", "image", "index_container"],
            },
        )

    def test_additional_arguments(self):
        # Create a parser and a subparser
        parser = argparse.ArgumentParser()
        subparser = parser.add_argument_group("test")

        # Call the method
        self.index_command.additional_arguments(subparser)
        # Parse an empty argument list to see the default
        args = parser.parse_args([])

        # Test that the default value for cmdline_regex is set correctly
        self.assertEqual(args.overwrite_existing, False)

        # Test that the help text for cmdline_regex is correctly set
        argument_help = subparser._group_actions[-1].help
        self.assertEqual(
            argument_help,
            "Overwrite an existing index, " "think twice before using this",
        )

    def test_get_input_options_for_json_valid_input(self):
        # Mock args with a valid multi_input_json and format
        args = MagicMock()
        args.multi_input_json = (
            '[{"cmd": "test_data1", "format": "json"}, '
            '{"cmd": "test_data2"}]'
        )
        args.format = "jsonld"  # Default format if not specified in JSON

        result = self.index_command.get_input_options_for_json(args)

        # Expected command-line options string based on the JSON data
        expected_result = (
            "-f <(test_data1) -g - -F json -p false "
            "-f <(test_data2) -g - -F jsonld -p false"
        )
        self.assertEqual(result, expected_result)

    @patch("qlever.commands.index.json")
    def test_get_input_options_for_json_invalid_format(self, mock_json):
        # Mock args with invalid JSON format
        args = MagicMock()
        # Set invalid JSON string instead of array
        args.multi_input_json = '{"cmd": "test_data"}'

        # Simulate a JSON loading error
        mock_json.loads.side_effect = Exception("Wrong format")

        # Execute the function and check for the exception
        with self.assertRaises(IndexCommand.InvalidInputJson) as context:
            self.index_command.get_input_options_for_json(args)

        # Check if the correct error message is in the raised exception
        self.assertEqual(
            context.exception.error_message,
            "Failed to parse `MULTI_INPUT_JSON` as either JSON "
            "or JSONL (Wrong format)",
        )
        self.assertEqual(
            context.exception.additional_info, args.multi_input_json
        )

    def test_get_input_options_for_json_no_array(self):
        # Mock args with JSON that's not an array
        args = MagicMock()
        args.multi_input_json = '{"key": "test_data1"}'

        with self.assertRaises(IndexCommand.InvalidInputJson) as context:
            self.index_command.get_input_options_for_json(args)

        # Verify error message mentions array requirement
        self.assertEqual(
            "Element 0 in `MULTI_INPUT_JSON` must contain a key `cmd`",
            context.exception.error_message,
        )
        self.assertEqual(
            {"key": "test_data1"}, context.exception.additional_info
        )

    def test_get_input_options_for_json_empty(self):
        # Mock args with an empty JSON array
        args = MagicMock()
        args.multi_input_json = "[]"

        with self.assertRaises(IndexCommand.InvalidInputJson) as context:
            self.index_command.get_input_options_for_json(args)

        # Verify error message mentions non-empty requirement
        self.assertEqual(
            context.exception.error_message,
            "`MULTI_INPUT_JSON` must contain at least one element",
        )
        self.assertEqual(
            context.exception.additional_info, args.multi_input_json
        )

    def test_get_input_options_for_json_object_structure(self):
        # Mock args where one of the JSON objects is missing the
        # required "cmd" key
        args = MagicMock()
        args.multi_input_json = (
            '[{"cmd": "test_data1", "format": "json"}, ' '{"format": "json2"}]'
        )  # Missing "cmd"

        with self.assertRaises(IndexCommand.InvalidInputJson) as context:
            self.index_command.get_input_options_for_json(args)

        # Verify error mentions the missing `cmd` key
        self.assertEqual(
            "Element 1 in `MULTI_INPUT_JSON` must contain " "a key `cmd`",
            context.exception.error_message,
        )

        self.assertEqual(
            {"format": "json2"}, context.exception.additional_info
        )

    def test_get_input_options_for_json_object_type(self):
        # Mock args where one of the JSON objects is not a dictionary
        args = MagicMock()
        args.multi_input_json = (
            '[{"cmd": "test_data1", "format": "json"}, ' "5]"
        )  # Missing "cmd"

        with self.assertRaises(IndexCommand.InvalidInputJson) as context:
            self.index_command.get_input_options_for_json(args)

        # Verify error mentions the missing `cmd` key
        self.assertEqual(
            "Element 1 in `MULTI_INPUT_JSON` must be a JSON " "object",
            context.exception.error_message,
        )

        self.assertEqual(5, context.exception.additional_info)

    def test_get_input_options_for_json_extra_keys(self):
        # Mock args where one of the JSON objects contains an extra key
        args = MagicMock()
        args.multi_input_json = '[{"cmd": "test_data1", "test_key": "data2"}]'

        with self.assertRaises(IndexCommand.InvalidInputJson) as context:
            self.index_command.get_input_options_for_json(args)

        # Verify error mentions the missing `cmd` key
        self.assertEqual(
            "Element 0 in `MULTI_INPUT_JSON` must only "
            "contain the keys `format`, `graph`, and `parallel`. "
            "Contains extra keys {'test_key'}.",
            context.exception.error_message,
        )

        self.assertEqual(
            {"cmd": "test_data1", "test_key": "data2"},
            context.exception.additional_info,
        )
