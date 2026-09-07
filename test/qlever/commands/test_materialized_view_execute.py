from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from qlever.commands.materialized_view import MaterializedViewCommand


def make_args(**overrides):
    args = MagicMock()
    args.view_name = "my-view"
    args.view_query = None
    args.sparql_endpoint = None
    args.host_name = "localhost"
    args.port = 7000
    args.access_token = "token"
    args.load = False
    args.unload = False
    args.delete = False
    args.qleverfile = "Qleverfile"
    args.show = False
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


class TestMaterializedViewCommand(unittest.TestCase):
    def test_unload_works_without_qleverfile(self):
        args = make_args(unload=True)
        with (
            patch(
                "qlever.commands.materialized_view.Path.is_file",
                return_value=False,
            ) as mock_is_file,
            patch(
                "qlever.commands.materialized_view.run_command"
            ) as mock_run_command,
        ):
            mock_run_command.return_value = (
                '{"materialized-view-unloaded": "my-view"}'
            )
            result = MaterializedViewCommand().execute(args)
        self.assertTrue(result)
        mock_is_file.assert_not_called()

    def test_unload_and_load_together_is_an_error(self):
        args = make_args(unload=True, load=True)
        result = MaterializedViewCommand().execute(args)
        self.assertFalse(result)

    def test_unload_and_delete_together_is_an_error(self):
        args = make_args(unload=True, delete=True)
        result = MaterializedViewCommand().execute(args)
        self.assertFalse(result)

    def test_query_with_unload_is_an_error(self):
        args = make_args(unload=True, view_query="SELECT * WHERE { ?s ?p ?o }")
        result = MaterializedViewCommand().execute(args)
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
