from abc import ABC, abstractmethod
from typing import Tuple

from sparql_conformance.config import Config


class EngineManager(ABC):
    """Abstract base class for SPARQL engine managers"""

    @abstractmethod
    def setup(self,
              config: Config,
              graph_paths: Tuple[Tuple[str, str], ...]
              ) -> Tuple[bool, bool, str, str]:
        """
        Set up the engine for testing.

        Args:
            config: Test suite config, used to set engine-specific settings
            graph_paths: ex. default graph + named graph (('graph_path', '-'),
                            ('graph_path2', 'graph_name2'))

        Returns:
            index_success (bool), server_success (bool), index_log (str), server_log (str)
        """
        pass

    @abstractmethod
    def cleanup(self, config: Config):
        """Clean up the test environment after testing"""
        pass

    @abstractmethod
    def query(self, config: Config, query: str, result_format: str) -> Tuple[int, str]:
        """
        Send a SPARQL query to the engine and return the result

        Args:
            config: Test suite config, used to set engine-specific settings
            query: The SPARQL query to be executed
            result_format: Type of the result

        Returns:
           HTTP status code (int), query result (str)
        """
        pass

    @abstractmethod
    def update(self, config: Config, query: str) -> Tuple[int, str]:
        """
        Send a SPARQL update query to the engine and return the result

        Args:
            config: Test suite config, used to set engine-specific settings
            query: The SPARQL update query to be executed

        Returns:
           HTTP status code (int), response (str)
        """
        pass

    @abstractmethod
    def protocol_endpoint(self) -> str:
        """
        Returns the name of the protocol endpoint for the engine.
        Used to replace the standard endpoint with the
        engine-specific endpoint in the protocol tests.
        Ex. POST /sparql/ HTTP/1.1 -> POST /qlever/ HTTP/1.1
        """
        pass

