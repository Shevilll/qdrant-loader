from abc import ABC, abstractmethod

from qdrant_loader.config.source_config import SourceConfig
from qdrant_loader.core.document import Document
from qdrant_loader.core.file_conversion import FileConversionConfig
from qdrant_loader.core.state import CheckpointManager


class ConnectorConfigurationError(Exception):
    """Raised when a connector's configuration is invalid or access is denied.

    This is a *fatal* error: the pipeline should stop rather than silently
    continuing with 0 documents.
    """


class BaseConnector(ABC):
    """Base class for all connectors."""

    def __init__(self, config: SourceConfig):
        self.config = config
        self._initialized = False
        self._checkpoint_manager: CheckpointManager | None = None

    async def __aenter__(self):
        """Async context manager entry."""
        self._initialized = True
        return self

    async def __aexit__(self, exc_type, exc_val, _exc_tb):
        """Async context manager exit."""
        self._initialized = False

    def set_checkpoint_manager(self, checkpoint_manager: CheckpointManager) -> None:
        """Set the checkpoint manager for resumable ingestion.

        Args:
            checkpoint_manager: Checkpoint manager instance
        """
        self._checkpoint_manager = checkpoint_manager

    def set_file_conversion_config(
        self, file_conversion_config: FileConversionConfig
    ) -> None:
        """Set file conversion configuration.

        This default implementation stores the configuration for potential
        use by subclasses that choose to honor it.

        Args:
            file_conversion_config: Global file conversion configuration
        """
        # Store on the instance so connectors that opt-in can access it.
        self._file_conversion_config = file_conversion_config

    @abstractmethod
    async def get_documents(self) -> list[Document]:
        """Get documents from the source."""
