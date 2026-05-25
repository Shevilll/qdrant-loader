from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime
import warnings

from qdrant_loader.config.source_config import SourceConfig
from qdrant_loader.core.document import Document
from qdrant_loader.core.file_conversion import FileConversionConfig


class ConnectorConfigurationError(Exception):
    """Raised when a connector's configuration is invalid or access is denied.

    This is a *fatal* error: the pipeline should stop rather than silently
    continuing with 0 documents.
    """


class BaseConnector(ABC):
    """Base class for all connectors."""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls is BaseConnector:
            return
        if (
            cls.stream_documents is BaseConnector.stream_documents
            and cls.get_documents is BaseConnector.get_documents
        ):
            raise TypeError(
                "Concrete connectors must implement either get_documents() or stream_documents()."
            )

    def __init__(self, config: SourceConfig):
        if type(self) is BaseConnector:
            raise TypeError("BaseConnector cannot be instantiated directly")
        self.config = config
        self._initialized = False

    async def __aenter__(self):
        """Async context manager entry."""
        self._initialized = True
        return self

    async def __aexit__(self, exc_type, exc_val, _exc_tb):
        """Async context manager exit."""
        self._initialized = False

    def set_file_conversion_config(
        self, file_conversion_config: FileConversionConfig
    ) -> None:
        """Set file conversion configuration.

        This default implementation stores the configuration for potential
        use by subclasses that choose to honor it.

        Args:
            file_conversion_config: Global file conversion configuration
        """
        self._file_conversion_config = file_conversion_config

    async def stream_documents(
        self, since: datetime | None = None
    ) -> AsyncIterator[Document]:
        """Stream documents from the source."""
        warnings.warn(
            "stream_documents() is not implemented; falling back to get_documents(). "
            "Implement stream_documents() for streaming support.",
            DeprecationWarning,
            stacklevel=2,
        )
        try:
            documents = await self.get_documents(since)
        except TypeError:
            documents = await self.get_documents()
        for document in documents:
            yield document

    async def get_documents(self, since: datetime | None = None) -> list[Document]:
        """Deprecated: Collect documents from the source.

        The default implementation materializes stream_documents() so
        connectors that still rely on get_documents() continue to work.
        """
        warnings.warn(
            "get_documents() is deprecated and will be removed in a future release. "
            "Implement stream_documents() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        try:
            return [document async for document in self.stream_documents(since)]
        except TypeError:
            return [document async for document in self.stream_documents()]

    async def fetch_by_id(self, entity_id: str) -> Document | None:
        """Fetch a single entity by ID."""
        async for document in self.stream_documents(None):
            if document.id == entity_id:
                return document
        return None

    async def list_entity_ids(self) -> AsyncIterator[str]:
        """List all entity IDs for delete detection."""
        async for document in self.stream_documents(None):
            yield document.id
