"""Base classes for connectors and change detectors."""

from collections.abc import AsyncIterator
from datetime import datetime
from urllib.parse import quote, unquote

from pydantic import BaseModel, ConfigDict

from qdrant_loader.config.sources import SourcesConfig
from qdrant_loader.core.document import Document
from qdrant_loader.core.state.exceptions import InvalidDocumentStateError
from qdrant_loader.core.state.state_manager import DocumentStateRecord, StateManager
from qdrant_loader.utils.logging import LoggingConfig


class DocumentState(BaseModel):
    """Standardized document state representation.

    This class provides a consistent way to represent document states across
    all sources. It includes the essential fields needed for change detection.
    """

    document_id: str
    uri: str  # Universal identifier in format: {source_type}:{source}:{url}
    content_hash: str  # Hash of document content
    updated_at: datetime  # Last update timestamp

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")


class StateChangeDetector:
    """Optimized change detector for document state management.

    This class provides efficient change detection functionality across
    all sources with minimal overhead and simplified logic.
    """

    def __init__(self, state_manager: StateManager):
        """Initialize the change detector."""
        self.logger = LoggingConfig.get_logger(
            f"qdrant_loader.{self.__class__.__name__}"
        )
        self._initialized = False
        self.state_manager = state_manager

    async def __aenter__(self):
        """Async context manager entry."""
        self._initialized = True
        return self

    async def __aexit__(self, exc_type, exc_val, _exc_tb):
        """Async context manager exit."""
        if exc_type:
            self.logger.error(
                "Error in StateChangeDetector context",
                error_type=exc_type.__name__,
                error=str(exc_val),
            )

    async def detect_changes(
        self,
        documents: list[Document] | AsyncIterator[Document],
        filtered_config: SourcesConfig,
        batch_uris: set[str] | None = None,
    ) -> dict[str, list[Document]]:
        """Detect changes in documents efficiently.
        
        Args:
            documents: Documents to check for changes
            filtered_config: Filtered source configuration
            batch_uris: Optional set of URIs in current batch. If provided, only
                       these URIs will be queried from state, enabling bounded-memory
                       operation on large document sets.
        """
        if not self._initialized:
            raise RuntimeError(
                "StateChangeDetector not initialized. Use as async context manager."
            )

        # Convert async iterator to list if needed, and collect URIs for optimization
        if isinstance(documents, list):
            document_list = documents
            document_iterator = documents
            self.logger.info(
                "Starting change detection", document_count=len(documents)
            )
        else:
            # Materialize the stream to collect URIs for efficient state querying
            document_list = []
            async for doc in documents:
                document_list.append(doc)
            document_iterator = document_list
            self.logger.info(
                "Starting change detection for streamed documents",
                document_count=len(document_list),
            )
            
            # If not provided, extract URIs from materialized batch for bounded memory
            if batch_uris is None:
                batch_uris = {
                    self._generate_uri_from_document(doc) for doc in document_list
                }

        # Get previous states for the configured sources
        # If batch_uris provided, query only those URIs for bounded memory usage
        previous_states = await self._get_previous_states(
            filtered_config, batch_uris=batch_uris
        )
        previous_states_dict: dict[str, DocumentState] = {
            state.uri: state for state in previous_states
        }
        previous_uris: set[str] = set(previous_states_dict.keys())
        current_uris: set[str] = set()

        new_docs: list[Document] = []
        updated_docs: list[Document] = []

        async for document in self._iterate_documents(document_iterator):
            current_state = self._get_document_state(document)
            current_uris.add(current_state.uri)

            previous_state = previous_states_dict.get(current_state.uri)
            if previous_state is None:
                new_docs.append(document)
            elif self._is_document_updated(current_state, previous_state):
                updated_docs.append(document)

        deleted_docs = [
            self._create_deleted_document(state)
            for state in previous_states
            if state.uri not in current_uris
        ]

        self.logger.info(
            "Change detection completed",
            new_count=len(new_docs),
            updated_count=len(updated_docs),
            deleted_count=len(deleted_docs),
        )

        return {
            "new": new_docs,
            "updated": updated_docs,
            "deleted": deleted_docs,
        }

    async def _iterate_documents(
        self, documents: list[Document] | AsyncIterator[Document]
    ) -> AsyncIterator[Document]:
        if isinstance(documents, list):
            for document in documents:
                yield document
        else:
            async for document in documents:
                yield document

    def _get_document_state(self, document: Document) -> DocumentState:
        """Get the standardized state of a document."""
        try:
            return DocumentState(
                document_id=document.id,
                uri=self._generate_uri_from_document(document),
                content_hash=document.content_hash,
                updated_at=document.updated_at,
            )
        except Exception as e:
            raise InvalidDocumentStateError(f"Failed to get document state: {e}") from e

    def _is_document_updated(
        self, current_state: DocumentState, previous_state: DocumentState
    ) -> bool:
        """Check if a document has been updated."""
        return (
            current_state.content_hash != previous_state.content_hash
            or current_state.updated_at > previous_state.updated_at
        )

    def _create_deleted_document(self, document_state: DocumentState) -> Document:
        """Create a minimal document for a deleted item."""
        source_type, source, url = document_state.uri.split(":", 2)
        url = unquote(url)

        return Document(
            id=document_state.document_id,
            content="",
            content_type="md",
            source=source,
            source_type=source_type,
            url=url,
            title="Deleted Document",
            metadata={
                "uri": document_state.uri,
                "title": "Deleted Document",
                "updated_at": document_state.updated_at.isoformat(),
                "content_hash": document_state.content_hash,
            },
        )

    async def _get_previous_states(
        self, filtered_config: SourcesConfig, batch_uris: set[str] | None = None
    ) -> list[DocumentState]:
        """Get previous document states from the state manager efficiently.
        
        Args:
            filtered_config: Source configuration
            batch_uris: Optional set of URIs to filter by. If provided, only states
                       matching these URIs will be loaded, enabling bounded memory.
        """
        previous_states_records: list[DocumentStateRecord] = []

        # Define source type mappings for cleaner iteration
        source_mappings = [
            ("git", filtered_config.git),
            ("confluence", filtered_config.confluence),
            ("jira", filtered_config.jira),
            ("publicdocs", filtered_config.publicdocs),
            ("localfile", filtered_config.localfile),
        ]

        # Process each source type
        for _source_name, source_configs in source_mappings:
            if source_configs:
                for config in source_configs.values():
                    records = await self.state_manager.get_document_state_records(
                        config
                    )
                    previous_states_records.extend(records)

        # Convert records to states, optionally filtering by batch URIs
        states = [
            DocumentState(
                document_id=record.document_id,  # type: ignore
                uri=self._generate_uri(
                    record.url, record.source, record.source_type, record.document_id  # type: ignore
                ),
                content_hash=record.content_hash,  # type: ignore
                updated_at=record.updated_at,  # type: ignore
            )
            for record in previous_states_records
        ]
        
        # If batch_uris provided, filter to only those URIs for bounded memory
        if batch_uris:
            states = [state for state in states if state.uri in batch_uris]
            self.logger.debug(
                "Filtered previous states to batch URIs",
                requested_uris=len(batch_uris),
                matching_states=len(states),
            )
        
        return states

    def _normalize_url(self, url: str) -> str:
        """Normalize a URL for consistent hashing."""
        return quote(url.rstrip("/"), safe="")

    def _generate_uri_from_document(self, document: Document) -> str:
        """Generate a URI from a document."""
        return self._generate_uri(
            document.url, document.source, document.source_type, document.id
        )

    def _generate_uri(
        self, url: str, source: str, source_type: str, document_id: str
    ) -> str:
        """Generate a URI from document components."""
        return f"{source_type}:{source}:{self._normalize_url(url)}"
