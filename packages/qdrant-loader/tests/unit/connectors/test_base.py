"""Tests for the connectors base module."""

from datetime import datetime, UTC
from unittest.mock import Mock

import pytest
from qdrant_loader.connectors.base import BaseConnector
from qdrant_loader.core.document import Document


class TestBaseConnector:
    """Test cases for BaseConnector."""

    def test_set_file_conversion_config_default_implementation(self):
        """Test the default set_file_conversion_config implementation - covers line 35."""

        # Create a concrete implementation of BaseConnector for testing
        class TestConnector(BaseConnector):
            def __init__(self, config=None):
                if config is None:
                    config = Mock()
                super().__init__(config)

            async def get_documents(self) -> list[Document]:
                return []  # Minimal implementation

        connector = TestConnector()

        # Call set_file_conversion_config - this should hit line 35 (the pass statement)
        file_conversion_config = Mock()

        # This should not raise any errors and should execute the pass statement
        result = connector.set_file_conversion_config(file_conversion_config)

        # The method should return None (implicit return from pass statement)
        assert result is None

    def test_abstract_method_signature(self):
        """Test that BaseConnector defines the abstract methods correctly."""
        # Verify that BaseConnector has the expected abstract methods
        assert hasattr(BaseConnector, "get_documents")
        assert hasattr(BaseConnector, "set_file_conversion_config")

        # Verify that set_file_conversion_config is not abstract (has default implementation)
        # We can test this by checking if it can be called on a concrete subclass
        class TestConnector(BaseConnector):
            def __init__(self, config=None):
                if config is None:
                    config = Mock()
                super().__init__(config)

            async def get_documents(self) -> list[Document]:
                return []

        connector = TestConnector()

        # Should be able to call set_file_conversion_config (covers line 35)
        connector.set_file_conversion_config(Mock())

    def test_base_connector_instantiation_fails(self):
        """Test that BaseConnector cannot be instantiated directly due to abstract methods."""
        with pytest.raises(TypeError):
            BaseConnector()

    def test_concrete_subclass_must_implement_either_get_or_stream_documents(self):
        """Test that concrete subclasses must implement either get_documents or stream_documents."""

        with pytest.raises(TypeError):
            class IncompleteConnector(BaseConnector):
                def __init__(self, config=None):
                    if config is None:
                        config = Mock()
                    super().__init__(config)

                # Missing both get_documents and stream_documents implementation

        with pytest.raises(TypeError):
            class OtherIncompleteConnector(BaseConnector):
                def __init__(self, config=None):
                    if config is None:
                        config = Mock()
                    super().__init__(config)

    def test_get_documents_delegates_to_stream_documents(self):
        """Test that get_documents falls back to stream_documents."""

        class TestConnector(BaseConnector):
            def __init__(self, config=None):
                if config is None:
                    config = Mock()
                super().__init__(config)

            async def stream_documents(self):
                yield Document(
                    id="1",
                    content="hello",
                    content_type="text",
                    source="s",
                    source_type="t",
                    created_at=datetime.now(UTC),
                    url="http://example.com",
                    title="Example",
                    updated_at=datetime.now(UTC),
                    is_deleted=False,
                    metadata={},
                )

        connector = TestConnector()
        result = pytest.importorskip("asyncio").run(connector.get_documents())
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].id == "1"

    def test_stream_documents_delegates_to_get_documents(self):
        """Test that stream_documents falls back to get_documents."""

        class TestConnector(BaseConnector):
            def __init__(self, config=None):
                if config is None:
                    config = Mock()
                super().__init__(config)

            async def get_documents(self):
                return [
                    Document(
                        id="1",
                        content="hello",
                        content_type="text",
                        source="s",
                        source_type="t",
                        created_at=datetime.now(UTC),
                        url="http://example.com",
                        title="Example",
                        updated_at=datetime.now(UTC),
                        is_deleted=False,
                        metadata={},
                    )
                ]

        connector = TestConnector()
        documents = []

        async def collect():
            async for doc in connector.stream_documents():
                documents.append(doc)

        import asyncio

        asyncio.run(collect())
        assert len(documents) == 1
        assert documents[0].id == "1"

    def test_set_file_conversion_config_with_none_config(self):
        """Test set_file_conversion_config with None config - covers line 35."""

        class TestConnector(BaseConnector):
            def __init__(self, config=None):
                if config is None:
                    config = Mock()
                super().__init__(config)

            async def get_documents(self) -> list[Document]:
                return []

        connector = TestConnector()

        # Call with None config - should still execute line 35 without errors
        result = connector.set_file_conversion_config(None)
        assert result is None

    def test_set_file_conversion_config_multiple_calls(self):
        """Test multiple calls to set_file_conversion_config - covers line 35 multiple times."""

        class TestConnector(BaseConnector):
            def __init__(self, config=None):
                if config is None:
                    config = Mock()
                super().__init__(config)

            async def get_documents(self) -> list[Document]:
                return []

        connector = TestConnector()

        # Multiple calls should all execute line 35
        for i in range(3):
            file_config = Mock()
            file_config.test_value = i
            result = connector.set_file_conversion_config(file_config)
            assert result is None
