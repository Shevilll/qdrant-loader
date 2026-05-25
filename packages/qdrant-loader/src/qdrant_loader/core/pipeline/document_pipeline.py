"""Document processing pipeline that coordinates chunking, embedding, and upserting."""

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable

from qdrant_loader.core.document import Document
from qdrant_loader.utils.logging import LoggingConfig

from .workers import ChunkingWorker, EmbeddingWorker, UpsertWorker
from .workers.upsert_worker import PipelineResult

logger = LoggingConfig.get_logger(__name__)


class DocumentPipeline:
    """Handles the chunking -> embedding -> upsert pipeline."""

    def __init__(
        self,
        chunking_worker: ChunkingWorker,
        embedding_worker: EmbeddingWorker,
        upsert_worker: UpsertWorker,
    ):
        self.chunking_worker = chunking_worker
        self.embedding_worker = embedding_worker
        self.upsert_worker = upsert_worker

    async def process_documents(
        self,
        documents: list[Document] | AsyncIterator[Document],
        batch_size: int = 25,
        on_batch_complete: Callable[[list[Document], PipelineResult], Awaitable[None]] | None = None,
    ) -> PipelineResult:
        """Process documents through the pipeline.

        Args:
            documents: List or async iterator of documents to process
            batch_size: Number of documents to process in each batch
            on_batch_complete: Optional callback invoked after each batch is processed

        Returns:
            PipelineResult with processing statistics
        """
        if isinstance(documents, list):
            document_iter = documents
            total_documents = len(documents)
        else:
            document_iter = documents
            total_documents = None

        if total_documents is not None:
            logger.info(
                f"⚙️ Processing {total_documents} documents through pipeline"
                + (f" (expected {total_documents} documents)" if total_documents is not None else "")
            )
        else:
            logger.info("⚙️ Processing documents through pipeline")

        start_time = time.time()

        try:
            batch: list[Document] = []
            result = PipelineResult()

            if total_documents is not None:
                logger.debug(f"🧾 Total documents to process: {total_documents}")

            async def _get_document_iterator():
                if isinstance(document_iter, list):
                    for document in document_iter:
                        yield document
                else:
                    async for document in document_iter:
                        yield document

            async for document in _get_document_iterator():
                batch.append(document)
                if len(batch) < batch_size:
                    continue

                logger.info(f"🔄 Processing document batch of {len(batch)} documents")
                batch_result = await self._process_document_batch(batch)
                result.merge(batch_result)
                if on_batch_complete is not None:
                    await on_batch_complete(batch, batch_result)
                batch.clear()

            if total_documents == 0:
                logger.info("🔄 Processing final document batch of 0 documents")
                batch_result = await self._process_document_batch([])
                result.merge(batch_result)
                if on_batch_complete is not None:
                    await on_batch_complete([], batch_result)
            elif batch:
                logger.info(f"🔄 Processing final document batch of {len(batch)} documents")
                batch_result = await self._process_document_batch(batch)
                result.merge(batch_result)
                if on_batch_complete is not None:
                    await on_batch_complete(batch, batch_result)

            total_duration = time.time() - start_time
            logger.info(
                f"⏱️ Total pipeline duration: {total_duration:.2f} seconds"
            )
            logger.info(
                f"✅ Pipeline completed: {result.success_count} chunks processed, "
                f"{result.error_count} errors"
            )

            return result

        except Exception as e:
            total_duration = time.time() - start_time
            logger.error(
                f"❌ Document pipeline failed after {total_duration:.2f} seconds: {e}",
                exc_info=True,
            )
            result = PipelineResult()
            result.error_count = total_documents if total_documents is not None else 0
            result.errors = [f"Pipeline failed: {e}"]
            return result

    async def _process_document_batch(
        self, batch: list[Document]
    ) -> PipelineResult:
        logger.info("🔄 Starting chunking phase...")
        chunking_start = time.time()
        chunks_iter = self.chunking_worker.process_documents(batch)

        logger.info(
            "🔄 Chunking completed, transitioning to embedding phase..."
        )
        chunking_duration = time.time() - chunking_start
        logger.info(f"⏱️ Chunking phase took {chunking_duration:.2f} seconds")

        embedding_start = time.time()
        embedded_chunks_iter = self.embedding_worker.process_chunks(chunks_iter)

        logger.info("🔄 Embedding phase ready, starting upsert phase...")

        try:
            result = await asyncio.wait_for(
                self.upsert_worker.process_embedded_chunks(embedded_chunks_iter),
                timeout=3600.0,
            )
            embedding_duration = time.time() - embedding_start
            logger.info(
                f"⏱️ Embedding + Upsert phase took {embedding_duration:.2f} seconds"
            )
        except TimeoutError:
            logger.error("❌ Pipeline timed out after 1 hour")
            result = PipelineResult()
            result.error_count = len(batch)
            result.errors = ["Pipeline timed out after 1 hour"]
        except Exception as e:
            logger.error(f"❌ Pipeline failed during batch processing: {e}")
            result = PipelineResult()
            result.error_count = len(batch)
            result.errors = [f"Pipeline failed: {e}"]

        return result
