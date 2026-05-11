"""
Checkpoint manager for resumable ingestion.
"""

import json
from datetime import datetime, UTC
from typing import Any, Dict, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from qdrant_loader.core.state.models import IngestionCheckpoint
from qdrant_loader.utils.logging import LoggingConfig

logger = LoggingConfig.get_logger(__name__)


class CheckpointData:
    """Data structure for checkpoint information."""

    def __init__(
        self,
        pagination_token: Optional[str] = None,
        last_processed_id: Optional[str] = None,
        processed_count: int = 0,
        total_count: Optional[int] = None,
        last_updated: Optional[datetime] = None,
        custom_data: Optional[Dict[str, Any]] = None,
    ):
        self.pagination_token = pagination_token
        self.last_processed_id = last_processed_id
        self.processed_count = processed_count
        self.total_count = total_count
        self.last_updated = last_updated or datetime.now(UTC)
        self.custom_data = custom_data or {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "pagination_token": self.pagination_token,
            "last_processed_id": self.last_processed_id,
            "processed_count": self.processed_count,
            "total_count": self.total_count,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
            "custom_data": self.custom_data,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CheckpointData":
        """Create from dictionary."""
        last_updated = None
        if data.get("last_updated"):
            try:
                last_updated = datetime.fromisoformat(data["last_updated"])
            except (ValueError, TypeError):
                last_updated = datetime.now(UTC)

        return cls(
            pagination_token=data.get("pagination_token"),
            last_processed_id=data.get("last_processed_id"),
            processed_count=data.get("processed_count", 0),
            total_count=data.get("total_count"),
            last_updated=last_updated,
            custom_data=data.get("custom_data", {}),
        )


class CheckpointManager:
    """Manages ingestion checkpoints for resumable data fetching."""

    def __init__(self, session_factory):
        self.session_factory = session_factory

    async def save_checkpoint(
        self,
        project_id: Optional[str],
        source_type: str,
        source_name: str,
        checkpoint_data: CheckpointData,
    ) -> None:
        """Save or update a checkpoint for a source."""
        async with self.session_factory() as session:
            # Check if checkpoint already exists
            stmt = select(IngestionCheckpoint).where(
                IngestionCheckpoint.project_id == project_id,
                IngestionCheckpoint.source_type == source_type,
                IngestionCheckpoint.source_name == source_name,
            )
            result = await session.execute(stmt)
            existing_checkpoint = result.scalar_one_or_none()

            checkpoint_json = json.dumps(checkpoint_data.to_dict())

            if existing_checkpoint:
                # Update existing checkpoint
                existing_checkpoint.checkpoint_data = checkpoint_json
                existing_checkpoint.last_updated = datetime.now(UTC)
                logger.debug(
                    f"Updated checkpoint for {source_type}/{source_name}",
                    project_id=project_id,
                    processed_count=checkpoint_data.processed_count,
                )
            else:
                # Create new checkpoint
                new_checkpoint = IngestionCheckpoint(
                    project_id=project_id,
                    source_type=source_type,
                    source_name=source_name,
                    checkpoint_data=checkpoint_json,
                    last_updated=datetime.now(UTC),
                    created_at=datetime.now(UTC),
                )
                session.add(new_checkpoint)
                logger.debug(
                    f"Created new checkpoint for {source_type}/{source_name}",
                    project_id=project_id,
                    processed_count=checkpoint_data.processed_count,
                )

            await session.commit()

    async def load_checkpoint(
        self,
        project_id: Optional[str],
        source_type: str,
        source_name: str,
    ) -> Optional[CheckpointData]:
        """Load checkpoint data for a source."""
        async with self.session_factory() as session:
            stmt = select(IngestionCheckpoint).where(
                IngestionCheckpoint.project_id == project_id,
                IngestionCheckpoint.source_type == source_type,
                IngestionCheckpoint.source_name == source_name,
            )
            result = await session.execute(stmt)
            checkpoint = result.scalar_one_or_none()

            if checkpoint:
                try:
                    data_dict = json.loads(checkpoint.checkpoint_data)
                    checkpoint_data = CheckpointData.from_dict(data_dict)
                    logger.debug(
                        f"Loaded checkpoint for {source_type}/{source_name}",
                        project_id=project_id,
                        processed_count=checkpoint_data.processed_count,
                    )
                    return checkpoint_data
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(
                        f"Failed to parse checkpoint data for {source_type}/{source_name}: {e}",
                        project_id=project_id,
                    )
                    return None
            else:
                logger.debug(
                    f"No checkpoint found for {source_type}/{source_name}",
                    project_id=project_id,
                )
                return None

    async def delete_checkpoint(
        self,
        project_id: Optional[str],
        source_type: str,
        source_name: str,
    ) -> bool:
        """Delete a checkpoint for a source. Returns True if deleted, False if not found."""
        async with self.session_factory() as session:
            stmt = select(IngestionCheckpoint).where(
                IngestionCheckpoint.project_id == project_id,
                IngestionCheckpoint.source_type == source_type,
                IngestionCheckpoint.source_name == source_name,
            )
            result = await session.execute(stmt)
            checkpoint = result.scalar_one_or_none()

            if checkpoint:
                await session.delete(checkpoint)
                await session.commit()
                logger.debug(
                    f"Deleted checkpoint for {source_type}/{source_name}",
                    project_id=project_id,
                )
                return True
            else:
                logger.debug(
                    f"No checkpoint found to delete for {source_type}/{source_name}",
                    project_id=project_id,
                )
                return False

    async def list_checkpoints(
        self,
        project_id: Optional[str] = None,
        source_type: Optional[str] = None,
    ) -> list[Dict[str, Any]]:
        """List all checkpoints, optionally filtered by project or source type."""
        async with self.session_factory() as session:
            stmt = select(IngestionCheckpoint)
            if project_id:
                stmt = stmt.where(IngestionCheckpoint.project_id == project_id)
            if source_type:
                stmt = stmt.where(IngestionCheckpoint.source_type == source_type)

            result = await session.execute(stmt)
            checkpoints = result.scalars().all()

            checkpoint_list = []
            for checkpoint in checkpoints:
                try:
                    data_dict = json.loads(checkpoint.checkpoint_data)
                    checkpoint_data = CheckpointData.from_dict(data_dict)
                    checkpoint_list.append({
                        "project_id": checkpoint.project_id,
                        "source_type": checkpoint.source_type,
                        "source_name": checkpoint.source_name,
                        "checkpoint_data": checkpoint_data,
                        "last_updated": checkpoint.last_updated,
                        "created_at": checkpoint.created_at,
                    })
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(
                        f"Failed to parse checkpoint data for {checkpoint.source_type}/{checkpoint.source_name}: {e}",
                        project_id=checkpoint.project_id,
                    )

            return checkpoint_list