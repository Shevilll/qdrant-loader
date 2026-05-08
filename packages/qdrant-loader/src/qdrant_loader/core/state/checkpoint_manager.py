from datetime import datetime 

from pydantic import BaseModel
from qdrant_loader.core.state.models import IngestionCheckpoint
from qdrant_loader.core.state.state_manager import StateManager
from sqlalchemy import select


class Checkpoint(BaseModel):
    project_id: str
    source_type: str
    source: str
    cursor_kind: str
    cursor_value: str
    batch_index: int = 0
    updated_at: datetime | None = None

class CheckpointManager:
    def __init__(self, state_manager: StateManager):
        self.state_manager = state_manager

    async def get_checkpoint(
        self,
        project_id: str,
        source_type: str,
        source: str,
    ) -> Checkpoint | None:
        async with await self.state_manager.get_session() as session:
            result = await session.execute(
                select(IngestionCheckpoint).where(
                    IngestionCheckpoint.project_id == project_id,
                    IngestionCheckpoint.source_type == source_type,
                    IngestionCheckpoint.source == source,
                )
            )
            record = result.scalar_one_or_none()

            if not record:
                return None
            
            return Checkpoint(
                project_id=record.project_id,
                source_type=record.source_type,
                source=record.source,
                cursor_kind=record.cursor_kind,
                cursor_value=record.cursor_value,
                batch_index=record.batch_index,
                updated_at=record.updated_at,
            )
    
    async def save_checkpoint(self, checkpoint: Checkpoint) -> None:
        async with await self.state_manager.get_session() as session:
            record = await session.execute(
                select(IngestionCheckpoint).where(
                    IngestionCheckpoint.project_id == checkpoint.project_id,
                    IngestionCheckpoint.source_type == checkpoint.source_type,
                    IngestionCheckpoint.source == checkpoint.source,
                )
            )
            existing = record.scalar_one_or_none()

            if existing:
                existing.cursor_kind = checkpoint.cursor_kind
                existing.cursor_value = checkpoint.cursor_value
                existing.batch_index = checkpoint.batch_index
                existing.updated_at = datetime.now()
            else:
                new_record = IngestionCheckpoint(
                    project_id=checkpoint.project_id,
                    source_type=checkpoint.source_type,
                    source=checkpoint.source,
                    cursor_kind=checkpoint.cursor_kind,
                    cursor_value=checkpoint.cursor_value,
                    batch_index=checkpoint.batch_index,
                )
                session.add(new_record)
            
            await session.commit()
    
    async def clear_checkpoint(
        self,
        project_id: str,
        source_type: str,
        source: str,
    ) -> None:

        async with await self.state_manager.get_session() as session:

            result = await session.execute(
                select(IngestionCheckpoint).where(
                    IngestionCheckpoint.project_id == project_id,
                    IngestionCheckpoint.source_type == source_type,
                    IngestionCheckpoint.source == source,
                )
            )

            existing = result.scalar_one_or_none()

            if existing:
                await session.delete(existing)
                await session.commit()