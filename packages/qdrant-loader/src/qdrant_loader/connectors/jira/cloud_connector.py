"""Jira connector implementation."""

from collections.abc import AsyncGenerator
from datetime import datetime
from urllib.parse import urlparse  # noqa: F401 - may be used in URL handling

from requests.auth import HTTPBasicAuth  # noqa: F401 - compatibility

from qdrant_loader.connectors.jira.connector import BaseJiraConnector
from qdrant_loader.connectors.jira.models import (
    JiraIssue,
)
from qdrant_loader.utils.logging import LoggingConfig

logger = LoggingConfig.get_logger(__name__)


class JiraCloudConnector(BaseJiraConnector):
    """Jira cloud connector for fetching and processing issues."""

    CLOUD_JIRA_VERSION = "3"
    SEARCH_ENDPOINT = "search/jql"

    def _get_api_url(self, endpoint: str) -> str:
        """Construct the full API URL for an endpoint.

        Args:
            endpoint: API endpoint path

        Returns:
            str: Full API URL
        """

        return f"{self.base_url}/rest/api/{self.CLOUD_JIRA_VERSION}/{endpoint}"

    async def get_issues(
        self, updated_after: datetime | None = None
    ) -> AsyncGenerator[JiraIssue, None]:
        """
        Get all issues from Jira.

        Supports resumable ingestion via checkpoints when checkpoint manager is set.

        Args:
            updated_after: Optional datetime to filter issues updated after this time

        Yields:
            JiraIssue objects
        """
        # Load checkpoint if available
        checkpoint_loaded = False
        if self._checkpoint_manager:
            self._checkpoint_data = await self._checkpoint_manager.load_checkpoint(
                project_id=getattr(self.config, 'project_id', None),
                source_type="jira",
                source_name=self.config.source,
            )
            if self._checkpoint_data:
                next_page_token = self._checkpoint_data.pagination_token
                processed_count = self._checkpoint_data.processed_count or 0
                checkpoint_loaded = True
                logger.info(
                    f"🎫 Resuming JIRA issue retrieval from checkpoint",
                    project_key=self.config.project_key,
                    processed_count=processed_count,
                    pagination_token=next_page_token,
                )
            else:
                logger.debug(
                    f"🎫 No checkpoint found, starting fresh JIRA issue retrieval",
                    project_key=self.config.project_key,
                )

        if not checkpoint_loaded:
            next_page_token = None
            processed_count = 0

        page_size = self.config.page_size
        attempted_count = processed_count  # Start from processed count
        # Log progress every 100 issues instead of every 50
        progress_log_interval = 100

        if not checkpoint_loaded:
            logger.info(
                "🎫 Starting JIRA issue retrieval",
                project_key=self.config.project_key,
                page_size=page_size,
                updated_after=updated_after.isoformat() if updated_after else None,
            )

        while True:
            jql = self._build_jql_filter(updated_after)

            params = {
                "jql": jql,
                "maxResults": page_size,
                "expand": "changelog",
                "fields": "*all",
            }

            if next_page_token:
                params["nextPageToken"] = next_page_token

            logger.debug(
                "Fetching JIRA issues page",
                next_page_token=next_page_token,
                page_size=page_size,
                jql=jql,
            )

            try:
                response = await self._make_request(
                    "GET", self.SEARCH_ENDPOINT, params=params
                )
            except Exception as e:
                logger.error(
                    "Failed to fetch JIRA issues page",
                    next_page_token=next_page_token,
                    page_size=page_size,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                raise

            if not response or not response.get("issues"):
                logger.debug(
                    "No more JIRA issues found, stopping pagination",
                    next_page_token=next_page_token,
                    total_processed=processed_count,
                )
                break

            issues = response["issues"]

            for issue in issues:
                try:
                    parsed_issue = self._parse_issue(issue)
                    yield parsed_issue
                    processed_count += 1

                    # Save checkpoint periodically
                    if (self._checkpoint_manager and
                        processed_count % self._checkpoint_save_interval == 0):
                        checkpoint_data = CheckpointData(
                            pagination_token=next_page_token,
                            processed_count=processed_count,
                            last_processed_id=parsed_issue.id,
                        )
                        await self._checkpoint_manager.save_checkpoint(
                            project_id=getattr(self.config, 'project_id', None),
                            source_type="jira",
                            source_name=self.config.source,
                            checkpoint_data=checkpoint_data,
                        )
                        logger.debug(
                            f"💾 Saved checkpoint at {processed_count} issues",
                            project_key=self.config.project_key,
                            pagination_token=next_page_token,
                        )

                    if (processed_count) % progress_log_interval == 0:
                        logger.info(
                            f"🎫 Processed {processed_count} JIRA issues so far"
                        )

                except Exception as e:
                    logger.error(
                        "Failed to parse JIRA issue",
                        issue_id=issue.get("id"),
                        issue_key=issue.get("key"),
                        error=str(e),
                        error_type=type(e).__name__,
                    )
                    # Continue processing other issues instead of failing completely
                    continue

            attempted_count += len(issues)
            # Check next page token
            next_page_token = response.get("nextPageToken")
            is_last = response.get("isLast")
            if is_last or not next_page_token:
                logger.info(
                    f"✅ Completed JIRA issue retrieval: "
                    f"{attempted_count} issues attempted, "
                    f"{processed_count} successfully processed"
                )
                # Clear checkpoint on successful completion
                if self._checkpoint_manager:
                    await self._checkpoint_manager.delete_checkpoint(
                        project_id=getattr(self.config, 'project_id', None),
                        source_type="jira",
                        source_name=self.config.source,
                    )
                    logger.debug(
                        f"🗑️ Cleared checkpoint after successful completion",
                        project_key=self.config.project_key,
                    )
                break
