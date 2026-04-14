"""
Advisory State Tracker

Tracks which NHC advisories have already been processed using
DynamoDB (on Lambda) or a local JSON file (for development).

DynamoDB Table Schema:
    Table: storm-detector-state
    Partition Key: storm_id (String) — e.g. "AL142024"
    Sort Key: advisory_guid (String) — RSS guid of the advisory

    Attributes:
        storm_name (String)
        storm_type (String)
        advisory_title (String)
        processed_at (String, ISO 8601)
        pipeline_execution_arn (String, optional)
        ttl (Number) — auto-expire after 90 days
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Set

from .nhc_feed import AdvisoryInfo

logger = logging.getLogger(__name__)

# TTL for DynamoDB items: 90 days after processing
TTL_DAYS = 90


class AdvisoryStateTracker:
    """
    Tracks which advisories have been seen/processed.

    Uses DynamoDB on AWS, falls back to local JSON file
    when running in dry_run / development mode.
    """

    def __init__(
        self,
        table_name: str,
        dynamodb_resource=None,
        dry_run: bool = False,
        local_state_path: Optional[str] = None,
    ):
        self.table_name = table_name
        self.dry_run = dry_run
        self._local_state_path = local_state_path or "/tmp/surgedps_state.json"

        if not dry_run and dynamodb_resource:
            self._table = dynamodb_resource.Table(table_name)
        else:
            self._table = None

        # In-memory cache of seen GUIDs for the current invocation
        self._seen_cache: Set[str] = set()

    def is_new_advisory(
        self, storm_id: str, advisory: AdvisoryInfo
    ) -> bool:
        """
        Check if an advisory has already been processed.

        Args:
            storm_id: ATCF storm ID (e.g. "AL142024")
            advisory: AdvisoryInfo from the feed parser

        Returns:
            True if this advisory has NOT been processed before
        """
        guid = advisory.advisory_id

        # Check in-memory cache first
        if guid in self._seen_cache:
            return False

        if self.dry_run or self._table is None:
            return self._is_new_local(storm_id, guid)

        try:
            response = self._table.get_item(
                Key={
                    "storm_id": storm_id,
                    "advisory_guid": guid,
                }
            )
            exists = "Item" in response
            if exists:
                self._seen_cache.add(guid)
            return not exists

        except Exception as e:
            logger.error(f"DynamoDB get_item failed: {e}")
            # On error, assume it's new (better to re-process than miss)
            return True

    def mark_processed(
        self,
        storm_id: str,
        advisory: AdvisoryInfo,
        pipeline_execution_arn: Optional[str] = None,
    ) -> None:
        """
        Record that an advisory has been processed.

        Args:
            storm_id: ATCF storm ID
            advisory: The advisory that was processed
            pipeline_execution_arn: ARN of the Step Functions execution
                                   that was triggered (if any)
        """
        guid = advisory.advisory_id
        self._seen_cache.add(guid)
        # datetime.utcnow() returns a naive datetime; .timestamp() then
        # interprets it as LOCAL time, so TTL on a non-UTC host was off
        # by the timezone offset. Use tz-aware utcnow.
        _now_dt = datetime.now(tz=timezone.utc)
        now = _now_dt.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        ttl_epoch = int((_now_dt + timedelta(days=TTL_DAYS)).timestamp())

        if self.dry_run or self._table is None:
            self._mark_local(storm_id, guid, now)
            return

        try:
            item = {
                "storm_id": storm_id,
                "advisory_guid": guid,
                "advisory_title": advisory.title[:500],  # Truncate
                "processed_at": now,
                "ttl": ttl_epoch,
            }
            if advisory.cyclone:
                item["storm_name"] = advisory.cyclone.name
                item["storm_type"] = advisory.cyclone.storm_type
            if pipeline_execution_arn:
                item["pipeline_execution_arn"] = pipeline_execution_arn

            self._table.put_item(Item=item)
            logger.info(
                f"Marked advisory as processed: {storm_id} / {guid}"
            )

        except Exception as e:
            logger.error(f"DynamoDB put_item failed: {e}")

    def get_active_storms(self) -> List[dict]:
        """
        Query DynamoDB for all storms with recent advisories.

        Returns a list of storm summary dicts.
        """
        if self.dry_run or self._table is None:
            return self._get_active_local()

        try:
            # Scan for items processed in the last 7 days
            cutoff = (
                datetime.now(tz=timezone.utc) - timedelta(days=7)
            ).strftime("%Y-%m-%dT%H:%M:%S") + "Z"

            response = self._table.scan(
                FilterExpression="processed_at > :cutoff",
                ExpressionAttributeValues={":cutoff": cutoff},
                ProjectionExpression="storm_id, storm_name, storm_type, processed_at",
            )

            # Deduplicate by storm_id, keeping the most recent
            storms = {}
            for item in response.get("Items", []):
                sid = item["storm_id"]
                if (
                    sid not in storms
                    or item["processed_at"] > storms[sid]["processed_at"]
                ):
                    storms[sid] = item

            return list(storms.values())

        except Exception as e:
            logger.error(f"DynamoDB scan failed: {e}")
            return []

    # ── Local File Fallback (Development) ──────────────────────────

    def _load_local_state(self) -> dict:
        """Load state from local JSON file."""
        if os.path.exists(self._local_state_path):
            try:
                with open(self._local_state_path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {"advisories": {}}

    def _save_local_state(self, state: dict) -> None:
        """Save state to local JSON file."""
        os.makedirs(os.path.dirname(self._local_state_path), exist_ok=True)
        with open(self._local_state_path, "w") as f:
            json.dump(state, f, indent=2)

    def _is_new_local(self, storm_id: str, guid: str) -> bool:
        state = self._load_local_state()
        key = f"{storm_id}:{guid}"
        return key not in state["advisories"]

    def _mark_local(self, storm_id: str, guid: str, timestamp: str) -> None:
        state = self._load_local_state()
        key = f"{storm_id}:{guid}"
        state["advisories"][key] = {"processed_at": timestamp}
        self._save_local_state(state)
        logger.info(f"[LOCAL] Marked advisory: {key}")

    def _get_active_local(self) -> List[dict]:
        state = self._load_local_state()
        storms = {}
        for key, val in state["advisories"].items():
            storm_id = key.split(":")[0]
            if storm_id not in storms:
                storms[storm_id] = {
                    "storm_id": storm_id,
                    "processed_at": val["processed_at"],
                }
        return list(storms.values())
