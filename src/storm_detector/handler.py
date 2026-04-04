"""
AWS Lambda Handler — Storm Detector

This is the entry point for the Lambda function invoked by EventBridge
on a 15-minute schedule. It:

1. Fetches NHC RSS feeds for all configured basins
2. Identifies active tropical cyclones
3. Checks for new advisories that haven't been processed yet
4. For qualifying storms (watches/warnings issued), downloads GIS data
5. Triggers the Step Functions flood modeling pipeline
6. Records the processed advisory in DynamoDB

Environment Variables:
    STATE_TABLE_NAME    — DynamoDB table for advisory state tracking
    DATA_BUCKET         — S3 bucket for storm data and tiles
    PIPELINE_STATE_MACHINE_ARN — Step Functions ARN to trigger
    ACTIVE_BASINS       — Comma-separated basin codes (default: "at")
    DRY_RUN             — "true" to skip AWS calls (local dev)
    LOG_LEVEL           — Logging level (default: INFO)
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from .config import StormDetectorConfig
from .nhc_feed import AdvisoryInfo, CycloneInfo, FeedResult, NHCFeedParser
from .gis_downloader import AdvisoryGISData, GISDownloader
from .state import AdvisoryStateTracker

# ── Logging Setup ──────────────────────────────────────────────────

log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("storm_detector")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Pipeline Trigger
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class PipelineTrigger:
    """Triggers the Step Functions flood modeling pipeline."""

    def __init__(self, sfn_client=None, state_machine_arn: str = ""):
        self.sfn_client = sfn_client
        self.state_machine_arn = state_machine_arn

    def trigger(
        self,
        cyclone: CycloneInfo,
        advisory: AdvisoryInfo,
        gis_data: AdvisoryGISData,
    ) -> Optional[str]:
        """
        Start a Step Functions execution for a storm advisory.

        Returns the execution ARN, or None if in dry-run mode.
        """
        input_payload = {
            "storm_id": cyclone.storm_id,
            "storm_name": cyclone.name,
            "storm_type": cyclone.storm_type,
            "basin": cyclone.basin,
            "advisory_number": gis_data.advisory_number,
            "advisory_guid": advisory.advisory_id,
            "center": {
                "lat": cyclone.center_lat,
                "lon": cyclone.center_lon,
            },
            "wind_mph": cyclone.wind_mph,
            "pressure_mb": cyclone.pressure_mb,
            "gis_data": {
                "s3_prefix": gis_data.s3_prefix,
                "products": {
                    name: {
                        "s3_key": prod.s3_key,
                        "downloaded": prod.downloaded,
                    }
                    for name, prod in gis_data.products.items()
                },
            },
            "triggered_at": datetime.utcnow().isoformat() + "Z",
        }

        if not self.sfn_client or not self.state_machine_arn:
            logger.info(
                f"[DRY RUN] Would trigger pipeline with:\n"
                f"{json.dumps(input_payload, indent=2)}"
            )
            return None

        execution_name = (
            f"{cyclone.storm_id}-adv{gis_data.advisory_number}-"
            f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        )

        response = self.sfn_client.start_execution(
            stateMachineArn=self.state_machine_arn,
            name=execution_name,
            input=json.dumps(input_payload),
        )

        arn = response["executionArn"]
        logger.info(f"Pipeline triggered: {arn}")
        return arn


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Core Detection Logic
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class StormDetector:
    """
    Orchestrates the storm detection workflow.

    This is the core class that ties together feed parsing,
    state tracking, GIS downloading, and pipeline triggering.
    """

    def __init__(
        self,
        config: StormDetectorConfig,
        feed_parser: NHCFeedParser,
        state_tracker: AdvisoryStateTracker,
        gis_downloader: GISDownloader,
        pipeline_trigger: PipelineTrigger,
    ):
        self.config = config
        self.feed_parser = feed_parser
        self.state_tracker = state_tracker
        self.gis_downloader = gis_downloader
        self.pipeline_trigger = pipeline_trigger

    def run(self) -> Dict[str, Any]:
        """
        Execute one full detection cycle.

        Returns a summary dict for Lambda response/logging.
        """
        summary = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "basins_checked": [],
            "active_cyclones": [],
            "new_advisories": [],
            "pipelines_triggered": [],
            "errors": [],
        }

        # Step 1: Fetch feeds for all active basins
        feed_results = self.feed_parser.fetch_all_basins()

        for result in feed_results:
            summary["basins_checked"].append(result.basin)

            if result.error:
                summary["errors"].append(
                    {"basin": result.basin, "error": result.error}
                )
                continue

            # Step 2: Process each active cyclone
            for storm_id, cyclone in result.cyclones.items():
                summary["active_cyclones"].append(
                    {
                        "storm_id": storm_id,
                        "name": cyclone.name,
                        "type": cyclone.storm_type,
                        "center": [cyclone.center_lat, cyclone.center_lon],
                    }
                )

                # Step 3: Check if this storm type qualifies for triggering
                if not self._is_triggerable(cyclone):
                    logger.info(
                        f"Skipping {cyclone.name} ({cyclone.storm_type}): "
                        f"does not meet trigger criteria"
                    )
                    continue

                # Step 4: Find new advisories for this storm
                storm_advisories = [
                    adv
                    for adv in result.advisories
                    if adv.cyclone
                    and adv.cyclone.atcf_id == cyclone.atcf_id
                ]

                for advisory in storm_advisories:
                    self._process_advisory(
                        cyclone, advisory, summary
                    )

        # Log summary
        n_new = len(summary["new_advisories"])
        n_triggered = len(summary["pipelines_triggered"])
        logger.info(
            f"Detection cycle complete: "
            f"{len(summary['active_cyclones'])} active cyclone(s), "
            f"{n_new} new advisory(ies), "
            f"{n_triggered} pipeline(s) triggered"
        )

        return summary

    def _process_advisory(
        self,
        cyclone: CycloneInfo,
        advisory: AdvisoryInfo,
        summary: dict,
    ) -> None:
        """Process a single advisory for a qualifying storm."""
        storm_id = cyclone.storm_id

        # Check if we've already processed this advisory
        if not self.state_tracker.is_new_advisory(storm_id, advisory):
            return

        logger.info(
            f"New advisory detected: {cyclone.name} — {advisory.title}"
        )
        summary["new_advisories"].append(
            {
                "storm_id": storm_id,
                "guid": advisory.advisory_id,
                "title": advisory.title,
                "has_watch_warning": advisory.has_watch_or_warning,
            }
        )

        # Extract advisory number from the title or GUID
        adv_num = self._extract_advisory_number(advisory)
        if not adv_num:
            logger.warning(
                f"Could not extract advisory number from: {advisory.title}"
            )
            adv_num = "001"  # Fallback

        # Download GIS data
        try:
            gis_data = self.gis_downloader.download_advisory_gis(
                cyclone, adv_num
            )
        except Exception as e:
            summary["errors"].append(
                {
                    "storm_id": storm_id,
                    "error": f"GIS download failed: {e}",
                }
            )
            logger.error(f"GIS download failed for {storm_id}: {e}")
            # Still mark as processed to avoid retry loops
            self.state_tracker.mark_processed(storm_id, advisory)
            return

        # Trigger the flood modeling pipeline
        execution_arn = None
        try:
            execution_arn = self.pipeline_trigger.trigger(
                cyclone, advisory, gis_data
            )
            if execution_arn:
                summary["pipelines_triggered"].append(
                    {
                        "storm_id": storm_id,
                        "execution_arn": execution_arn,
                    }
                )
        except Exception as e:
            summary["errors"].append(
                {
                    "storm_id": storm_id,
                    "error": f"Pipeline trigger failed: {e}",
                }
            )
            logger.error(f"Pipeline trigger failed for {storm_id}: {e}")

        # Mark advisory as processed
        self.state_tracker.mark_processed(
            storm_id, advisory, execution_arn
        )

    def _is_triggerable(self, cyclone: CycloneInfo) -> bool:
        """Check if a cyclone's type meets the trigger criteria."""
        return cyclone.storm_type in self.config.trigger_storm_types

    @staticmethod
    def _extract_advisory_number(advisory: AdvisoryInfo) -> Optional[str]:
        """
        Extract the advisory number from the title or GUID.

        NHC advisory titles often contain patterns like:
            "Advisory #12" or "Advisory #3A"
        GUIDs may contain patterns like:
            "...advisory_012..."
        """
        # Try title first: "Advisory #12", "Advisory #3A"
        match = re.search(
            r"Advisory\s*#?\s*(\d+[A-Za-z]?)", advisory.title, re.IGNORECASE
        )
        if match:
            return match.group(1).zfill(3)

        # Try GUID
        match = re.search(r"advisory[_-]?(\d+[A-Za-z]?)", advisory.guid, re.IGNORECASE)
        if match:
            return match.group(1).zfill(3)

        # Try description
        match = re.search(
            r"Advisory\s*(?:Number|#|No\.?)\s*(\d+[A-Za-z]?)",
            advisory.description,
            re.IGNORECASE,
        )
        if match:
            return match.group(1).zfill(3)

        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Lambda Entry Point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _build_detector() -> StormDetector:
    """
    Construct a StormDetector with all dependencies wired up.

    AWS clients are created lazily here so they're reused across
    warm Lambda invocations but don't break local development.
    """
    config = StormDetectorConfig()

    # AWS clients (only created if not in dry-run mode)
    dynamodb_resource = None
    s3_client = None
    sfn_client = None

    if not config.dry_run:
        import boto3

        dynamodb_resource = boto3.resource("dynamodb")
        s3_client = boto3.client("s3")
        sfn_client = boto3.client("stepfunctions")

    feed_parser = NHCFeedParser(config)

    state_tracker = AdvisoryStateTracker(
        table_name=config.state_table_name,
        dynamodb_resource=dynamodb_resource,
        dry_run=config.dry_run,
    )

    gis_downloader = GISDownloader(
        config=config,
        s3_client=s3_client,
    )

    pipeline_trigger = PipelineTrigger(
        sfn_client=sfn_client,
        state_machine_arn=config.pipeline_state_machine_arn,
    )

    return StormDetector(
        config=config,
        feed_parser=feed_parser,
        state_tracker=state_tracker,
        gis_downloader=gis_downloader,
        pipeline_trigger=pipeline_trigger,
    )


# Module-level detector instance for Lambda warm-start reuse
_detector: Optional[StormDetector] = None


def lambda_handler(event: dict, context: Any) -> dict:
    """
    AWS Lambda entry point.

    Invoked by EventBridge Scheduler every 15 minutes.
    Returns a summary of what was detected and triggered.
    """
    global _detector

    logger.info(f"Storm detector invoked. Event: {json.dumps(event)}")

    try:
        if _detector is None:
            _detector = _build_detector()

        summary = _detector.run()

        return {
            "statusCode": 200,
            "body": summary,
        }

    except Exception as e:
        logger.exception(f"Storm detector failed: {e}")
        return {
            "statusCode": 500,
            "body": {"error": str(e)},
        }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI Entry Point (Local Development)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def main():
    """Run the storm detector locally (dry-run mode)."""
    os.environ.setdefault("DRY_RUN", "true")
    os.environ.setdefault("LOG_LEVEL", "DEBUG")

    # Re-initialize logging for CLI
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        force=True,
    )

    result = lambda_handler({}, None)
    print("\n" + "=" * 60)
    print("DETECTION RESULT:")
    print("=" * 60)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
