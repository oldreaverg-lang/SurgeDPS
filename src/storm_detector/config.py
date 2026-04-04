"""
Configuration for the Storm Detector Lambda.

All tuneable parameters are defined here and can be overridden via
environment variables so the same code works locally and on Lambda.
"""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class StormDetectorConfig:
    """Immutable configuration for a single detector run."""

    # ── NHC Feed URLs ──────────────────────────────────────────────
    # Basin-wide tropical cyclone RSS feeds (XML)
    nhc_atlantic_rss: str = "https://www.nhc.noaa.gov/index-at.xml"
    nhc_east_pacific_rss: str = "https://www.nhc.noaa.gov/index-ep.xml"
    nhc_central_pacific_rss: str = "https://www.nhc.noaa.gov/index-cp.xml"

    # GIS-specific RSS feeds (contain shapefile download links)
    nhc_gis_atlantic_rss: str = "https://www.nhc.noaa.gov/gis-at.xml"
    nhc_gis_east_pacific_rss: str = "https://www.nhc.noaa.gov/gis-ep.xml"
    nhc_gis_central_pacific_rss: str = "https://www.nhc.noaa.gov/gis-cp.xml"

    # NHC XML namespace used in RSS feeds
    nhc_xml_namespace: str = "https://www.nhc.noaa.gov"

    # Base URL for NHC advisory XML products
    nhc_advisory_xml_base: str = "https://www.nhc.noaa.gov/xml/"

    # Base URL for NHC GIS shapefile downloads
    nhc_gis_base: str = "https://www.nhc.noaa.gov/gis/"

    # Base URL for NHC storm graphics API (KMZ, etc.)
    nhc_storm_graphics_api: str = "https://www.nhc.noaa.gov/storm_graphics/api/"

    # ── Basins to Monitor ──────────────────────────────────────────
    # Which basins to poll.  Default: Atlantic only (cheapest).
    # Options: "at" (Atlantic), "ep" (East Pacific), "cp" (Central Pacific)
    active_basins: List[str] = field(
        default_factory=lambda: os.getenv(
            "ACTIVE_BASINS", "at"
        ).split(",")
    )

    # ── Trigger Criteria ───────────────────────────────────────────
    # Storm types that should trigger the analysis pipeline.
    # NHC types: "Tropical Depression", "Tropical Storm", "Hurricane",
    #            "Post-Tropical Cyclone", "Subtropical Storm",
    #            "Subtropical Depression", "Potential Tropical Cyclone"
    trigger_storm_types: List[str] = field(
        default_factory=lambda: [
            "Tropical Storm",
            "Hurricane",
            "Potential Tropical Cyclone",
            "Subtropical Storm",
        ]
    )

    # Keywords in advisory titles/descriptions that indicate a
    # watch or warning has been issued (case-insensitive search).
    watch_warning_keywords: List[str] = field(
        default_factory=lambda: [
            "Hurricane Watch",
            "Hurricane Warning",
            "Tropical Storm Watch",
            "Tropical Storm Warning",
            "Storm Surge Watch",
            "Storm Surge Warning",
        ]
    )

    # ── AWS Resources ──────────────────────────────────────────────
    # DynamoDB table that tracks which advisories we've already seen.
    state_table_name: str = field(
        default_factory=lambda: os.getenv(
            "STATE_TABLE_NAME", "storm-detector-state"
        )
    )

    # S3 bucket for storing downloaded storm data and triggering
    # the downstream pipeline.
    data_bucket: str = field(
        default_factory=lambda: os.getenv("DATA_BUCKET", "surgedps-data")
    )

    # Step Functions state machine ARN for the flood modeling pipeline.
    pipeline_state_machine_arn: str = field(
        default_factory=lambda: os.getenv("PIPELINE_STATE_MACHINE_ARN", "")
    )

    # ── Networking ─────────────────────────────────────────────────
    # HTTP timeout in seconds for fetching NHC feeds and data.
    http_timeout: int = int(os.getenv("HTTP_TIMEOUT", "30"))

    # User-Agent header for NHC requests (be a good citizen).
    user_agent: str = (
        "SurgeDPS-StormDetector/1.0 (flood-risk-research; contact@surgedps.com)"
    )

    # ── Local Development ──────────────────────────────────────────
    # When True, skip DynamoDB and Step Functions calls (print instead).
    dry_run: bool = os.getenv("DRY_RUN", "false").lower() == "true"

    # Local directory for storing downloaded data during development.
    local_data_dir: str = os.getenv("LOCAL_DATA_DIR", "/tmp/surgedps")

    @property
    def nhc_namespace(self) -> dict:
        """Return the namespace dict for xml.etree.ElementTree lookups."""
        return {"nhc": self.nhc_xml_namespace}

    def feed_urls_for_basin(self, basin: str) -> dict:
        """Return the cyclone RSS and GIS RSS URLs for a given basin code."""
        mapping = {
            "at": {
                "cyclone_rss": self.nhc_atlantic_rss,
                "gis_rss": self.nhc_gis_atlantic_rss,
            },
            "ep": {
                "cyclone_rss": self.nhc_east_pacific_rss,
                "gis_rss": self.nhc_gis_east_pacific_rss,
            },
            "cp": {
                "cyclone_rss": self.nhc_central_pacific_rss,
                "gis_rss": self.nhc_gis_central_pacific_rss,
            },
        }
        if basin not in mapping:
            raise ValueError(
                f"Unknown basin '{basin}'. Expected one of: {list(mapping.keys())}"
            )
        return mapping[basin]
