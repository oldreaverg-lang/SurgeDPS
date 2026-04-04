"""
NHC RSS Feed Parser

Fetches and parses the National Hurricane Center's basin-wide tropical
cyclone RSS feeds to detect active storms and new advisories.

Feed structure (from NHC XML namespace https://www.nhc.noaa.gov):
    <channel>
      <item>
        <title>Advisory text...</title>
        <description>...</description>
        <link>...</link>
        <guid>...</guid>
        <pubDate>...</pubDate>
        <nhc:Cyclone>
          <nhc:center>lat, lon</nhc:center>
          <nhc:type>Hurricane</nhc:type>
          <nhc:name>Milton</nhc:name>
          <nhc:wallet>AT4</nhc:wallet>
          <nhc:atcf>AL142024</nhc:atcf>
          <nhc:datetime>...</nhc:datetime>
          <nhc:movement>...</nhc:movement>
          <nhc:pressure>...</nhc:pressure>
          <nhc:wind>...</nhc:wind>
          <nhc:headline>...</nhc:headline>
        </nhc:Cyclone>
      </item>
      ...
    </channel>
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests

from .config import StormDetectorConfig

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Data Classes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class CycloneInfo:
    """Represents a single active tropical cyclone parsed from the NHC feed."""

    atcf_id: str  # e.g. "AL142024"
    name: str  # e.g. "Milton"
    storm_type: str  # e.g. "Hurricane"
    wallet: str  # e.g. "AT4"
    basin: str  # e.g. "at"
    center_lat: Optional[float] = None
    center_lon: Optional[float] = None
    movement: Optional[str] = None
    pressure_mb: Optional[int] = None
    wind_mph: Optional[int] = None
    headline: Optional[str] = None

    @property
    def storm_id(self) -> str:
        """Canonical storm identifier used for S3 paths and state keys."""
        return self.atcf_id.upper()

    @property
    def advisory_xml_url(self) -> str:
        """URL for the full advisory XML product."""
        return f"https://www.nhc.noaa.gov/xml/TCP{self.wallet}.xml"


@dataclass
class AdvisoryInfo:
    """Represents a single advisory/product from the RSS feed."""

    title: str
    link: str
    guid: str
    pub_date: str
    description: str = ""
    cyclone: Optional[CycloneInfo] = None

    @property
    def advisory_id(self) -> str:
        """Unique identifier for this advisory (the RSS guid)."""
        return self.guid

    @property
    def has_watch_or_warning(self) -> bool:
        """Check if this advisory mentions a watch or warning."""
        text = f"{self.title} {self.description}".lower()
        keywords = [
            "hurricane watch",
            "hurricane warning",
            "tropical storm watch",
            "tropical storm warning",
            "storm surge watch",
            "storm surge warning",
        ]
        return any(kw in text for kw in keywords)


@dataclass
class FeedResult:
    """Result of parsing one NHC RSS feed."""

    basin: str
    cyclones: Dict[str, CycloneInfo] = field(default_factory=dict)
    advisories: List[AdvisoryInfo] = field(default_factory=list)
    raw_xml: Optional[str] = None
    fetch_time: Optional[datetime] = None
    error: Optional[str] = None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Feed Parser
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class NHCFeedParser:
    """
    Parses NHC basin-wide tropical cyclone RSS feeds.

    Usage:
        config = StormDetectorConfig()
        parser = NHCFeedParser(config)
        result = parser.fetch_and_parse("at")
        for storm_id, cyclone in result.cyclones.items():
            print(f"{cyclone.name} ({cyclone.storm_type})")
    """

    def __init__(self, config: StormDetectorConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": config.user_agent})

    def fetch_and_parse(self, basin: str) -> FeedResult:
        """
        Fetch and parse the cyclone RSS feed for a given basin.

        Args:
            basin: Basin code ("at", "ep", or "cp")

        Returns:
            FeedResult with parsed cyclones and advisories
        """
        urls = self.config.feed_urls_for_basin(basin)
        feed_url = urls["cyclone_rss"]
        result = FeedResult(basin=basin, fetch_time=datetime.utcnow())

        logger.info(f"Fetching NHC feed: {feed_url}")

        try:
            response = self.session.get(
                feed_url, timeout=self.config.http_timeout
            )
            response.raise_for_status()
            result.raw_xml = response.text
        except requests.RequestException as e:
            result.error = f"Failed to fetch {feed_url}: {e}"
            logger.error(result.error)
            return result

        try:
            root = ET.fromstring(result.raw_xml)
            result.cyclones, result.advisories = self._parse_feed(root, basin)
        except ET.ParseError as e:
            result.error = f"Failed to parse XML from {feed_url}: {e}"
            logger.error(result.error)

        logger.info(
            f"Basin '{basin}': found {len(result.cyclones)} active cyclone(s), "
            f"{len(result.advisories)} advisory item(s)"
        )
        return result

    def fetch_all_basins(self) -> List[FeedResult]:
        """Fetch and parse feeds for all configured active basins."""
        results = []
        for basin in self.config.active_basins:
            basin = basin.strip().lower()
            results.append(self.fetch_and_parse(basin))
        return results

    # ── Internal Parsing ───────────────────────────────────────────

    def _parse_feed(
        self, root: ET.Element, basin: str
    ) -> Tuple[Dict[str, CycloneInfo], List[AdvisoryInfo]]:
        """
        Parse the XML root element of an NHC RSS feed.

        Returns:
            Tuple of (cyclones dict keyed by ATCF ID, list of advisories)
        """
        ns = self.config.nhc_namespace
        cyclones: Dict[str, CycloneInfo] = {}
        advisories: List[AdvisoryInfo] = []

        # Find all <item> elements in the RSS channel
        for item in root.iter("item"):
            advisory = self._parse_item(item, ns, basin)
            if advisory:
                advisories.append(advisory)
                # Track unique cyclones
                if advisory.cyclone and advisory.cyclone.atcf_id:
                    cyclones[advisory.cyclone.storm_id] = advisory.cyclone

        return cyclones, advisories

    def _parse_item(
        self, item: ET.Element, ns: dict, basin: str
    ) -> Optional[AdvisoryInfo]:
        """Parse a single <item> element from the RSS feed."""

        title = self._text(item, "title") or ""
        link = self._text(item, "link") or ""
        guid = self._text(item, "guid") or ""
        pub_date = self._text(item, "pubDate") or ""
        description = self._text(item, "description") or ""

        if not guid:
            return None

        advisory = AdvisoryInfo(
            title=title,
            link=link,
            guid=guid,
            pub_date=pub_date,
            description=description,
        )

        # Parse nhc:Cyclone element if present
        cyclone_elem = item.find("nhc:Cyclone", ns)
        if cyclone_elem is not None:
            advisory.cyclone = self._parse_cyclone(cyclone_elem, ns, basin)

        return advisory

    def _parse_cyclone(
        self, elem: ET.Element, ns: dict, basin: str
    ) -> Optional[CycloneInfo]:
        """Parse an nhc:Cyclone element into a CycloneInfo."""

        atcf_id = self._text(elem, "nhc:atcf", ns) or ""
        name = self._text(elem, "nhc:name", ns) or ""
        storm_type = self._text(elem, "nhc:type", ns) or ""
        wallet = self._text(elem, "nhc:wallet", ns) or ""

        if not atcf_id:
            logger.debug("Skipping cyclone element with no ATCF ID")
            return None

        cyclone = CycloneInfo(
            atcf_id=atcf_id.upper(),
            name=name.title(),
            storm_type=storm_type,
            wallet=wallet,
            basin=basin,
        )

        # Parse center coordinates: "lat, lon" format
        center_text = self._text(elem, "nhc:center", ns)
        if center_text:
            cyclone.center_lat, cyclone.center_lon = self._parse_center(
                center_text
            )

        # Parse optional fields
        cyclone.movement = self._text(elem, "nhc:movement", ns)
        cyclone.headline = self._text(elem, "nhc:headline", ns)

        pressure_text = self._text(elem, "nhc:pressure", ns)
        if pressure_text:
            cyclone.pressure_mb = self._parse_int(pressure_text)

        wind_text = self._text(elem, "nhc:wind", ns)
        if wind_text:
            cyclone.wind_mph = self._parse_int(wind_text)

        return cyclone

    # ── Utility Methods ────────────────────────────────────────────

    @staticmethod
    def _text(
        parent: ET.Element,
        tag: str,
        ns: Optional[dict] = None,
    ) -> Optional[str]:
        """Safely extract text from a child element."""
        elem = parent.find(tag, ns) if ns else parent.find(tag)
        if elem is not None and elem.text:
            return elem.text.strip()
        return None

    @staticmethod
    def _parse_center(text: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Parse NHC center coordinates.
        Expected format: "25.4, -80.2" or "25.4N 80.2W"
        """
        # Try comma-separated decimal format first
        if "," in text:
            parts = text.split(",")
            if len(parts) == 2:
                try:
                    return float(parts[0].strip()), float(parts[1].strip())
                except ValueError:
                    pass

        # Try "25.4N 80.2W" format
        match = re.match(
            r"([\d.]+)\s*([NS])\s+([\d.]+)\s*([EW])", text, re.IGNORECASE
        )
        if match:
            lat = float(match.group(1))
            if match.group(2).upper() == "S":
                lat = -lat
            lon = float(match.group(3))
            if match.group(4).upper() == "W":
                lon = -lon
            return lat, lon

        logger.warning(f"Could not parse center coordinates: '{text}'")
        return None, None

    @staticmethod
    def _parse_int(text: str) -> Optional[int]:
        """Extract the first integer from a string like '150 mph' or '940 mb'."""
        match = re.search(r"(\d+)", text)
        if match:
            return int(match.group(1))
        return None
