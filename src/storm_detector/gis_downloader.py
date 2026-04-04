"""
NHC GIS Data Downloader

Downloads forecast cone, track, wind radii, and watch/warning shapefiles
from the National Hurricane Center for a specific storm advisory.

NHC GIS shapefile URL patterns:
    Forecast Track + Cone (5-day):
        https://www.nhc.noaa.gov/gis/forecast/archive/{atcf_id}_5day_{adv_num}.zip
    Watches/Warnings:
        https://www.nhc.noaa.gov/gis/forecast/archive/{atcf_id}_ww_wwlin{adv_num}.zip
    Wind Speed Probabilities:
        https://www.nhc.noaa.gov/gis/forecast/archive/wsp_120hr{adv_num}km.zip
    Initial Wind Field:
        https://www.nhc.noaa.gov/gis/forecast/archive/{atcf_id}_init_{adv_num}.zip
    Storm Surge Watch/Warning:
        https://www.nhc.noaa.gov/gis/forecast/archive/{atcf_id}_5day_pgn_{adv_num}.zip

ATCF ID format: AL142024 (basin + number + year)
Advisory number format: "003" (zero-padded to 3 digits), "003A" for intermediates
"""

from __future__ import annotations

import io
import logging
import os
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import requests

from .config import StormDetectorConfig
from .nhc_feed import CycloneInfo

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Data Classes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@dataclass
class GISProduct:
    """Metadata for a single GIS shapefile product."""

    product_type: str  # "forecast_cone", "track", "watches_warnings", etc.
    url: str
    local_path: Optional[str] = None
    s3_key: Optional[str] = None
    downloaded: bool = False
    error: Optional[str] = None


@dataclass
class AdvisoryGISData:
    """All GIS products downloaded for a single advisory."""

    storm_id: str  # e.g. "AL142024"
    advisory_number: str  # e.g. "003" or "003A"
    products: Dict[str, GISProduct] = field(default_factory=dict)
    local_dir: Optional[str] = None
    s3_prefix: Optional[str] = None

    @property
    def is_complete(self) -> bool:
        """True if the critical products (cone) were downloaded."""
        cone = self.products.get("forecast_cone")
        return cone is not None and cone.downloaded

    @property
    def cone_path(self) -> Optional[str]:
        """Path to the downloaded forecast cone shapefile directory."""
        cone = self.products.get("forecast_cone")
        if cone and cone.local_path:
            return cone.local_path
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# URL Builder
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class NHCGISURLBuilder:
    """
    Constructs download URLs for NHC GIS products.

    The URL patterns are based on observed NHC archive structure and
    may need updating if NHC changes their file naming conventions.
    """

    BASE_URL = "https://www.nhc.noaa.gov/gis/forecast/archive"

    @classmethod
    def forecast_cone_url(cls, atcf_id: str, advisory_num: str) -> str:
        """5-day forecast track and cone of uncertainty shapefile."""
        return f"{cls.BASE_URL}/{atcf_id.lower()}_5day_{advisory_num}.zip"

    @classmethod
    def watches_warnings_url(cls, atcf_id: str, advisory_num: str) -> str:
        """Coastal watches and warnings shapefile."""
        return f"{cls.BASE_URL}/{atcf_id.lower()}_ww_wwlin{advisory_num}.zip"

    @classmethod
    def wind_field_url(cls, atcf_id: str, advisory_num: str) -> str:
        """Initial wind field (34/50/64 kt wind radii) shapefile."""
        return f"{cls.BASE_URL}/{atcf_id.lower()}_init_{advisory_num}.zip"

    @classmethod
    def surge_watch_warning_url(
        cls, atcf_id: str, advisory_num: str
    ) -> str:
        """Storm surge watch/warning polygon shapefile."""
        return f"{cls.BASE_URL}/{atcf_id.lower()}_5day_pgn_{advisory_num}.zip"

    @classmethod
    def all_product_urls(
        cls, atcf_id: str, advisory_num: str
    ) -> Dict[str, str]:
        """Return all known GIS product URLs for a storm advisory."""
        return {
            "forecast_cone": cls.forecast_cone_url(atcf_id, advisory_num),
            "watches_warnings": cls.watches_warnings_url(
                atcf_id, advisory_num
            ),
            "wind_field": cls.wind_field_url(atcf_id, advisory_num),
            "surge_watch_warning": cls.surge_watch_warning_url(
                atcf_id, advisory_num
            ),
        }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Downloader
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class GISDownloader:
    """
    Downloads and extracts NHC GIS shapefiles for a storm advisory.

    Can store files locally (for Lambda /tmp or dev) and/or upload
    to S3 for the downstream pipeline.
    """

    def __init__(
        self,
        config: StormDetectorConfig,
        s3_client=None,
    ):
        self.config = config
        self.s3_client = s3_client
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": config.user_agent})

    def download_advisory_gis(
        self,
        cyclone: CycloneInfo,
        advisory_number: str,
        output_dir: Optional[str] = None,
    ) -> AdvisoryGISData:
        """
        Download all GIS products for a specific storm advisory.

        Args:
            cyclone: CycloneInfo from the feed parser
            advisory_number: Advisory number (e.g. "003", "012A")
            output_dir: Local directory to extract files into.
                        Defaults to config.local_data_dir.

        Returns:
            AdvisoryGISData with download status for each product
        """
        storm_id = cyclone.storm_id
        adv_num = advisory_number.zfill(3)  # Zero-pad to 3 digits

        # Set up output directory
        base_dir = output_dir or self.config.local_data_dir
        local_dir = os.path.join(base_dir, storm_id, f"advisory_{adv_num}")
        os.makedirs(local_dir, exist_ok=True)

        s3_prefix = f"storms/{storm_id}/advisory_{adv_num}"

        gis_data = AdvisoryGISData(
            storm_id=storm_id,
            advisory_number=adv_num,
            local_dir=local_dir,
            s3_prefix=s3_prefix,
        )

        # Build URLs for all products
        urls = NHCGISURLBuilder.all_product_urls(storm_id, adv_num)

        # Download each product (forecast_cone is required, others optional)
        for product_type, url in urls.items():
            product = GISProduct(product_type=product_type, url=url)
            required = product_type == "forecast_cone"

            try:
                product_dir = os.path.join(local_dir, product_type)
                self._download_and_extract(url, product_dir)
                product.local_path = product_dir
                product.downloaded = True
                logger.info(f"Downloaded {product_type}: {url}")

                # Upload to S3 if client is available
                if self.s3_client and not self.config.dry_run:
                    product.s3_key = f"{s3_prefix}/{product_type}"
                    self._upload_directory_to_s3(
                        product_dir, product.s3_key
                    )

            except requests.HTTPError as e:
                product.error = str(e)
                if required:
                    logger.error(
                        f"REQUIRED product {product_type} failed: {e}"
                    )
                else:
                    # Many optional products aren't always available
                    logger.info(
                        f"Optional product {product_type} not available: "
                        f"{e.response.status_code if e.response else e}"
                    )
            except Exception as e:
                product.error = str(e)
                logger.error(f"Error downloading {product_type}: {e}")

            gis_data.products[product_type] = product

        return gis_data

    def _download_and_extract(self, url: str, output_dir: str) -> None:
        """Download a ZIP file and extract its contents."""
        response = self.session.get(url, timeout=self.config.http_timeout)
        response.raise_for_status()

        os.makedirs(output_dir, exist_ok=True)

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            # Validate zip contents (security: no path traversal)
            for name in zf.namelist():
                if name.startswith("/") or ".." in name:
                    raise ValueError(
                        f"Suspicious path in ZIP: {name}"
                    )
            zf.extractall(output_dir)

        logger.debug(
            f"Extracted {len(zf.namelist())} files to {output_dir}"
        )

    def _upload_directory_to_s3(
        self, local_dir: str, s3_prefix: str
    ) -> None:
        """Upload all files in a directory to S3."""
        bucket = self.config.data_bucket
        for root, _dirs, files in os.walk(local_dir):
            for filename in files:
                local_path = os.path.join(root, filename)
                relative_path = os.path.relpath(local_path, local_dir)
                s3_key = f"{s3_prefix}/{relative_path}"
                self.s3_client.upload_file(local_path, bucket, s3_key)
                logger.debug(f"Uploaded to s3://{bucket}/{s3_key}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Cone Geometry Extractor
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def extract_cone_geometry(shapefile_dir: str) -> Optional[dict]:
    """
    Read the forecast cone shapefile and return the cone polygon
    as a GeoJSON-like dict.

    This is used downstream to define the spatial processing extent
    for DEM clipping, NWM reach selection, etc.

    Requires fiona (imported here to keep it optional for the
    detector Lambda, which may not need it).
    """
    try:
        import fiona
    except ImportError:
        logger.warning(
            "fiona not installed — cannot extract cone geometry. "
            "Install with: pip install fiona"
        )
        return None

    # Find the .shp file in the directory
    shp_files = [
        f for f in os.listdir(shapefile_dir)
        if f.lower().endswith(".shp") and "pgn" in f.lower()
    ]

    if not shp_files:
        # Fall back: look for any .shp file with "5day" in the name
        shp_files = [
            f for f in os.listdir(shapefile_dir)
            if f.lower().endswith(".shp")
        ]

    if not shp_files:
        logger.error(f"No shapefiles found in {shapefile_dir}")
        return None

    shp_path = os.path.join(shapefile_dir, shp_files[0])
    logger.info(f"Reading cone geometry from: {shp_path}")

    with fiona.open(shp_path) as src:
        # The cone is typically the largest polygon in the file.
        # NHC forecast cone shapefiles usually contain one feature
        # for the 5-day cone polygon.
        features = list(src)
        if not features:
            logger.error("No features in cone shapefile")
            return None

        # Return the geometry of the first (and usually only) polygon
        cone_geom = features[0]["geometry"]
        logger.info(
            f"Extracted cone geometry: type={cone_geom['type']}, "
            f"{len(features)} feature(s) total"
        )
        return cone_geom
