"""
Centralized Pipeline Configuration Module.

Loads parameters from config/config.yaml and environment variables (.env)
with sensible fallbacks and type-safe accessors.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import yaml

# Base Directory: root of ecommerce-data-engineering
BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env if present (with fallback if python-dotenv is not yet installed)
env_path = BASE_DIR / ".env"
try:
    from dotenv import load_dotenv
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
except ImportError:
    if env_path.exists():
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip("'\""))



def load_yaml_config(yaml_file: Path | str | None = None) -> Dict[str, Any]:
    """Loads configuration dictionary from YAML file."""
    if yaml_file is None:
        yaml_file = BASE_DIR / "config" / "config.yaml"
    else:
        yaml_file = Path(yaml_file)

    if not yaml_file.exists():
        return {}

    with open(yaml_file, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


_raw_yaml = load_yaml_config()


@dataclass
class PathConfig:
    """Filesystem path configurations."""
    base_dir: Path = BASE_DIR
    raw_dir: Path = BASE_DIR / _raw_yaml.get("paths", {}).get("raw_data_dir", "data/raw")
    processed_dir: Path = BASE_DIR / _raw_yaml.get("paths", {}).get("processed_data_dir", "data/processed")
    rejected_dir: Path = BASE_DIR / _raw_yaml.get("paths", {}).get("rejected_data_dir", "data/rejected")
    logs_dir: Path = BASE_DIR / _raw_yaml.get("paths", {}).get("logs_dir", "logs")
    sql_dir: Path = BASE_DIR / _raw_yaml.get("paths", {}).get("sql_dir", "sql")
    analytics_dir: Path = BASE_DIR / _raw_yaml.get("paths", {}).get("processed_data_dir", "data/processed") / "analytics"

    # File names
    customers_file: str = _raw_yaml.get("files", {}).get("customers_file", "customers.csv")
    products_file: str = _raw_yaml.get("files", {}).get("products_file", "products.csv")
    orders_file: str = _raw_yaml.get("files", {}).get("orders_file", "orders.csv")
    order_items_file: str = _raw_yaml.get("files", {}).get("order_items_file", "order_items.csv")
    quarantine_file: str = _raw_yaml.get("files", {}).get("quarantine_file", "quarantine_records.csv")

    def ensure_directories(self) -> None:
        """Creates all requisite directories if they do not exist."""
        for d in [self.raw_dir, self.processed_dir, self.rejected_dir, self.logs_dir, self.sql_dir, self.analytics_dir]:
            d.mkdir(parents=True, exist_ok=True)


@dataclass
class DatabaseConfig:
    """MySQL Data Warehouse connection settings."""
    dialect: str = _raw_yaml.get("database", {}).get("dialect", "mysql+pymysql")
    host: str = os.getenv("MYSQL_HOST", _raw_yaml.get("database", {}).get("host", "localhost"))
    port: int = int(os.getenv("MYSQL_PORT", _raw_yaml.get("database", {}).get("port", 3306)))
    database: str = os.getenv("MYSQL_DATABASE", _raw_yaml.get("database", {}).get("database", "ecommerce_dwh"))
    user: str = os.getenv("MYSQL_USER", _raw_yaml.get("database", {}).get("user", "ecommerce_user"))
    password: str = os.getenv("MYSQL_PASSWORD", "ecommerce_pass")
    pool_size: int = _raw_yaml.get("database", {}).get("pool_size", 5)
    max_overflow: int = _raw_yaml.get("database", {}).get("max_overflow", 10)

    @property
    def connection_url(self) -> str:
        """Returns SQLAlchemy connection URI."""
        return f"{self.dialect}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def server_url(self) -> str:
        """Returns SQLAlchemy connection URI without database name (for DB creation)."""
        return f"{self.dialect}://{self.user}:{self.password}@{self.host}:{self.port}"


@dataclass
class SparkConfig:
    """Apache Spark session configurations."""
    app_name: str = os.getenv("SPARK_APP_NAME", _raw_yaml.get("spark", {}).get("app_name", "EcommerceDataPlatform"))
    master: str = os.getenv("SPARK_MASTER", _raw_yaml.get("spark", {}).get("master", "local[*]"))
    driver_memory: str = os.getenv("SPARK_DRIVER_MEMORY", _raw_yaml.get("spark", {}).get("driver_memory", "2g"))
    shuffle_partitions: str = _raw_yaml.get("spark", {}).get("shuffle_partitions", "4")


@dataclass
class ValidationConfig:
    """Thresholds and column integrity expectations."""
    mandatory_customer_fields: List[str] = field(
        default_factory=lambda: _raw_yaml.get("validation", {}).get(
            "mandatory_customer_fields", ["customer_id", "customer_name", "signup_date"]
        )
    )
    mandatory_product_fields: List[str] = field(
        default_factory=lambda: _raw_yaml.get("validation", {}).get(
            "mandatory_product_fields", ["product_id", "product_name", "category", "unit_cost", "list_price"]
        )
    )
    mandatory_order_fields: List[str] = field(
        default_factory=lambda: _raw_yaml.get("validation", {}).get(
            "mandatory_order_fields", ["order_id", "customer_id", "order_date", "order_status"]
        )
    )
    mandatory_item_fields: List[str] = field(
        default_factory=lambda: _raw_yaml.get("validation", {}).get(
            "mandatory_item_fields", ["order_item_id", "order_id", "product_id", "quantity", "unit_price"]
        )
    )
    min_quantity: int = _raw_yaml.get("validation", {}).get("thresholds", {}).get("min_quantity", 1)
    max_quantity: int = _raw_yaml.get("validation", {}).get("thresholds", {}).get("max_quantity", 500)
    min_price: float = _raw_yaml.get("validation", {}).get("thresholds", {}).get("min_price", 0.01)
    max_discount_pct: float = _raw_yaml.get("validation", {}).get("thresholds", {}).get("max_discount_pct", 100.0)


@dataclass
class MLConfig:
    """Customer segmentation and modeling configurations."""
    snapshot_date: str = _raw_yaml.get("ml", {}).get("rfm", {}).get("snapshot_date", "2026-10-08")
    k_range_min: int = _raw_yaml.get("ml", {}).get("rfm", {}).get("k_range_min", 2)
    k_range_max: int = _raw_yaml.get("ml", {}).get("rfm", {}).get("k_range_max", 6)
    random_state: int = _raw_yaml.get("ml", {}).get("rfm", {}).get("random_state", 42)
    default_clusters: int = _raw_yaml.get("ml", {}).get("rfm", {}).get("default_clusters", 4)


@dataclass
class PipelineConfig:
    """Master Pipeline Configuration aggregation."""
    name: str = _raw_yaml.get("pipeline", {}).get("name", "ecommerce_data_platform")
    version: str = _raw_yaml.get("pipeline", {}).get("version", "1.0.0")
    paths: PathConfig = field(default_factory=PathConfig)
    db: DatabaseConfig = field(default_factory=DatabaseConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    ml: MLConfig = field(default_factory=MLConfig)

    @property
    def database(self) -> DatabaseConfig:
        """Alias for self.db for compatibility with warehouse loader."""
        return self.db


# Global configuration singleton
config = PipelineConfig()
config.paths.ensure_directories()

