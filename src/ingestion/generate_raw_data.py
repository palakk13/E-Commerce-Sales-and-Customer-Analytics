"""
Synthetic E-Commerce Raw Dataset Generator.

Generates realistic multi-table relational e-commerce data with controlled,
realistic data-quality anomalies to exercise downstream validation, quarantine,
cleaning, and PySpark transformation pipelines.

Entities:
- Customers (customers.csv)
- Products (products.csv)
- Orders (orders.csv)
- Order Items (order_items.csv)
"""

import csv
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

from config.logging_config import setup_logger
from config.pipeline_config import config

logger = setup_logger("data_generator")

# Set deterministic seed for absolute reproducibility
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# Sample reference data pools
FIRST_NAMES = [
    "Aarav", "Aditi", "Alexander", "Amara", "Carlos", "Chen", "David", "Elena",
    "Fatima", "Grace", "Hiroshi", "Ibrahim", "Isabella", "James", "Kavita", "Liam",
    "Maya", "Noah", "Olivia", "Priya", "Rahul", "Sophia", "Tariq", "Vikram", "Zara"
]
LAST_NAMES = [
    "Sharma", "Patel", "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee"
]
CITIES_BY_STATE = [
    ("Mumbai", "Maharashtra", "India", "400001"),
    ("Bengaluru", "Karnataka", "India", "560001"),
    ("Delhi", "Delhi", "India", "110001"),
    ("Hyderabad", "Telangana", "India", "500001"),
    ("Chennai", "Tamil Nadu", "India", "600001"),
    ("Pune", "Maharashtra", "India", "411001"),
    ("Kolkata", "West Bengal", "India", "700001"),
    ("Ahmedabad", "Gujarat", "India", "380001"),
    ("New York", "New York", "USA", "10001"),
    ("San Francisco", "California", "USA", "94102"),
    ("Austin", "Texas", "USA", "73301"),
    ("Seattle", "Washington", "USA", "98101"),
    ("Chicago", "Illinois", "USA", "60601"),
    ("London", "Greater London", "UK", "EC1A 1BB"),
    ("Manchester", "Greater Manchester", "UK", "M1 1AE"),
]

PRODUCT_CATEGORIES = {
    "Electronics": [
        ("Noise-Cancelling Headphones", 45.0, 99.99),
        ("Wireless Mechanical Keyboard", 35.0, 79.99),
        ("Ergonomic Gaming Mouse", 18.0, 44.99),
        ("Ultra-HD 27-inch Monitor", 140.0, 279.99),
        ("Fast Wireless Charger", 9.0, 24.99),
        ("Smart Fitness Watch", 55.0, 129.99),
        ("Bluetooth Portable Speaker", 22.0, 49.99),
        ("USB-C Multiport Dock", 28.0, 59.99),
    ],
    "Apparel": [
        ("Organic Cotton T-Shirt", 6.0, 19.99),
        ("Slim-Fit Stretch Jeans", 18.0, 49.99),
        ("Waterproof Trail Jacket", 40.0, 99.99),
        ("Merino Wool Sweater", 32.0, 79.99),
        ("Athletic Running Shorts", 10.0, 29.99),
        ("Breathable Crew Socks (3-Pack)", 3.5, 12.99),
    ],
    "Home & Kitchen": [
        ("Pour-Over Coffee Maker", 12.0, 34.99),
        ("Non-Stick Cast Iron Skillet", 20.0, 49.99),
        ("Stainless Steel Chef Knife 8-in", 15.0, 39.99),
        ("Ceramic Dinner Plate Set", 25.0, 59.99),
        ("Electric Milk Frother", 8.0, 21.99),
        ("Air Purifier HEPA Filter", 50.0, 119.99),
    ],
    "Sports & Outdoors": [
        ("High-Density Yoga Mat", 11.0, 29.99),
        ("Insulated Stainless Water Bottle", 7.0, 22.99),
        ("Adjustable Dumbbell Set (Pair)", 60.0, 149.99),
        ("Camping Dome Tent (2-Person)", 45.0, 109.99),
        ("LED Rechargeable Headlamp", 8.5, 24.99),
    ],
    "Books & Media": [
        ("Data Engineering with Python", 14.0, 39.99),
        ("Designing Data-Intensive Applications", 16.0, 45.00),
        ("Machine Learning Yearning", 12.0, 32.50),
        ("Clean Architecture Handbook", 15.0, 42.00),
        ("The Lean Startup Playbook", 9.0, 24.99),
    ],
}

PAYMENT_METHODS = ["Credit Card", "UPI", "Debit Card", "PayPal", "Net Banking"]
ORDER_STATUSES = ["Delivered", "Delivered", "Delivered", "Shipped", "Processing", "Cancelled", "Returned"]


def generate_customers(num_customers: int = 1000) -> List[Dict[str, str]]:
    """Generates customer profiles with minor realistic noise."""
    customers = []
    base_signup = datetime(2023, 1, 1)

    for i in range(1, num_customers + 1):
        cust_id = f"CUST_{i:05d}"
        fname = random.choice(FIRST_NAMES)
        lname = random.choice(LAST_NAMES)
        full_name = f"{fname} {lname}"

        # Inject minor formatting noise (leading/trailing whitespace)
        if random.random() < 0.03:
            full_name = f"  {full_name}  "

        email = f"{fname.lower()}.{lname.lower()}{i}@example.com"
        phone = f"+91-{random.randint(7000000000, 9999999999)}" if random.random() > 0.04 else ""

        city, state, country, postal = random.choice(CITIES_BY_STATE)
        signup_dt = base_signup + timedelta(days=random.randint(0, 900))
        signup_str = signup_dt.strftime("%Y-%m-%d")

        customers.append({
            "customer_id": cust_id,
            "customer_name": full_name,
            "email": email,
            "phone": phone,
            "city": city,
            "state": state,
            "country": country,
            "postal_code": postal,
            "signup_date": signup_str,
        })
    return customers


def generate_products() -> List[Dict[str, str]]:
    """Generates product catalog with category hierarchies."""
    products = []
    prod_counter = 1

    for category, items in PRODUCT_CATEGORIES.items():
        for prod_name, cost, price in items:
            prod_id = f"PROD_{prod_counter:04d}"
            sub_cat = prod_name.split()[0]  # Simplified sub-category

            # Inject minor casing noise in category
            cat_val = category
            if random.random() < 0.05:
                cat_val = category.lower()

            products.append({
                "product_id": prod_id,
                "product_name": prod_name,
                "category": cat_val,
                "sub_category": sub_cat,
                "unit_cost": f"{cost:.2f}",
                "list_price": f"{price:.2f}",
            })
            prod_counter += 1

    return products


def generate_orders_and_items(
    customers: List[Dict[str, str]],
    products: List[Dict[str, str]],
    num_orders: int = 4000,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], Dict[str, int]]:
    """
    Generates orders and order line items with deliberate data-quality anomalies
    to test the validation and quarantine engine.
    """
    orders = []
    order_items = []
    anomaly_counts = {
        "duplicate_order_id": 0,
        "missing_customer_id": 0,
        "invalid_date": 0,
        "invalid_quantity": 0,
        "negative_unit_price": 0,
        "invalid_product_fk": 0,
        "excess_discount": 0,
    }

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2026, 9, 30)
    total_days = (end_date - start_date).days

    cust_ids = [c["customer_id"] for c in customers]
    prod_map = {p["product_id"]: (float(p["unit_cost"]), float(p["list_price"])) for p in products}
    prod_ids = list(prod_map.keys())

    item_counter = 1

    for order_idx in range(1, num_orders + 1):
        order_id = f"ORD_{order_idx:06d}"
        cust_id = random.choice(cust_ids)
        order_dt = start_date + timedelta(days=random.randint(0, total_days))
        order_date_str = order_dt.strftime("%Y-%m-%d")
        status = random.choice(ORDER_STATUSES)
        pay_method = random.choice(PAYMENT_METHODS)
        shipping_cost = round(random.uniform(0.0, 15.0), 2)

        # Inject Anomaly 1: Missing customer_id (approx 1.5% of records)
        if random.random() < 0.015:
            cust_id = ""
            anomaly_counts["missing_customer_id"] += 1

        # Inject Anomaly 2: Invalid Date (approx 1% of records)
        if random.random() < 0.010:
            order_date_str = "2026-99-99" if random.random() < 0.5 else "INVALID_DATE"
            anomaly_counts["invalid_date"] += 1

        order_record = {
            "order_id": order_id,
            "customer_id": cust_id,
            "order_date": order_date_str,
            "order_status": status,
            "payment_method": pay_method,
            "shipping_cost": f"{shipping_cost:.2f}",
        }
        orders.append(order_record)

        # Inject Anomaly 3: Duplicate Order ID (duplicate order header)
        if random.random() < 0.010:
            duplicate_rec = order_record.copy()
            orders.append(duplicate_rec)
            anomaly_counts["duplicate_order_id"] += 1

        # Generate 1 to 4 line items per order
        num_items = random.choices([1, 2, 3, 4], weights=[0.55, 0.25, 0.15, 0.05])[0]

        for _ in range(num_items):
            item_id = f"ITEM_{item_counter:07d}"
            item_counter += 1
            chosen_prod_id = random.choice(prod_ids)
            _, list_p = prod_map[chosen_prod_id]

            # Realistic variation in unit price around list price
            unit_price = round(list_p * random.uniform(0.95, 1.05), 2)
            qty = random.randint(1, 4)
            discount = random.choices([0.0, 5.0, 10.0, 15.0, 20.0], weights=[0.6, 0.15, 0.1, 0.1, 0.05])[0]

            # Inject Anomaly 4: Invalid/Non-existent Product FK (approx 1% of items)
            if random.random() < 0.010:
                chosen_prod_id = "PROD_INVALID_9999"
                anomaly_counts["invalid_product_fk"] += 1

            # Inject Anomaly 5: Invalid Quantity (<= 0) (approx 1% of items)
            if random.random() < 0.010:
                qty = -1 if random.random() < 0.5 else 0
                anomaly_counts["invalid_quantity"] += 1

            # Inject Anomaly 6: Negative Unit Price (approx 0.8% of items)
            if random.random() < 0.008:
                unit_price = -abs(unit_price)
                anomaly_counts["negative_unit_price"] += 1

            # Inject Anomaly 7: Excess discount > 100% (approx 0.5% of items)
            if random.random() < 0.005:
                discount = 120.0
                anomaly_counts["excess_discount"] += 1

            order_items.append({
                "order_item_id": item_id,
                "order_id": order_id,
                "product_id": chosen_prod_id,
                "quantity": str(qty),
                "unit_price": f"{unit_price:.2f}",
                "discount_percent": f"{discount:.2f}",
            })

    return orders, order_items, anomaly_counts


def write_csv_file(path: Path, fieldnames: List[str], data: List[Dict[str, str]]) -> None:
    """Writes list of dicts to a CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    logger.info("Saved %d records to %s", len(data), path.resolve())


def run_data_generation(num_customers: int = 1000, num_orders: int = 4000) -> Dict[str, Any]:
    """
    Executes the entire synthetic raw data generation workflow and writes to data/raw/.
    """
    logger.info("Starting synthetic raw data generation (seed=%d)...", RANDOM_SEED)

    # 1. Generate Customers
    customers = generate_customers(num_customers)
    cust_path = config.paths.raw_dir / config.paths.customers_file
    write_csv_file(cust_path, list(customers[0].keys()), customers)

    # 2. Generate Products
    products = generate_products()
    prod_path = config.paths.raw_dir / config.paths.products_file
    write_csv_file(prod_path, list(products[0].keys()), products)

    # 3. Generate Orders & Order Items with injected anomalies
    orders, order_items, anomalies = generate_orders_and_items(
        customers=customers, products=products, num_orders=num_orders
    )
    orders_path = config.paths.raw_dir / config.paths.orders_file
    write_csv_file(orders_path, list(orders[0].keys()), orders)

    items_path = config.paths.raw_dir / config.paths.order_items_file
    write_csv_file(items_path, list(order_items[0].keys()), order_items)

    total_records = len(customers) + len(products) + len(orders) + len(order_items)
    summary = {
        "num_customers": len(customers),
        "num_products": len(products),
        "num_orders": len(orders),
        "num_order_items": len(order_items),
        "total_records": total_records,
        "injected_anomalies": anomalies,
    }

    logger.info("=== Raw Data Generation Completed Successfully ===")
    logger.info("Summary: %s", summary)
    return summary


if __name__ == "__main__":
    summary = run_data_generation()
    print("\n--- Data Generation Summary ---")
    for k, v in summary.items():
        print(f"  {k}: {v}")
