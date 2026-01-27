import os
import random
import pyodbc
from faker import Faker
from dotenv import load_dotenv

# -----------------------------
# CONFIG
# -----------------------------
BATCH_SIZE = 10_000
CUSTOMERS_TOTAL = 900_000
PRODUCTS_TOTAL = 150_000

COMMIT_EVERY_N_BATCHES = 10  # commit every 10 batches for large tables

# Initialize faker + seeds (repeatable)
fake = Faker()
Faker.seed(42)
random.seed(42)

# Load env
load_dotenv()
SQL_HOST = os.getenv("SQL_SERVER_HOST")
SQL_DB = os.getenv("SQL_SERVER_DB", "TransactionDB_UAT")  # default to your DB name

if not SQL_HOST:
    raise ValueError(
        "SQL_SERVER_HOST is missing. Add it to your .env file, e.g.:\n"
        "SQL_SERVER_HOST=Inteli5SSD-Laptop\\SQLEXPRESS"
    )

print("=" * 70)
print("SQL SERVER DATA GENERATOR (1M+ ROWS)")
print("=" * 70)
print(f"Connecting to: {SQL_HOST}")
print(f"Database:      {SQL_DB}")
print("=" * 70)

# -----------------------------
# CONSTANT DATA
# -----------------------------
PRODUCT_NAMES = [
    "Wireless Bluetooth Headphones",
    "USB-C Charging Cable",
    "Portable Power Bank",
    "Laptop Stand",
    "Wireless Mouse",
    "Stainless Steel Water Bottle",
    "Coffee Maker",
    "Blender",
    "Non-Stick Frying Pan",
    "Kitchen Knife Set",
    "Cotton T-Shirt",
    "Denim Jeans",
    "Running Shoes",
    "Winter Jacket",
    "Baseball Cap",
    "Electric Toothbrush",
    "Yoga Mat",
    "Resistance Bands",
    "Face Moisturizer",
    "Shampoo & Conditioner Set",
    "Notebook Set",
    "Ballpoint Pens (Pack of 10)",
    "Desk Organizer",
    "Sticky Notes",
    "Printer Paper (500 Sheets)",
]

SUPPLIER_SUFFIXES = ["LLC", "Ltd", "PLC", "Inc", "Corp", "Co", "Group", "Industries", ""]
SUPPLIER_TYPES = [
    "Electronics", "Distribution", "Supply", "Manufacturing", "Trading",
    "Global", "International", "Wholesale", "Solutions", "Technologies"
]

def generate_supplier_name() -> str:
    """Generate realistic supplier/company names."""
    company_type = random.choice(["company", "business", "combined"])

    if company_type == "company":
        base = fake.company().replace(", Inc.", "").replace(", LLC", "").replace(", Ltd", "")
    elif company_type == "business":
        base = f"{fake.bs().title().split()[0]} {random.choice(SUPPLIER_TYPES)}"
    else:
        base = f"{fake.last_name()} {random.choice(SUPPLIER_TYPES)}"

    suffix = random.choice(SUPPLIER_SUFFIXES)
    return f"{base} {suffix}".strip()

def commit_periodically(conn, batch_num, total_inserted, label):
    """Commit every N batches for speed, and print progress."""
    if (batch_num + 1) % COMMIT_EVERY_N_BATCHES == 0:
        conn.commit()
        print(f"  ✓ Committed {total_inserted:,} {label}...")

# -----------------------------
# CONNECT
# -----------------------------
conn_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SQL_HOST};"
    f"DATABASE={SQL_DB};"
    "Trusted_Connection=yes;"
)

conn = None
cursor = None

try:
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()
    cursor.fast_executemany = True  # BIG speed boost for executemany

    print("✓ Connected to SQL Server\n")

    # -----------------------------
    # TABLE 1: Categories
    # -----------------------------
    print("Creating dbo.Categories ...")
    cursor.execute("IF OBJECT_ID('dbo.Categories', 'U') IS NOT NULL DROP TABLE dbo.Categories;")
    cursor.execute("""
        CREATE TABLE dbo.Categories (
            CategoryID INT IDENTITY(1,1) PRIMARY KEY,
            CategoryName NVARCHAR(50),
            Description NVARCHAR(MAX)
        );
    """)

    categories_data = [
        ("Electronics", "Electronic devices and accessories"),
        ("Clothing", "Apparel and fashion items"),
        ("Food", "Food and beverages"),
        ("Books", "Books and publications"),
        ("Toys", "Toys and games"),
        ("Sports", "Sports equipment and gear"),
        ("Home", "Home and garden products"),
        ("Beauty", "Beauty and personal care"),
    ]

    cursor.executemany(
        "INSERT INTO dbo.Categories (CategoryName, Description) VALUES (?, ?);",
        categories_data
    )
    conn.commit()
    print(f"✓ Categories inserted: {len(categories_data)} rows\n")

    # -----------------------------
    # TABLE 2: Suppliers
    # -----------------------------
    print("Creating dbo.Suppliers (5,000 rows) ...")
    cursor.execute("IF OBJECT_ID('dbo.Suppliers', 'U') IS NOT NULL DROP TABLE dbo.Suppliers;")
    cursor.execute("""
        CREATE TABLE dbo.Suppliers (
            SupplierID INT IDENTITY(1,1) PRIMARY KEY,
            SupplierName NVARCHAR(150),
            ContactName NVARCHAR(100),
            Country NVARCHAR(100),
            Phone NVARCHAR(20)
        );
    """)

    supplier_names = [generate_supplier_name() for _ in range(5000)]
    suppliers_data = [
        (supplier_names[i], fake.name(), fake.country()[:100], fake.phone_number()[:20])
        for i in range(5000)
    ]

    cursor.executemany(
        "INSERT INTO dbo.Suppliers (SupplierName, ContactName, Country, Phone) VALUES (?, ?, ?, ?);",
        suppliers_data
    )
    conn.commit()
    print("✓ Suppliers inserted: 5,000 rows\n")

    # -----------------------------
    # TABLE 3: Customers (900,000)
    # -----------------------------
    print("Creating dbo.Customers (900,000 rows - batching) ...")
    cursor.execute("IF OBJECT_ID('dbo.Customers', 'U') IS NOT NULL DROP TABLE dbo.Customers;")
    cursor.execute("""
        CREATE TABLE dbo.Customers (
            CustomerID INT IDENTITY(1,1) PRIMARY KEY,
            CustomerName NVARCHAR(100),
            Email NVARCHAR(100),
            Phone NVARCHAR(20),
            Country NVARCHAR(100),
            CreatedDate DATETIME,
            IsActive BIT
        );
    """)
    conn.commit()

    insert_customers_sql = """
        INSERT INTO dbo.Customers (CustomerName, Email, Phone, Country, CreatedDate, IsActive)
        VALUES (?, ?, ?, ?, ?, ?);
    """

    total_batches = CUSTOMERS_TOTAL // BATCH_SIZE
    total_inserted = 0

    for batch_num in range(total_batches):
        customers_batch = []

        for _ in range(BATCH_SIZE):
            # Dirty data #1: NULL CustomerName (~0.5%)
            if random.random() < 0.005:
                customer_name = None
                email = fake.email()
            else:
                customer_name = fake.name()
                name_parts = customer_name.lower().split()

                if len(name_parts) >= 2:
                    first_name = name_parts[0]
                    last_name = name_parts[-1]

                    # Dirty data #2: invalid email (~1%)
                    if random.random() < 0.01:
                        email = f"{first_name}.{last_name}@invalid"
                    else:
                        domain = random.choice(
                            ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "email.com", "mail.com"]
                        )
                        email = f"{first_name}.{last_name}@{domain}"
                else:
                    email = fake.email()

            # Dirty data #3: future CreatedDate (~1%)
            if random.random() < 0.01:
                created_date = fake.date_time_between(start_date="+1d", end_date="+30d")
            else:
                created_date = fake.date_time_between(start_date="-5y", end_date="now")

            customers_batch.append((
                customer_name,
                email,
                fake.phone_number()[:20],
                fake.country()[:100],
                created_date,
                random.choice([0, 1])
            ))

        cursor.executemany(insert_customers_sql, customers_batch)
        total_inserted += len(customers_batch)

        # commit every N batches for speed
        commit_periodically(conn, batch_num, total_inserted, "customers")

        if (batch_num + 1) % 10 == 0:
            print(f"  Inserted {total_inserted:,} customers...")

    conn.commit()
    print(f"✓ Customers inserted: {total_inserted:,} rows\n")

    # -----------------------------
    # TABLE 4: Products (150,000)
    # -----------------------------
    print("Creating dbo.Products (150,000 rows - batching) ...")
    cursor.execute("IF OBJECT_ID('dbo.Products', 'U') IS NOT NULL DROP TABLE dbo.Products;")
    cursor.execute("""
        CREATE TABLE dbo.Products (
            ProductID INT IDENTITY(1,1) PRIMARY KEY,
            ProductName NVARCHAR(200),
            CategoryID INT,
            SupplierID INT,
            UnitPrice MONEY,
            StockQuantity INT,
            CreatedDate DATETIME
        );
    """)
    conn.commit()

    insert_products_sql = """
        INSERT INTO dbo.Products (ProductName, CategoryID, SupplierID, UnitPrice, StockQuantity, CreatedDate)
        VALUES (?, ?, ?, ?, ?, ?);
    """

    total_batches = PRODUCTS_TOTAL // BATCH_SIZE
    total_inserted = 0

    for batch_num in range(total_batches):
        products_batch = []

        for i in range(BATCH_SIZE):
            # Dirty data #4: NULL ProductName (~0.2%)
            if random.random() < 0.002:
                product_name = None
            else:
                product_name = PRODUCT_NAMES[i % len(PRODUCT_NAMES)]

            # Dirty data #5: Negative UnitPrice (~0.5%)
            if random.random() < 0.005:
                unit_price = -random.uniform(10, 1000)
            else:
                unit_price = random.uniform(5, 2000)

            # Dirty data #6: Negative StockQuantity (~1%)
            if random.random() < 0.01:
                stock_quantity = -random.randint(1, 100)
            else:
                stock_quantity = random.randint(0, 1000)

            # Dirty data #7: Orphan SupplierID (~16%)
            # valid suppliers: 1..5000; orphaned: 5001..6000
            supplier_id = random.randint(1, 6000)

            products_batch.append((
                product_name,
                random.randint(1, 8),  # CategoryID valid range
                supplier_id,
                unit_price,
                stock_quantity,
                fake.date_time_between(start_date="-3y", end_date="now")
            ))

        cursor.executemany(insert_products_sql, products_batch)
        total_inserted += len(products_batch)

        commit_periodically(conn, batch_num, total_inserted, "products")

        if (batch_num + 1) % 5 == 0:
            print(f"  Inserted {total_inserted:,} products...")

    conn.commit()
    print(f"✓ Products inserted: {total_inserted:,} rows\n")

    # -----------------------------
    # FINAL SUMMARY
    # -----------------------------
    total_rows = len(categories_data) + 5000 + CUSTOMERS_TOTAL + PRODUCTS_TOTAL

    print("=" * 70)
    print("✓ DATA GENERATION COMPLETE!")
    print("=" * 70)
    print("Data created:")
    print(f"  Categories:       {len(categories_data):,} rows")
    print(f"  Suppliers:        {5000:,} rows")
    print(f"  Customers:        {CUSTOMERS_TOTAL:,} rows")
    print(f"  Products:         {PRODUCTS_TOTAL:,} rows")
    print(f"  {'-'*30}")
    print(f"  TOTAL:            {total_rows:,} rows")
    print("\nData quality issues included:")
    print("  ✓ NULL CustomerName (~0.5%)")
    print("  ✓ Invalid email formats (~1%)")
    print("  ✓ Future CreatedDate (~1%)")
    print("  ✓ NULL ProductName (~0.2%)")
    print("  ✓ Negative UnitPrice (~0.5%)")
    print("  ✓ Negative StockQuantity (~1%)")
    print("  ✓ Orphaned SupplierID (~16%)")
    print("=" * 70)

except pyodbc.Error as e:
    print("\n❌ SQL Server error occurred.")
    print(e)
    raise
except Exception as e:
    print("\n❌ General error occurred.")
    print(e)
    raise
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
