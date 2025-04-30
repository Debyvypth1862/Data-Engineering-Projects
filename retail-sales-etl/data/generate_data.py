import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Set random seed for reproducibility
np.random.seed(42)

# Generate data parameters
num_rows = 150000  # Generate 150K rows
num_customers = 1000
num_products = 500
start_date = datetime(2023, 1, 1)
end_date = datetime(2024, 12, 31)

# Generate random data
order_ids = range(1, num_rows + 1)
customer_ids = np.random.randint(101, 101 + num_customers, size=num_rows)
product_ids = np.random.randint(201, 201 + num_products, size=num_rows)
quantities = np.random.randint(1, 20, size=num_rows)

# Generate dates with more recent dates being more common
date_range = (end_date - start_date).days
weights = np.linspace(1, 2, date_range)  # More weight to recent dates
random_days = np.random.choice(range(date_range), size=num_rows, p=weights/sum(weights))
order_dates = [start_date + timedelta(days=int(x)) for x in random_days]

# Create DataFrame
df = pd.DataFrame({
    'order_id': order_ids,
    'customer_id': customer_ids,
    'product_id': product_ids,
    'quantity': quantities,
    'order_date': order_dates
})

# Sort by order_date
df = df.sort_values('order_date')

# Save to CSV
df.to_csv('raw_sales.csv', index=False)
print(f"Generated {num_rows} rows of sales data")