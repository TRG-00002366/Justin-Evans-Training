Data format (JSON, CSV, etc.)


Update frequency (real-time, daily, etc.)
Key fields that need transformation
Potential data quality issues


{
  "event_id": "e-abc123",
  "event_type": "page_view",
  "timestamp": "2024-01-15T10:30:00Z",
  "user_id": "u-12345",
  "session_id": "s-99887",
  "page_url": "/products/laptop-pro",
  "device": "mobile",
  "properties": {
    "referrer": "google.com",
    "product_id": "p-001"
  }
}


order_id, customer_id, order_date, status, total_amount, shipping_address, payment_method


{
  "product_id": "p-001",
  "name": "Laptop Pro 15",
  "category": "Electronics",
  "subcategory": "Laptops",
  "price": 1299.99,
  "inventory_count": 45,
  "attributes": {
    "brand": "TechBrand",
    "weight_kg": 1.8
  }
}