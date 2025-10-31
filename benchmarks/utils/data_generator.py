"""Test data generation utilities"""
import random
import string
from datetime import datetime, timedelta

def generate_users(count=1000):
    """Generate realistic user test data"""
    users = []
    for i in range(count):
        users.append({
            'id': i + 1,
            'name': f"User{i}",
            'email': f"user{i}@example.com",
            'age': random.randint(18, 80),
            'salary': random.uniform(30000, 150000),
            'department': random.choice(['Engineering', 'Sales', 'Marketing', 'HR']),
            'active': random.choice([True, False]),
            'created_at': datetime.now() - timedelta(days=random.randint(0, 365))
        })
    return users

def generate_orders(user_count=1000, orders_per_user=5):
    """Generate order test data"""
    orders = []
    order_id = 1
    for user_id in range(1, user_count + 1):
        for _ in range(random.randint(0, orders_per_user)):
            orders.append({
                'id': order_id,
                'user_id': user_id,
                'amount': random.uniform(10, 1000),
                'status': random.choice(['pending', 'completed', 'cancelled']),
                'created_at': datetime.now() - timedelta(days=random.randint(0, 365))
            })
            order_id += 1
    return orders

def generate_varchar_data(count, min_length=5, max_length=100):
    """Generate variable-length string data"""
    return [
        ''.join(random.choices(string.ascii_letters + string.digits,
                               k=random.randint(min_length, max_length)))
        for _ in range(count)
    ]
