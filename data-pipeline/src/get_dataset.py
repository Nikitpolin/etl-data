import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def get_dataset(rows=1000):
    """
    Генерация синтетических данных с аномалиями и историчностью типа 2 (SCD2)
    """
    np.random.seed(42)
    
    # Базовые данные
    users = [f'user_{i:04d}' for i in range(1, 101)]
    products = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
    regions = ['North', 'South', 'East', 'West', 'Central']
    statuses = ['active', 'inactive', 'pending']
    
    data = []
    
    for i in range(rows):
        user_id = random.choice(users)
        base_date = datetime(2023, 1, 1)
        
        # Историчность типа 2 - несколько записей для одного пользователя с разными периодами действия
        effective_from = base_date + timedelta(days=random.randint(0, 300))
        effective_to = effective_from + timedelta(days=random.randint(30, 365))
        
        record = {
            'user_id': user_id,
            'user_name': f'User {user_id.split("_")[1]}',
            'age': random.randint(18, 70),
            'salary': np.random.normal(50000, 20000),
            'purchase_amount': np.random.gamma(2, 50),
            'product_category': random.choice(products),
            'region': random.choice(regions),
            'customer_status': random.choice(statuses),
            'transaction_count': random.randint(1, 100),
            'effective_from': effective_from,
            'effective_to': effective_to,
            'current_flag': True if random.random() > 0.3 else False
        }
        
        # Добавление аномалий
        if random.random() < 0.05:  # 5% записей с отрицательной зарплатой
            record['salary'] = -abs(record['salary'])
        
        if random.random() < 0.03:  # 3% записей с пропущенными значениями
            record['age'] = None
            
        if random.random() < 0.04:  # 4% записей с некорректными датами
            record['effective_to'] = record['effective_from'] - timedelta(days=10)
            
        if random.random() < 0.02:  # 2% записей с очень большими значениями
            record['purchase_amount'] = record['purchase_amount'] * 1000
            
        if random.random() < 0.03:  # 3% записей с невалидными категориями
            record['product_category'] = 'Invalid_Category'
            
        data.append(record)
    
    df = pd.DataFrame(data)
    
    # Добавление дубликатов
    duplicates = df.sample(n=int(rows * 0.02))
    df = pd.concat([df, duplicates], ignore_index=True)
    
    # Ограничим значения, чтобы избежать переполнения
    df['salary'] = df['salary'].clip(lower=-1000000, upper=1000000)
    df['purchase_amount'] = df['purchase_amount'].clip(lower=-1000000, upper=1000000)
    df['age'] = df['age'].fillna(0).clip(lower=0, upper=150).replace(0, None)
    df['transaction_count'] = df['transaction_count'].clip(lower=0, upper=10000)
    
    return df