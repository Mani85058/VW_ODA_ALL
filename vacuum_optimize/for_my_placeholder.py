import pandas as pd
import random

# Sample data
names = ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Helen', 'Ian', 'Jane']
cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Seattle', 'Boston', 'Denver', 'Miami', 'Austin']

# Generate 20 rows
data = {
    'name': [random.choice(names) for _ in range(20)],
    'age': [random.randint(20, 60) for _ in range(20)],
    'city': [random.choice(cities) for _ in range(20)],
}

df = pd.DataFrame(data)
name_list = df["names"]
print(df)
