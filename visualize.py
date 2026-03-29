from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
from config import DB_CONFIG


engine = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

query = """
SELECT store, category, COUNT(*) AS total_apps
FROM apps
GROUP BY store, category
ORDER BY store, total_apps DESC;
"""

df = pd.read_sql(query, engine)

for store in df['store'].unique():
    subset = df[df['store'] == store]

    # Sort + keep top 10
    subset = subset.sort_values(by="total_apps", ascending=False).head(10)

    total = subset['total_apps'].sum()

    plt.figure()
    plt.bar(subset['category'], subset['total_apps'])
    plt.title(f"{store} (Top 10 Categories, Total: {total})")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"./images/{store}.png")
    plt.close()