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
    total = subset['total_apps'].sum()

    plt.figure()
    plt.bar(subset['category'], subset['total_apps'])

    plt.text(
        0.95, 0.95,
        f"Total: {total}",
        transform=plt.gca().transAxes,
        ha='right',
        va='top'
    )

    plt.title(f"Apps per Category - {store}")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"./images/{store}.png")
    plt.close()