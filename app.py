import csv
import os
from flask import Flask, request
import pandas as pd
import psycopg2

app = Flask(__name__)
app.debug = True

def etl():
    # Load CSV files
    users = pd.read_csv('data/users.csv',',\t')
    user_experiments = pd.read_csv('data/user_experiments.csv',',\t')
    compounds = pd.read_csv('data/compounds.csv',',\t')

    # Process files to derive features
    total_experiments = user_experiments.groupby('user_id').size()
    average_experiments = total_experiments.mean()
    most_common_compound = user_experiments['experiment_compound_ids'].str.split(';').explode().mode()[0]

    # Upload processed data into a database
    conn = psycopg2.connect(
        host='host.docker.internal',
        port=5432,
        database='database',
        user='root',
        password='Root123'
    )

    cur = conn.cursor()

    # Create table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS derived_features (
            user_id INTEGER PRIMARY KEY,
            total_experiments INTEGER,
            average_experiments FLOAT,
            most_common_compound VARCHAR(255)
        );
    """)

    # Insert or update data in the table
    for user_id, total_exp in total_experiments.items():
        cur.execute("""
            INSERT INTO derived_features (user_id, total_experiments, average_experiments, most_common_compound)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
            SET total_experiments = excluded.total_experiments,
                average_experiments = excluded.average_experiments,
                most_common_compound = excluded.most_common_compound;
        """, (user_id, total_exp, average_experiments, most_common_compound))

    conn.commit()
    cur.close()
    conn.close()

    return 'ETL process completed successfully.'


# Your API that can be called to trigger your ETL process
@app.route('/etl', methods=['POST'])
def trigger_etl():
    # Trigger your ETL process here
    etl()
    return {"message": "ETL process started"}, 200

@app.route('/')
def hello_world():
    return "HI"  

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)

