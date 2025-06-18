from flask import Flask, render_template, request
import psycopg2
import matplotlib.pyplot as plt
import io
import base64
import seaborn as sns

app = Flask(__name__)

# Настройка подключения к PostgreSQL
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    dbname=os.getenv('POSTGRES_DB')
)
cur = conn.cursor()

@app.route('/')
def index():
    cur.execute('SELECT * FROM transactions WHERE fraud_flag = 1 ORDER BY transaction_id DESC LIMIT 10;')
    fraud_transactions = cur.fetchall()
    return render_template('index.html', transactions=fraud_transactions)

@app.route('/plot')
def plot():
    cur.execute('SELECT score FROM transactions ORDER BY transaction_id DESC LIMIT 100;')
    scores = [row[0] for row in cur.fetchall()]
    if not scores:
        return "Недостаточно данных для построения графика."
    
    plt.figure(figsize=(10, 6))
    sns.histplot(scores, bins=20, kde=True, color='royalblue')
    plt.title('Распределение скоров последних 100 транзакций')
    plt.xlabel('Скор')
    plt.ylabel('Частота')
    
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue()).decode()
    return '<img src="data:image/png;base64,{}">'.format(plot_url)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)