"""Flask-приложение для отображения результатов."""

from flask import Flask, render_template
import psycopg2
import matplotlib.pyplot as plt
from io import BytesIO
import base64

app = Flask(__name__)

def get_db_connection():
    import yaml
    with open('config/postgres_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    return psycopg2.connect(
        dbname=config['database'],
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port']
    )

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/results')
def show_results():
    # Получение последних 10 фродовых транзакций
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM transactions WHERE fraud_flag = TRUE ORDER BY processed_at DESC LIMIT 10")
    fraud_transactions = cur.fetchall()
    # Получение последних 100 транзакций для гистограммы
    cur.execute("SELECT score FROM transactions ORDER BY processed_at DESC LIMIT 100")
    scores = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    # Генерация гистограммы
    plt.figure(figsize=(10, 6))
    plt.hist(scores, bins=20, color='skyblue', edgecolor='black')
    plt.title('Распределение скоров последних транзакций')
    plt.xlabel('Score')
    plt.ylabel('Количество')
    # Сохранение графика в base64
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    plot_data = base64.b64encode(buffer.getvalue()).decode('utf-8')
    plt.close()
    return render_template(
        'results.html',
        transactions=fraud_transactions,
        plot_url=plot_data
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)