import os
import json
import psycopg2
from psycopg2 import errors
from flask import Flask, render_template_string

app = Flask(__name__)

DB_HOST = os.getenv("WAREHOUSE_DB_HOST", "warehouse-db")
DB_PORT = int(os.getenv("WAREHOUSE_DB_PORT", 5432))
DB_NAME = os.getenv("WAREHOUSE_DB_NAME", "warehouse")
DB_USER = os.getenv("WAREHOUSE_DB_USER", "warehouse")
DB_PASSWORD = os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse")

TEMPLATE = """
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Big Data News Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600&display=swap" rel="stylesheet">
    <style>
        :root {
            --primary: #6366f1;
            --secondary: #a855f7;
            --bg: #0f172a;
            --card-bg: rgba(30, 41, 59, 0.7);
            --text: #f8fafc;
            --accent: #22d3ee;
        }
        body {
            font-family: 'Outfit', sans-serif;
            background: var(--bg);
            background-image: radial-gradient(circle at top right, #1e1b4b, transparent), radial-gradient(circle at bottom left, #1e1b4b, transparent);
            color: var(--text);
            margin: 0;
            padding: 20px;
            min-height: 100vh;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        header {
            text-align: center;
            margin-bottom: 40px;
            padding: 20px;
            background: var(--card-bg);
            border-radius: 20px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255,255,255,0.1);
        }
        h1 { margin: 0; font-size: 2.5rem; background: linear-gradient(to right, var(--accent), var(--primary)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); gap: 24px; margin-bottom: 24px; }
        .card {
            background: var(--card-bg);
            padding: 24px;
            border-radius: 20px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255,255,255,0.1);
            transition: transform 0.3s ease;
        }
        .card:hover { transform: translateY(-5px); }
        .full-width { grid-column: 1 / -1; }
        h2 { font-size: 1.25rem; margin-top: 0; color: var(--accent); border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 10px; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid rgba(255,255,255,0.05); }
        th { color: var(--primary); font-weight: 600; }
        .stats-summary { display: flex; justify-content: space-around; text-align: center; margin-top: 20px; }
        .stat-item { flex: 1; }
        .stat-value { font-size: 2rem; font-weight: 600; color: var(--primary); }
        .stat-label { font-size: 0.875rem; opacity: 0.7; }
        canvas { max-height: 300px; }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>News Insight Analytics</h1>
            <p>Plateforme Big Data d'Analyse des Médias en Temps Réel</p>
        </header>

        <div class="grid">
            <!-- Résumé Rapide -->
            <div class="card full-width">
                <h2>Résumé du dernier run : {{ latest_run[0] if latest_run else 'N/A' }}</h2>
                <div class="stats-summary">
                    <div class="stat-item">
                        <div class="stat-value">{{ latest_run[1] if latest_run else 0 }}</div>
                        <div class="stat-label">Articles Collectés</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value">{{ top_keywords[0][0] if top_keywords else 'N/A' }}</div>
                        <div class="stat-label">Mot-Clé Principal</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value">{{ latest_run[3].strftime('%H:%M') if latest_run else '--:--' }}</div>
                        <div class="stat-label">Dernière Mise à Jour</div>
                    </div>
                </div>
            </div>

            <!-- Graphique Trends -->
            <div class="card full-width">
                <h2>Tendances d'Actualité (Articles par jour)</h2>
                <canvas id="trendsChart"></canvas>
            </div>

            <!-- Categories -->
            <div class="card">
                <h2>Articles par Catégorie</h2>
                <canvas id="categoryChart"></canvas>
            </div>

            <!-- Pays -->
            <div class="card">
                <h2>Articles par Pays / Zone</h2>
                <canvas id="countryChart"></canvas>
            </div>

            <!-- Keywords -->
            <div class="card">
                <h2>Top Sujets (Mots-Clés)</h2>
                <table>
                    <tr><th>Mot</th><th>Fréquence</th></tr>
                    {% for keyword, count in top_keywords[:8] %}
                    <tr><td>{{ keyword }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                </table>
            </div>

            <!-- Historique -->
            <div class="card">
                <h2>Historique des Runs</h2>
                <table>
                    <tr><th>Run ID</th><th>Articles</th><th>Heure</th></tr>
                    {% for row in summary %}
                    <tr>
                        <td>{{ row[0][-8:] }}...</td>
                        <td>{{ row[1] }}</td>
                        <td>{{ row[3].strftime('%Y-%m-%d %H:%M') }}</td>
                    </tr>
                    {% endfor %}
                </table>
            </div>
        </div>
    </div>

    <script>
        const ctxTrends = document.getElementById('trendsChart').getContext('2d');
        new Chart(ctxTrends, {
            type: 'line',
            data: {
                labels: {{ per_day_labels | tojson }},
                datasets: [{
                    label: 'Volume d\\'articles',
                    data: {{ per_day_values | tojson }},
                    borderColor: '#6366f1',
                    backgroundColor: 'rgba(99, 102, 241, 0.2)',
                    fill: true,
                    tension: 0.4
                }]
            },
            options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, grid: { color: 'rgba(255,255,255,0.05)' } }, x: { grid: { display: false } } } }
        });

        const ctxCat = document.getElementById('categoryChart').getContext('2d');
        new Chart(ctxCat, {
            type: 'doughnut',
            data: {
                labels: {{ cat_labels | tojson }},
                datasets: [{
                    data: {{ cat_values | tojson }},
                    backgroundColor: ['#6366f1', '#a855f7', '#22d3ee', '#f43f5e', '#fbbf24']
                }]
            },
            options: { responsive: true, plugins: { legend: { position: 'bottom', labels: { color: '#f8fafc' } } } }
        });

        const ctxCountry = document.getElementById('countryChart').getContext('2d');
        new Chart(ctxCountry, {
            type: 'bar',
            data: {
                labels: {{ country_labels | tojson }},
                datasets: [{
                    label: 'Articles',
                    data: {{ country_values | tojson }},
                    backgroundColor: '#22d3ee'
                }]
            },
            options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true, grid: { color: 'rgba(255,255,255,0.05)' } }, y: { grid: { display: false } } } }
        });
    </script>
</body>
</html>
"""

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

def init_db():
    create_table_statements = [
        "CREATE TABLE IF NOT EXISTS warehouse_summary (run_id TEXT PRIMARY KEY, total_articles INTEGER, generated_at TIMESTAMP, imported_at TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS warehouse_articles_by_source (run_id TEXT, source TEXT, count INTEGER, imported_at TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS warehouse_articles_by_category (run_id TEXT, category TEXT, count INTEGER, imported_at TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS warehouse_articles_by_language (run_id TEXT, language TEXT, count INTEGER, imported_at TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS warehouse_articles_per_day (run_id TEXT, day DATE, count INTEGER, imported_at TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS warehouse_top_keywords (run_id TEXT, keyword TEXT, count INTEGER, imported_at TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS warehouse_articles_by_country (run_id TEXT, country TEXT, count INTEGER, imported_at TIMESTAMP)"
    ]
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for statement in create_table_statements:
                cur.execute(statement)
        conn.commit()

def query_data():
    init_db() # Créer les tables si elles n'existent pas
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT run_id, total_articles, generated_at, imported_at FROM warehouse_summary ORDER BY imported_at DESC LIMIT 10")
                summary = cur.fetchall()
                latest_run = summary[0] if summary else None

                if latest_run:
                    rid = latest_run[0]
                    # By Source
                    cur.execute("SELECT source, SUM(count) FROM warehouse_articles_by_source WHERE run_id = %s GROUP BY source ORDER BY SUM(count) DESC", (rid,))
                    by_source = cur.fetchall()
                    # By Category
                    cur.execute("SELECT category, SUM(count) FROM warehouse_articles_by_category WHERE run_id = %s GROUP BY category ORDER BY SUM(count) DESC", (rid,))
                    by_category = cur.fetchall()
                    # Top Keywords
                    cur.execute("SELECT keyword, SUM(count) FROM warehouse_top_keywords WHERE run_id = %s GROUP BY keyword ORDER BY SUM(count) DESC LIMIT 20", (rid,))
                    top_keywords = cur.fetchall()
                    # By Country
                    cur.execute("SELECT country, SUM(count) FROM warehouse_articles_by_country WHERE run_id = %s GROUP BY country ORDER BY SUM(count) DESC", (rid,))
                    by_country = cur.fetchall()
                    # Per day (trends) - show last 14 days
                    cur.execute("SELECT day, SUM(count) FROM warehouse_articles_per_day GROUP BY day ORDER BY day DESC LIMIT 14")
                    per_day = sorted(cur.fetchall(), key=lambda x: x[0])
                else:
                    by_source, by_category, top_keywords, by_country, per_day = [], [], [], [], []

                return summary, latest_run, by_source, by_category, top_keywords, by_country, per_day
            except Exception as e:
                print(f"Error querying data: {e}")
                return [], None, [], [], [], [], []

@app.route("/")
def index():
    try:
        summary, latest_run, by_source, by_category, top_keywords, by_country, per_day = query_data()
        
        # Prepare Chart data
        cat_labels = [r[0] for r in by_category]
        cat_values = [r[1] for r in by_category]
        country_labels = [r[0] for r in by_country]
        country_values = [r[1] for r in by_country]
        per_day_labels = [r[0].strftime('%d/%m') if hasattr(r[0], 'strftime') else str(r[0]) for r in per_day]
        per_day_values = [r[1] for r in per_day]

    except Exception as exc:
        return f"Erreur : {exc}", 500
    
    return render_template_string(
        TEMPLATE,
        summary=summary,
        latest_run=latest_run,
        top_keywords=top_keywords,
        cat_labels=cat_labels,
        cat_values=cat_values,
        country_labels=country_labels,
        country_values=country_values,
        per_day_labels=per_day_labels,
        per_day_values=per_day_values
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050)
