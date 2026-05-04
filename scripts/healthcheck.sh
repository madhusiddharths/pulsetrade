#!/bin/bash
# PulseTrade Day 1 Health Check
set -e
cd "$(dirname "$0")/.."

echo "==================================="
echo "  PulseTrade — Health Check"
echo "==================================="

# --- 1. Project structure ---
echo ""
echo "[1/8] Project structure"
for dir in producers api dashboard airflow databricks postgres infra scripts; do
  if [ -d "$dir" ]; then echo "  ✅ $dir/"; else echo "  ❌ $dir/ MISSING"; fi
done

# --- 2. .env file ---
echo ""
echo "[2/8] .env variables"
required_vars=(
  GOOGLE_API_KEY GROQ_API_KEY NEWSAPI_KEY
  KAFKA_BOOTSTRAP_SERVERS KAFKA_API_KEY KAFKA_API_SECRET
  DATABRICKS_HOST DATABRICKS_TOKEN
  TAVILY_API_KEY LANGSMITH_API_KEY POSTGRES_URL
)
for var in "${required_vars[@]}"; do
  val=$(grep "^$var=" .env 2>/dev/null | cut -d= -f2- | tr -d '[:space:]')
  if [ -z "$val" ]; then echo "  ❌ $var EMPTY"; else echo "  ✅ $var (${#val} chars)"; fi
done

# --- 3. .gitignore protects .env ---
echo ""
echo "[3/8] .env not in git"
if git check-ignore .env >/dev/null 2>&1; then
  echo "  ✅ .env properly gitignored"
else
  echo "  ❌ .env NOT gitignored — DO NOT COMMIT"
fi

# --- 4. Venvs exist ---
echo ""
echo "[4/8] Python venvs"
for venv in producers/.venv api/.venv dashboard/.venv; do
  if [ -d "$venv" ]; then echo "  ✅ $venv"; else echo "  ❌ $venv MISSING"; fi
done

# --- 5. Docker images ---
echo ""
echo "[5/8] Docker images"
for img in apache/airflow postgres redis; do
  if docker images --format '{{.Repository}}' | grep -q "$img"; then
    echo "  ✅ $img cached"
  else
    echo "  ❌ $img MISSING"
  fi
done

# --- 6. Postgres running ---
echo ""
echo "[6/8] Postgres container"
if docker ps --format '{{.Names}}' | grep -q "pulsetrade-postgres"; then
  echo "  ✅ pulsetrade-postgres running"
else
  echo "  ⚠️  pulsetrade-postgres not running (start with: cd postgres && docker compose start)"
fi

# --- 7. Kafka (producers venv) ---
echo ""
echo "[7/8] Kafka connectivity (producers venv)"
source producers/.venv/bin/activate 2>/dev/null
python <<'PYEOF' 2>&1 | grep -E "(✅|❌)"
import os
from dotenv import load_dotenv
load_dotenv('.env')
try:
    from confluent_kafka import Producer
    p = Producer({
        'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.environ['KAFKA_API_KEY'],
        'sasl.password': os.environ['KAFKA_API_SECRET'],
    })
    p.produce('stock-prices', key='HEALTHCHECK', value='ping')
    p.flush(timeout=10)
    print("  ✅ Kafka: produced test message")
except Exception as e:
    print(f"  ❌ Kafka: {e}")
PYEOF
deactivate 2>/dev/null

# --- 8. API/cloud connectivity (api venv) ---
echo ""
echo "[8/8] API connectivity (api venv)"
source api/.venv/bin/activate 2>/dev/null
python <<'PYEOF' 2>&1 | grep -E "(✅|❌)"
import os
from dotenv import load_dotenv
load_dotenv('.env')

# Gemini
try:
    from langchain_google_genai import ChatGoogleGenerativeAI
    llm = ChatGoogleGenerativeAI(model='gemini-2.5-flash')
    r = llm.invoke('reply with just OK')
    print(f"  ✅ Gemini: {r.content.strip()[:30]}")
except Exception as e:
    print(f"  ❌ Gemini: {e}")

# Groq
try:
    from langchain_groq import ChatGroq
    llm = ChatGroq(model='llama-3.3-70b-versatile')
    r = llm.invoke('reply with just OK')
    print(f"  ✅ Groq: {r.content.strip()[:30]}")
except Exception as e:
    print(f"  ❌ Groq: {e}")

# Databricks
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(host=os.environ['DATABRICKS_HOST'], token=os.environ['DATABRICKS_TOKEN'])
    print(f"  ✅ Databricks: {w.current_user.me().user_name}")
except Exception as e:
    print(f"  ❌ Databricks: {e}")

# Postgres
try:
    import psycopg2
    conn = psycopg2.connect(os.environ['POSTGRES_URL'], connect_timeout=5)
    cur = conn.cursor(); cur.execute('SELECT 1;'); cur.fetchone(); conn.close()
    print("  ✅ Postgres: connected")
except Exception as e:
    print(f"  ❌ Postgres: {e}")

# Tavily
try:
    from tavily import TavilyClient
    tc = TavilyClient(api_key=os.environ['TAVILY_API_KEY'])
    r = tc.search(query='test', max_results=1)
    print(f"  ✅ Tavily: got {len(r.get('results', []))} result(s)")
except Exception as e:
    print(f"  ❌ Tavily: {e}")

# NewsAPI
try:
    import requests
    r = requests.get(
        'https://newsapi.org/v2/top-headlines',
        params={'country': 'us', 'category': 'business', 'apiKey': os.environ['NEWSAPI_KEY']},
        timeout=10
    )
    j = r.json()
    if j.get('status') == 'ok':
        print(f"  ✅ NewsAPI: {j.get('totalResults', 0)} articles available")
    else:
        print(f"  ❌ NewsAPI: {j}")
except Exception as e:
    print(f"  ❌ NewsAPI: {e}")
PYEOF
deactivate 2>/dev/null

echo ""
echo "==================================="
echo "  Done."
echo "==================================="
