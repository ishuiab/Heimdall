"""
Lightweight Flask application for viewing trading orders
Uses HTMX for dynamic updates without heavy JavaScript frameworks
"""

from flask import Flask, render_template, request, jsonify
import psycopg2
import os
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
from config import Config
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)
app.config["APPLICATION_ROOT"] = "/heimdall"
app.wsgi_app = ProxyFix(app.wsgi_app, x_prefix=1)

def get_db_connection():
    """Create a database connection"""
    return psycopg2.connect(
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        database=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD
    )

def execute_query(query, params=None):
    """Execute a query and return results"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params or ())
            if cur.description:
                return cur.fetchall()
            return []
    finally:
        conn.close()

# Available brokers (only Shoonya for now)
BROKERS = [
    {"id": "shoonya", "name": "Shoonya", "table": "shoonya_orders"}
]

# Config files directory
CONFIG_DIR = "/home/algobaba/DATALORE/hypotheis/FMV_SCALPER/configs"

@app.route("/")
def index():
    """Main page with filter dropdowns"""
    return render_template("index.html", brokers=BROKERS)

@app.route("/api/accounts")
def get_accounts():
    """Get unique accounts for a broker"""
    broker = request.args.get("broker", "shoonya")
    
    query = f"""
        SELECT DISTINCT account 
        FROM "{Config.DB_SCHEMA}".shoonya_orders 
        ORDER BY account
    """
    accounts = execute_query(query)
    return jsonify([row["account"] for row in accounts])

@app.route("/api/dates")
def get_dates():
    """Get unique dates for an account"""
    account = request.args.get("account")
    
    if not account:
        return jsonify([])
    
    query = f"""
        SELECT DISTINCT DATE(created_at) as order_date
        FROM "{Config.DB_SCHEMA}".shoonya_orders
        WHERE account = %s
        ORDER BY order_date DESC
    """
    dates = execute_query(query, (account,))
    return jsonify([row["order_date"].isoformat() for row in dates if row["order_date"]])

@app.route("/api/symbols")
def get_symbols():
    """Get unique symbols for an account and date"""
    account = request.args.get("account")
    order_date = request.args.get("date")
    
    if not account:
        return jsonify([])
    
    query = f"""
        SELECT DISTINCT symbol
        FROM "{Config.DB_SCHEMA}".shoonya_orders
        WHERE account = %s
    """
    params = [account]
    
    if order_date:
        query += " AND DATE(created_at) = %s"
        params.append(order_date)
    
    query += " ORDER BY symbol"
    
    symbols = execute_query(query, tuple(params))
    return jsonify([row["symbol"] for row in symbols])

@app.route("/api/statuses")
def get_statuses():
    """Get unique statuses"""
    account = request.args.get("account")
    
    query = f"""
        SELECT DISTINCT status
        FROM "{Config.DB_SCHEMA}".shoonya_orders
        WHERE 1=1
    """
    params = []
    
    if account:
        query += " AND account = %s"
        params.append(account)
    
    query += " ORDER BY status"
    
    statuses = execute_query(query, tuple(params))
    return jsonify([row["status"] for row in statuses])

@app.route("/api/orders")
def get_orders():
    """Get orders based on filters"""
    broker = request.args.get("broker", "shoonya")
    account = request.args.get("account")
    order_date = request.args.get("date")
    symbols = request.args.getlist("symbol")  # Multiple symbols
    statuses = request.args.getlist("status")  # Multiple statuses
    
    query = f"""
        SELECT 
            order_id,
            symbol,
            exchange,
            transaction_type,
            price,
            qty,
            status,
            order_type,
            product_type,
            order_time,
            remarks,
            spl_remarks,
            rejection_reason,
            account,
            created_at,
            exit_time,
            total_order_time
        FROM "{Config.DB_SCHEMA}".shoonya_orders
        WHERE 1=1
    """
    params = []
    
    if account:
        query += " AND account = %s"
        params.append(account)
    
    if order_date:
        query += " AND DATE(created_at) = %s"
        params.append(order_date)
    
    if symbols:
        placeholders = ','.join(['%s'] * len(symbols))
        query += f" AND symbol IN ({placeholders})"
        params.extend(symbols)
    
    if statuses:
        placeholders = ','.join(['%s'] * len(statuses))
        query += f" AND status IN ({placeholders})"
        params.extend(statuses)
    
    query += " ORDER BY order_id ASC LIMIT 500"
    
    orders = execute_query(query, tuple(params))
    # Convert datetime objects to strings
    for order in orders:
        for key, value in order.items():
            if isinstance(value, (datetime, date)):
                order[key] = value.isoformat() if value else None
            elif hasattr(value, 'total_seconds'):  # timedelta/interval
                order[key] = str(value) if value else None
    
    return jsonify(orders)

@app.route("/api/stats")
def get_stats():
    """Get summary statistics for filters"""
    account = request.args.get("account")
    order_date = request.args.get("date")
    symbols = request.args.getlist("symbol")  # Multiple symbols
    statuses = request.args.getlist("status")  # Multiple statuses
    
    query = f"""
        SELECT 
            COUNT(*) as total_orders,
            COUNT(CASE WHEN transaction_type = 'B' THEN 1 END) as buy_orders,
            COUNT(CASE WHEN transaction_type = 'S' THEN 1 END) as sell_orders,
            COUNT(CASE WHEN status = 'COMPLETE' THEN 1 END) as completed,
            COUNT(CASE WHEN status = 'REJECTED' THEN 1 END) as rejected,
            COUNT(DISTINCT symbol) as unique_symbols
        FROM "{Config.DB_SCHEMA}".shoonya_orders
        WHERE 1=1
    """
    params = []
    
    if account:
        query += " AND account = %s"
        params.append(account)
    
    if order_date:
        query += " AND DATE(created_at) = %s"
        params.append(order_date)
    
    if symbols:
        placeholders = ','.join(['%s'] * len(symbols))
        query += f" AND symbol IN ({placeholders})"
        params.extend(symbols)
    
    if statuses:
        placeholders = ','.join(['%s'] * len(statuses))
        query += f" AND status IN ({placeholders})"
        params.extend(statuses)
    
    stats = execute_query(query, tuple(params))
    return jsonify(stats[0] if stats else {})

# ============ Config Editor API Routes ============

@app.route("/api/config/files")
def get_config_files():
    """Get list of JSON config files"""
    try:
        files = []
        if os.path.exists(CONFIG_DIR):
            for f in sorted(os.listdir(CONFIG_DIR)):
                if f.endswith('.json'):
                    filepath = os.path.join(CONFIG_DIR, f)
                    files.append({
                        "name": f,
                        "size": os.path.getsize(filepath),
                        "modified": os.path.getmtime(filepath)
                    })
        return jsonify({"success": True, "files": files})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/config/file/<filename>")
def get_config_file(filename):
    """Get content of a specific config file"""
    try:
        # Security: ensure filename doesn't contain path traversal
        if '..' in filename or '/' in filename:
            return jsonify({"success": False, "error": "Invalid filename"}), 400
        
        filepath = os.path.join(CONFIG_DIR, filename)
        if not os.path.exists(filepath):
            return jsonify({"success": False, "error": "File not found"}), 404
        
        with open(filepath, 'r') as f:
            content = json.load(f)
        
        return jsonify({"success": True, "filename": filename, "content": content})
    except json.JSONDecodeError as e:
        return jsonify({"success": False, "error": f"Invalid JSON: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/config/file/<filename>", methods=["POST"])
def save_config_file(filename):
    """Save content to a config file"""
    try:
        # Security: ensure filename doesn't contain path traversal
        if '..' in filename or '/' in filename:
            return jsonify({"success": False, "error": "Invalid filename"}), 400
        
        filepath = os.path.join(CONFIG_DIR, filename)
        
        data = request.get_json()
        if data is None:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        
        content = data.get('content')
        if content is None:
            return jsonify({"success": False, "error": "No content provided"}), 400
        
        # Validate it's valid JSON by parsing it
        if isinstance(content, str):
            content = json.loads(content)
        
        with open(filepath, 'w') as f:
            json.dump(content, f, indent=4)
        
        return jsonify({"success": True, "message": f"File {filename} saved successfully"})
    except json.JSONDecodeError as e:
        return jsonify({"success": False, "error": f"Invalid JSON: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
