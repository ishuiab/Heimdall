"""
Lightweight Flask application for viewing trading orders
Uses HTMX for dynamic updates without heavy JavaScript frameworks
"""

from fileinput import filename
from flask import Flask, render_template, request, jsonify
import psycopg2
import redis
import os
import json
import requests
from pathlib import Path
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

# Add CSP headers to allow inline scripts and evaluation
@app.after_request
def set_security_headers(response):
    response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self';"
    return response

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
CONFIGS_BASE_PATH = Config.CONFIG_BASE_PATH
CONFIG_DIR        = "/home/algobaba/DATALORE/hypotheis/FMV_SCALPER/configs"
TOKEN_PATH        = Config.TOKENS_PATH
LOGS_BASE_PATH    = Config.LOGS_BASE_PATH

#Services and bot commands and logs path from Config
SERVICE_COMMANDS = Config.SERVICE_COMMANDS
BOT_COMMANDS     = Config.BOT_COMMANDS

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

# ============ Heartbeat API Routes ============

def check_postgres_health():
    """Check if PostgreSQL is running and accessible"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        return {
            "status": "healthy",
            "host": Config.DB_HOST,
            "port": Config.DB_PORT,
            "database": Config.DB_NAME,
            "message": "Connection successful"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "host": Config.DB_HOST,
            "port": Config.DB_PORT,
            "database": Config.DB_NAME,
            "message": str(e)
        }

def check_redis_health():
    """Check if Redis is running and accessible"""
    try:
        r = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            password=Config.REDIS_PASSWORD,
            db=Config.REDIS_DB,
            socket_timeout=5
        )
        r.ping()
        info = r.info()
        return {
            "status": "healthy",
            "host": Config.REDIS_HOST,
            "port": Config.REDIS_PORT,
            "version": info.get("redis_version", "unknown"),
            "connected_clients": info.get("connected_clients", 0),
            "used_memory_human": info.get("used_memory_human", "unknown"),
            "message": "Connection successful"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "host": Config.REDIS_HOST,
            "port": Config.REDIS_PORT,
            "message": str(e)
        }

def check_dataapi_health():
    """Check if DataAPI (TickEngine) is running by checking Redis heartbeat key"""
    try:
        r = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            password=Config.REDIS_PASSWORD,
            db=Config.REDIS_DB,
            socket_timeout=5
        )
        
        heartbeat_key = "tickengine:heartbeat"
        
        # Check if key exists
        if r.exists(heartbeat_key):
            # Get hash values
            heartbeat_data = r.hgetall(heartbeat_key)
            data = {k.decode('utf-8'): v.decode('utf-8') for k, v in heartbeat_data.items()}
            
            # Parse timestamp and convert to readable format
            timestamp_str = data.get('timestamp', '')
            last_heartbeat = None
            if timestamp_str:
                try:
                    ts = int(timestamp_str)
                    last_heartbeat = datetime.fromtimestamp(ts).isoformat()
                except:
                    last_heartbeat = timestamp_str
            
            return {
                "status": "healthy",
                "key": heartbeat_key,
                "last_heartbeat": last_heartbeat,
                "tick_count": data.get('tick_count', '0'),
                "error_count": data.get('error_count', '0'),
                "engine_status": data.get('status', 'unknown'),
                "message": "TickEngine is running"
            }
        else:
            return {
                "status": "unhealthy",
                "key": heartbeat_key,
                "message": "TickEngine heartbeat key not found - TickEngine may be down"
            }
    except Exception as e:
        return {
            "status": "unhealthy",
            "key": "tickengine:heartbeat",
            "message": f"Redis error: {str(e)}"
        }

def check_service_heartbeat(service_name, redis_key):
    """Check heartbeat for a service by checking Redis key with timestamp value.
    Returns status based on age: green (<10s), yellow (11-30s), red (>30s or missing)
    """
    try:
        r = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            password=Config.REDIS_PASSWORD,
            db=Config.REDIS_DB,
            socket_timeout=5
        )
        
        # Check if key exists
        if r.exists(redis_key):
            # Get the timestamp value
            timestamp_value = r.get(redis_key)
            if timestamp_value:
                timestamp_str = timestamp_value.decode('utf-8')
                try:
                    # Try parsing as float/int timestamp
                    ts = float(timestamp_str)
                    last_heartbeat_dt = datetime.fromtimestamp(ts)
                    last_heartbeat = last_heartbeat_dt.isoformat()
                    
                    # Calculate age in seconds
                    age_seconds = (datetime.now() - last_heartbeat_dt).total_seconds()
                    
                    if age_seconds <= 10:
                        status = "healthy"  # Green
                        message = f"{service_name} is running (last heartbeat {age_seconds:.1f}s ago)"
                    elif age_seconds <= 30:
                        status = "warning"  # Yellow
                        message = f"{service_name} heartbeat is stale ({age_seconds:.1f}s ago)"
                    else:
                        status = "unhealthy"  # Red
                        message = f"{service_name} heartbeat is too old ({age_seconds:.1f}s ago)"
                    
                    return {
                        "status": status,
                        "key": redis_key,
                        "last_heartbeat": last_heartbeat,
                        "age_seconds": round(age_seconds, 1),
                        "message": message
                    }
                except (ValueError, TypeError):
                    return {
                        "status": "unhealthy",
                        "key": redis_key,
                        "message": f"Invalid timestamp format in {redis_key}"
                    }
            else:
                return {
                    "status": "unhealthy",
                    "key": redis_key,
                    "message": f"{service_name} heartbeat key exists but has no value"
                }
        else:
            return {
                "status": "unhealthy",
                "key": redis_key,
                "message": f"{service_name} heartbeat key not found - service may be down"
            }
    except Exception as e:
        return {
            "status": "unhealthy",
            "key": redis_key,
            "message": f"Redis error: {str(e)}"
        }

def check_order_processor_health():
    """Check if OrderProcessor is running by checking Redis heartbeat key"""
    return check_service_heartbeat("OrderProcessor", "heartbeat:OrderProcessor")

def check_execution_engine_health():
    """Check if ExecutionEngine is running by checking Redis heartbeat key"""
    return check_service_heartbeat("ExecutionEngine", "heartbeat:ExecutionEngine")

def get_token_status(account, broker):
    """Dummy function to get token status for an account and broker"""
    # In real implementation, check token validity from database or file
    token_file = os.path.join(TOKEN_PATH, f"{broker}_token_{account}")
    try:
        if os.path.exists(token_file):
            with open(token_file, 'r') as f:
                token = f.read().strip()
        else:
            return f"Token file not found {token_file}"
    except Exception as e:
        return f"Token file error: {token_file}"
    
    if broker.lower() == "shoonya":
        jData = {
            "uid": account,
            "actid": account
        }
        jData_str    = json.dumps(jData)
        request_data = f"jData={jData_str}&jKey={token}"
        sess         = requests.Session()
        try:
            resp = sess.post(
                f"https://api.shoonya.com/NorenWClientTP/Limits",
                data=request_data,
                timeout=10
            )
            if resp.status_code == 200:
                try:
                    resp_json = resp.json()
                    if resp_json.get("stat") == "Ok":
                        return "Valid"
                    else:
                        return f"Invalid token: {resp_json.get('emsg', 'Unknown error')}"
                except json.JSONDecodeError:
                    return "Invalid token: Unable to parse response"
            else:
                return f"Invalid"
        except Exception as e:
            return f"Invalid token: {str(e)}"
    elif broker.lower() == "dhan":
        headers = {
            'access-token': f'{token}',
            'Content-Type': 'application/json'
        }
        try:
            resp = requests.get(
                'https://api.dhan.co/v2/fundlimit',
                headers=headers,
                timeout=10
            )
            if resp.status_code == 200:
                try:
                    resp_json = resp.json()
                    if resp_json.get("dhanClientId") == account:
                        return "Valid"
                    else:
                        return f"Invalid token: {resp_json.get('message', 'Unknown error')}"
                except json.JSONDecodeError:
                    return "Invalid token: Unable to parse response"
            elif resp.status_code == 401:
                return f"Invalid"
            else:
                return f"Invalid token: HTTP {resp.status_code}"
        except Exception as e:
            return f"Invalid token: {str(e)}"
    else:
        return f"Unknown broker {broker.upper()}"
    
    return "Unknown error"

def verify_token_with_value(account, broker, token_value):
    """Verify a provided token value for a broker/account. Returns a status string similar to get_token_status."""
    token = token_value
    if not token:
        return "Invalid: empty token"

    if broker.lower() == "shoonya":
        jData = {
            "uid": account,
            "actid": account
        }
        jData_str = json.dumps(jData)
        request_data = f"jData={jData_str}&jKey={token}"
        sess = requests.Session()
        try:
            resp = sess.post(
                f"https://api.shoonya.com/NorenWClientTP/Limits",
                data=request_data,
                timeout=10
            )
            if resp.status_code == 200:
                try:
                    resp_json = resp.json()
                    if resp_json.get("stat") == "Ok":
                        return "Valid"
                    else:
                        return f"Invalid token: {resp_json.get('emsg', 'Unknown error')}"
                except json.JSONDecodeError:
                    return "Invalid token: Unable to parse response"
            else:
                return f"Invalid token: HTTP {resp.status_code}"
        except Exception as e:
            return f"Invalid token: {str(e)}"
    elif broker.lower() == "dhan":
        headers = {
            'access-token': f'{token}',
            'Content-Type': 'application/json'
        }
        try:
            resp = requests.get(
                'https://api.dhan.co/v2/fundlimit',
                headers=headers,
                timeout=10
            )
            if resp.status_code == 200:
                try:
                    resp_json = resp.json()
                    if resp_json.get("dhanClientId") == account:
                        return "Valid"
                    else:
                        return f"Invalid token: {resp_json.get('message', 'Unknown error')}"
                except json.JSONDecodeError:
                    return "Invalid token: Unable to parse response"
            elif resp.status_code == 401:
                return f"Invalid token: Unauthorized"
            else:
                return f"Invalid token: HTTP {resp.status_code}"
        except Exception as e:
            return f"Invalid token: {str(e)}"
    else:
        return f"Unknown broker {broker.upper()}"

    return "Unknown error"

@app.route("/api/heartbeat")
def get_heartbeat():
    """Get health status of all services"""
    services = {
        "postgres": check_postgres_health(),
        "redis": check_redis_health(),
        "dataapi": check_dataapi_health(),
        "orderprocessor": check_order_processor_health(),
        "executionengine": check_execution_engine_health()
    }
    
    # Overall status - healthy only if all are healthy, degraded if any warning/unhealthy
    statuses = [svc["status"] for svc in services.values()]
    if all(s == "healthy" for s in statuses):
        overall = "healthy"
    elif any(s == "unhealthy" for s in statuses):
        overall = "degraded"
    else:
        overall = "warning"
    
    return jsonify({
        "overall_status": overall,
        "timestamp": datetime.now().isoformat(),
        "services": services
    })

@app.route("/api/heartbeat/<service>")
def get_service_heartbeat(service):
    """Get health status of a specific service"""
    if service == "postgres":
        result = check_postgres_health()
    elif service == "redis":
        result = check_redis_health()
    elif service == "dataapi":
        result = check_dataapi_health()
    elif service == "orderprocessor":
        result = check_order_processor_health()
    elif service == "executionengine":
        result = check_execution_engine_health()
    else:
        return jsonify({"error": f"Unknown service: {service}"}), 404
    
    result["timestamp"] = datetime.now().isoformat()
    return jsonify(result)

@app.route("/api/token-validity")
def get_token_validity():
    """Get list of valid tokens from TOKEN_PATH"""
    try:
        tokens = []
        if os.path.exists(TOKEN_PATH):
            for filename in sorted(os.listdir(TOKEN_PATH)):
                if "_token_" in filename.lower():
                    # Parse filename to extract broker and account
                    # Format: broker_token_account (e.g., dhan_token_1109120000)
                    parts = filename.split('_token_')
                    if len(parts) == 2:
                        broker = parts[0]
                        account = parts[1].replace('.json', '').replace('.txt', '')
                        tokens.append({
                            "broker": broker,
                            "account": account,
                            "filename": filename,
                            "status": get_token_status(account, broker),
                            "path": os.path.join(TOKEN_PATH, filename)
                        })
        
        return jsonify({
            "success": True,
            "token_path": TOKEN_PATH,
            "tokens": tokens,
            "total_tokens": len(tokens)
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "token_path": TOKEN_PATH
        }), 500

@app.route("/api/tokens")
def api_list_tokens():
    """Return tokens found in TOKEN_PATH for the frontend token manager."""
    try:
        tokens = []
        if os.path.exists(TOKEN_PATH):
            for filename in sorted(os.listdir(TOKEN_PATH)):
                if "_token_" in filename.lower():
                    parts = filename.split('_token_')
                    if len(parts) == 2:
                        broker = parts[0]
                        account = parts[1].replace('.json', '').replace('.txt', '')
                        path = os.path.join(TOKEN_PATH, filename)
                        token_val = None
                        try:
                            with open(path, 'r') as f:
                                token_val = f.read().strip()
                        except Exception:
                            token_val = None

                        tokens.append({
                            "broker": broker,
                            "account": account,
                            "token": token_val,
                            "status": get_token_status(account, broker) if token_val else 'Unknown',
                            "filename": filename
                        })

        return jsonify({"success": True, "token_path": TOKEN_PATH, "tokens": tokens})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/tokens/check', methods=['POST'])
def api_check_token():
    try:
        data = request.get_json() or {}
        broker = data.get('broker')
        account = data.get('account')
        token = (data.get('token') or '').strip()
        if not broker or not account:
            return jsonify({'success': False, 'error': 'Missing broker or account'}), 400

        status = verify_token_with_value(account, broker, token)
        # Return a simple valid boolean too for compatibility
        valid = str(status).lower().startswith('valid')
        return jsonify({'success': True, 'status': status, 'valid': valid})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/tokens/save', methods=['POST'])
def api_save_token():
    try:
        data = request.get_json() or {}
        broker = data.get('broker')
        account = data.get('account')
        token = (data.get('token') or '').strip()
        if not broker or not account or token == '':
            return jsonify({'success': False, 'error': 'Missing broker, account or token'}), 400

        # Ensure token directory exists
        os.makedirs(TOKEN_PATH, exist_ok=True)
        filename = f"{broker}_token_{account}"
        filepath = os.path.join(TOKEN_PATH, filename)
        with open(filepath, 'w') as f:
            f.write(token)

        return jsonify({'success': True, 'message': 'Token saved', 'filename': filename})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============ Positions API Routes ============

@app.route("/api/positions/accounts")
def get_positions_accounts():
    """Get list of accounts from token files for positions"""
    try:
        accounts = []
        if os.path.exists(TOKEN_PATH):
            for filename in sorted(os.listdir(TOKEN_PATH)):
                if "_token_" in filename.lower():
                    # Parse filename to extract broker and account
                    # Format: broker_token_account (e.g., shoonya_token_FA392638)
                    parts = filename.split('_token_')
                    if len(parts) == 2:
                        broker = parts[0]
                        account = parts[1].replace('.json', '').replace('.txt', '')
                        accounts.append({
                            "broker": broker,
                            "account": account,
                            "filename": filename
                        })
        
        return jsonify({
            "success": True,
            "accounts": accounts,
            "total": len(accounts)
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route("/api/positions/all")
def get_all_positions():
    """Get positions for all accounts from token files."""
    try:
        accounts = []
        if os.path.exists(TOKEN_PATH):
            for filename in sorted(os.listdir(TOKEN_PATH)):
                if "_token_" in filename.lower():
                    # Parse filename to extract broker and account
                    parts = filename.split('_token_')
                    if len(parts) == 2:
                        broker  = parts[0]
                        account = parts[1].replace('.json', '').replace('.txt', '')
                        
                        # Fetch positions for this account
                        result = fetch_positions_for_account(broker, account)
                        accounts.append(result)
        
        return jsonify({
            "success": True,
            "accounts": accounts,
            "total": len(accounts)
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

def fetch_positions_for_account(broker, account):
    """Fetch positions for a specific broker and account.
    First checks token validity, then fetches positions.
    """
    result = {
        "broker": broker,
        "account": account,
        "positions": [],
        "error": None,
        "token_status": None
    }
    
    # Read token from file
    token_file = os.path.join(TOKEN_PATH, f"{broker}_token_{account}")
    try:
        if os.path.exists(token_file):
            with open(token_file, 'r') as f:
                token = f.read().strip()
        else:
            result["error"] = f"Token file not found"
            result["token_status"] = "missing"
            return result
    except Exception as e:
        result["error"] = f"Token file error: {str(e)}"
        result["token_status"] = "error"
        return result
    
    # Fetch positions based on broker
    if broker.lower() == "shoonya":
        return fetch_shoonya_positions(account, token, result)
    elif broker.lower() == "dhan":
        return fetch_dhan_positions(account, token, result)
    else:
        result["error"] = f"Unknown broker: {broker}"
        return result

def fetch_shoonya_positions(account, token, result):
    """Fetch positions from Shoonya API"""
    try:
        jData = {
            "uid": account,
            "actid": account
        }
        jData_str = json.dumps(jData)
        request_data = f"jData={jData_str}&jKey={token}"
        
        sess = requests.Session()
        resp = sess.post(
            "https://api.shoonya.com/NorenWClientTP/PositionBook",
            data=request_data,
            timeout=10
        )
        
        if resp.status_code == 200:
            try:
                resp_json = resp.json()
                
                # Check if response is valid
                if isinstance(resp_json, list):
                    # Successful response - array of positions
                    result["token_status"] = "valid"
                    result["positions"] = normalize_shoonya_positions(resp_json)
                elif isinstance(resp_json, dict):
                    if resp_json.get("stat") == "Ok":
                        result["token_status"] = "valid"
                        result["positions"] = []
                    elif resp_json.get("stat") == "Not_Ok":
                        error_msg = resp_json.get("emsg", "Unknown error")
                        if "Session" in error_msg or "Invalid" in error_msg:
                            result["token_status"] = "invalid"
                            result["error"] = f"Invalid token: {error_msg}"
                        elif "no data" in error_msg:
                            result["token_status"] = "valid"
                            result["positions"] = []
                        else:
                            result["token_status"] = "valid"
                            result["error"]        = error_msg
                    else:
                        result["token_status"] = "valid"
                        result["positions"] = []
                else:
                    result["error"] = "Unexpected response format"
                    
            except json.JSONDecodeError:
                result["error"] = "Invalid JSON response"
                result["token_status"] = "unknown"
        else:
            result["error"] = f"HTTP {resp.status_code}"
            result["token_status"] = "unknown"
            
    except Exception as e:
        result["error"] = f"{str(e)}"
        result["token_status"] = "error"
    
    return result

def normalize_shoonya_positions(positions):
    """Normalize Shoonya positions to common format"""
    normalized = []
    for pos in positions:
        try:
            # Calculate values
            buy_qty = int(pos.get("daybuyqty", 0) or 0) + int(pos.get("cfbuyqty", 0) or 0)
            sell_qty = int(pos.get("daysellqty", 0) or 0) + int(pos.get("cfsellqty", 0) or 0)
            net_qty = int(pos.get("netqty", 0) or 0)
            
            buy_avg = float(pos.get("daybuyavgprc", 0) or 0)
            sell_avg = float(pos.get("daysellavgprc", 0) or 0)
            ltp = float(pos.get("lp", 0) or 0)
            
            # PnL calculations
            realized_pnl = float(pos.get("rpnl", 0) or 0)
            unrealized_pnl = float(pos.get("urmtom", 0) or 0)
            
            # Map product type: I -> DAY, C -> CNC
            prd = pos.get("prd", "")
            product_map = {"I": "DAY", "C": "CNC", "M": "MARGIN", "B": "BO", "H": "CO"}
            product_type = product_map.get(prd, prd)
            
            normalized.append({
                "symbol": pos.get("tsym", ""),
                "exchange": pos.get("exch", ""),
                "product_type": product_type,
                "buy_qty": buy_qty,
                "sell_qty": sell_qty,
                "net_qty": net_qty,
                "buy_avg": buy_avg,
                "sell_avg": sell_avg,
                "ltp": ltp,
                "realized_pnl": realized_pnl,
                "unrealized_pnl": unrealized_pnl,
                "total_pnl": realized_pnl + unrealized_pnl
            })
        except Exception as e:
            # Skip malformed positions
            continue
    
    return normalized

def fetch_dhan_positions(account, token, result):
    """Fetch positions from Dhan API"""
    try:
        headers = {
            'Content-Type': 'application/json',
            'access-token': token
        }
        
        resp = requests.get(
            'https://api.dhan.co/v2/positions',
            headers=headers,
            timeout=10
        )
        
        if resp.status_code == 200:
            try:
                resp_json = resp.json()
                result["token_status"] = "valid"
                
                # Dhan returns positions in a list
                if isinstance(resp_json, list):
                    result["positions"] = normalize_dhan_positions(resp_json)
                elif isinstance(resp_json, dict) and "data" in resp_json:
                    result["positions"] = normalize_dhan_positions(resp_json["data"])
                else:
                    result["positions"] = []
                    
            except json.JSONDecodeError:
                result["error"] = "Invalid JSON response"
                result["token_status"] = "unknown"
        elif resp.status_code == 401:
            result["error"] = "Invalid token"
            result["token_status"] = "invalid"
        else:
            result["error"] = f"HTTP {resp.status_code}"
            result["token_status"] = "unknown"
            
    except Exception as e:
        result["error"] = str(e)
        result["token_status"] = "error"
    
    return result

def normalize_dhan_positions(positions):
    """Normalize Dhan positions to common format"""
    normalized = []
    for pos in positions:
        try:
            buy_qty        = int(pos.get("buyQty", 0) or 0)
            sell_qty       = int(pos.get("sellQty", 0) or 0)
            net_qty        = int(pos.get("netQty", 0) or 0)
            
            buy_avg        = float(pos.get("buyAvg", 0) or 0)
            sell_avg       = float(pos.get("sellAvg", 0) or 0)
            ltp            = float(pos.get("lastTradedPrice", 0) or pos.get("ltp", 0) or 0)
            
            realized_pnl   = float(pos.get("realizedProfit", 0) or 0)
            unrealized_pnl = float(pos.get("unrealizedProfit", 0) or 0)
            total_pnl      = float(pos.get("dayPnl", 0) or pos.get("totalPnl", 0) or (realized_pnl + unrealized_pnl))
            
            # If LTP not provided by API, derive from unrealized PNL
            # Formula: unrealized_pnl = (ltp - buy_avg) * net_qty for long positions
            #          unrealized_pnl = (sell_avg - ltp) * abs(net_qty) for short positions
            if ltp == 0 and net_qty != 0:
                if net_qty > 0:
                    # Long position: LTP = Buy Avg + (Unrealized PNL / Net Qty)
                    ltp = buy_avg + (unrealized_pnl / net_qty)
                else:
                    # Short position: LTP = Sell Avg - (Unrealized PNL / abs(Net Qty))
                    ltp = sell_avg - (unrealized_pnl / abs(net_qty))
            
            # Map exchange codes
            exchange_map = {
                "NSE_EQ": "NSE",
                "BSE_EQ": "BSE",
                "NSE_FNO": "NFO",
                "BSE_FNO": "BFO",
                "MCX_COMM": "MCX",
                "NSE_CURRENCY": "CDS",
                "BSE_CURRENCY": "BCD"
            }
            exchange = exchange_map.get(pos.get("exchangeSegment", ""), pos.get("exchangeSegment", ""))
            
            # Map product type: INTRADAY -> DAY, CNC -> CNC
            product_map = {
                "INTRADAY": "DAY",
                "CNC": "CNC",
                "MARGIN": "MARGIN",
                "CO": "CO",
                "BO": "BO"
            }
            product = product_map.get(pos.get("productType", ""), pos.get("productType", ""))
            
            normalized.append({
                "symbol": pos.get("tradingSymbol", "") or pos.get("securityId", ""),
                "exchange": exchange,
                "product_type": product,
                "buy_qty": buy_qty,
                "sell_qty": sell_qty,
                "net_qty": net_qty,
                "buy_avg": buy_avg,
                "sell_avg": sell_avg,
                "ltp": ltp,
                "realized_pnl": realized_pnl,
                "unrealized_pnl": unrealized_pnl,
                "total_pnl": total_pnl
            })
        except Exception as e:
            # Skip malformed positions
            continue
    
    return normalized

@app.route("/api/positions")
def get_positions():
    """Get positions for a specific broker and account.
    This is a placeholder - actual implementation will be provided by user.
    """
    broker  = request.args.get("broker")
    account = request.args.get("account")
    
    if not broker or not account:
        return jsonify({
            "success": False,
            "error": "Both broker and account parameters are required"
        }), 400
    
    try:
        # TODO: Implement actual position fetching logic here
        # This is a placeholder that returns empty positions
        # The user will provide the actual implementation
        
        positions = []
        
        return jsonify({
            "success": True,
            "broker": broker,
            "account": account,
            "positions": positions,
            "total": len(positions),
            "message": "Position fetching logic not yet implemented - will be provided by user"
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route("/api/tickers")
def get_tickers():
    """Get all ticker data from Redis (keys like tick:NSE:4529)"""
    try:
        r = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            password=Config.REDIS_PASSWORD,
            db=Config.REDIS_DB,
            socket_timeout=5
        )
        
        # Get all keys from Redis to understand what we're working with
        all_keys = r.keys("*")
        all_keys_str = [k.decode('utf-8') if isinstance(k, bytes) else k for k in all_keys]
        
        # Filter out keys starting with "inst"
        filtered_keys_str = [k for k in all_keys_str if not k.lower().startswith("inst")]
        
        # Find all keys matching the pattern tick:*
        pattern = "tick:*"
        keys = r.keys(pattern)
        
        # If no keys found with tick:*, try other common patterns
        if not keys:
            keys = r.keys("tick*")
        
        tickers = []
        failed_keys = []
        
        for key in keys:
            try:
                key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                
                # Get the type of the key
                key_type = r.type(key)
                if isinstance(key_type, bytes):
                    key_type = key_type.decode('utf-8')
                
                value_data = None
                
                # Handle different Redis data types
                if key_type == 'string':
                    value = r.get(key)
                    if value:
                        if isinstance(value, bytes):
                            value_decoded = value.decode('utf-8')
                        else:
                            value_decoded = str(value)
                        
                        try:
                            value_data = json.loads(value_decoded)
                        except json.JSONDecodeError:
                            value_data = {"raw": value_decoded}
                
                elif key_type == 'hash':
                    # For hash, get all fields
                    hash_data = r.hgetall(key)
                    value_data = {}
                    for hkey, hval in hash_data.items():
                        hkey_str = hkey.decode('utf-8') if isinstance(hkey, bytes) else hkey
                        hval_str = hval.decode('utf-8') if isinstance(hval, bytes) else str(hval)
                        try:
                            value_data[hkey_str] = json.loads(hval_str)
                        except (json.JSONDecodeError, ValueError):
                            value_data[hkey_str] = hval_str
                
                elif key_type == 'list':
                    # For list, get all elements
                    list_data = r.lrange(key, 0, -1)
                    value_data = []
                    for item in list_data:
                        item_str = item.decode('utf-8') if isinstance(item, bytes) else str(item)
                        try:
                            value_data.append(json.loads(item_str))
                        except (json.JSONDecodeError, ValueError):
                            value_data.append(item_str)
                
                elif key_type == 'set':
                    # For set, get all members
                    set_data = r.smembers(key)
                    value_data = []
                    for item in set_data:
                        item_str = item.decode('utf-8') if isinstance(item, bytes) else str(item)
                        try:
                            value_data.append(json.loads(item_str))
                        except (json.JSONDecodeError, ValueError):
                            value_data.append(item_str)
                
                elif key_type == 'zset':
                    # For sorted set, get all members with scores
                    zset_data = r.zrange(key, 0, -1, withscores=True)
                    value_data = []
                    for item, score in zset_data:
                        item_str = item.decode('utf-8') if isinstance(item, bytes) else str(item)
                        try:
                            value_data.append({"value": json.loads(item_str), "score": score})
                        except (json.JSONDecodeError, ValueError):
                            value_data.append({"value": item_str, "score": score})
                
                else:
                    value_data = {"type": key_type, "message": "Unsupported Redis data type"}
                
                if value_data is not None:
                    tickers.append({
                        "key": key_str,
                        "type": key_type,
                        "data": value_data
                    })
                else:
                    failed_keys.append({"key": key_str, "reason": f"No data for type {key_type}"})
                    
            except Exception as e:
                failed_keys.append({"key": key.decode('utf-8') if isinstance(key, bytes) else str(key), "reason": str(e)})
        
        response = {
            "success": True,
            "total": len(tickers),
            "tickers": sorted(tickers, key=lambda x: x['key']),
            "timestamp": datetime.now().isoformat(),
            "redis_info": {
                "host": Config.REDIS_HOST,
                "port": Config.REDIS_PORT,
                "db": Config.REDIS_DB,
                "total_keys_in_redis": len(all_keys),
                "inst_keys_count": len(all_keys_str) - len(filtered_keys_str),
                "other_keys": sorted(filtered_keys_str),
                "pattern_searched": pattern,
                "keys_found_with_pattern": len(keys),
                "tickers_processed": len(tickers),
                "failed_keys_count": len(failed_keys),
                "failed_keys_debug": failed_keys[:10]  # Show first 10 failed keys for debugging
            }
        }
        
        return jsonify(response)
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "type": type(e).__name__
        }), 500

# ============ Funds API Routes ============
@app.route("/api/funds/all")
def get_all_funds():
    """Get funds/balance data for all accounts from token files."""
    try:
        accounts = []
        if os.path.exists(TOKEN_PATH):
            for filename in sorted(os.listdir(TOKEN_PATH)):
                if "_token_" in filename.lower():
                    # Parse filename to extract broker and account
                    parts = filename.split('_token_')
                    if len(parts) == 2:
                        broker = parts[0]
                        account = parts[1].replace('.json', '').replace('.txt', '')
                        
                        # Fetch funds for this account
                        result = fetch_funds_for_account(broker, account)
                        accounts.append(result)
        
        return jsonify({
            "success": True,
            "accounts": accounts,
            "total": len(accounts)
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

def fetch_funds_for_account(broker, account):
    """Fetch funds/balance data for a specific broker and account."""
    result = {
        "broker": broker,
        "account": account,
        "available_balance": 0,
        "utilized_amount": 0,
        "remaining_balance": 0,
        "cash_used_percentage": 0,
        "error": None,
        "token_status": None
    }
    
    # Read token from file
    token_file = os.path.join(TOKEN_PATH, f"{broker}_token_{account}")
    try:
        if os.path.exists(token_file):
            with open(token_file, 'r') as f:
                token = f.read().strip()
        else:
            result["error"] = f"Token file not found"
            result["token_status"] = "missing"
            return result
    except Exception as e:
        result["error"] = f"Token file error: {str(e)}"
        result["token_status"] = "error"
        return result
    
    # Fetch funds based on broker
    if broker.lower() == "shoonya":
        return fetch_shoonya_funds(account, token, result)
    elif broker.lower() == "dhan":
        return fetch_dhan_funds(account, token, result)
    else:
        result["error"] = f"Unknown broker: {broker}"
        return result

def fetch_shoonya_funds(account, token, result):
    """Fetch funds/limits from Shoonya API"""
    try:
        jData = {
            "uid": account,
            "actid": account
        }
        jData_str = json.dumps(jData)
        request_data = f"jData={jData_str}&jKey={token}"
        
        sess = requests.Session()
        resp = sess.post(
            "https://api.shoonya.com/NorenWClientTP/Limits",
            data=request_data,
            timeout=10
        )
        
        if resp.status_code == 200:
            try:
                resp_json = resp.json()
                
                if isinstance(resp_json, dict):
                    if resp_json.get("stat") == "Ok":
                        result["token_status"] = "valid"
                        
                        # Extract funds data
                        # In Shoonya: "cash" is available balance, "marginused" is utilized amount
                        cash = float(resp_json.get("cash", 0) or 0)
                        marginused = float(resp_json.get("marginused", 0) or 0)
                        
                        result["available_balance"] = cash
                        result["utilized_amount"] = marginused
                        result["remaining_balance"] = cash - marginused
                        
                        # Calculate percentage
                        total_balance = cash + marginused if (cash + marginused) > 0 else 1
                        result["cash_used_percentage"] = (marginused / total_balance) * 100
                        
                    elif resp_json.get("stat") == "Not_Ok":
                        error_msg = resp_json.get("emsg", "Unknown error")
                        if "Session" in error_msg or "Invalid" in error_msg:
                            result["token_status"] = "invalid"
                            result["error"] = f"Invalid token: {error_msg}"
                        else:
                            result["token_status"] = "valid"
                            result["error"] = error_msg
                    else:
                        result["token_status"] = "valid"
                else:
                    result["error"] = "Unexpected response format"
                    
            except json.JSONDecodeError:
                result["error"] = "Invalid JSON response"
                result["token_status"] = "unknown"
        else:
            result["error"] = f"HTTP {resp.status_code}"
            result["token_status"] = "unknown"
            
    except Exception as e:
        result["error"] = str(e)
        result["token_status"] = "error"
    
    return result

def fetch_dhan_funds(account, token, result):
    """Fetch funds/balance from Dhan API"""
    try:
        headers = {
            'Content-Type': 'application/json',
            'access-token': token
        }
        
        resp = requests.get(
            'https://api.dhan.co/v2/fundlimit',
            headers=headers,
            timeout=10
        )
        
        if resp.status_code == 200:
            try:
                resp_json = resp.json()
                result["token_status"] = "valid"
                
                # Extract funds data from Dhan
                # In Dhan: "sodLimit" is total balance, "availableBalance" is available balance
                available_balance = float(resp_json.get("sodLimit", 0) or 0)
                utilized_amount         = float(resp_json.get("utilizedAmount", 0) or 0)
                
                result["available_balance"] = available_balance
                result["remaining_balance"] = available_balance
                
                # Calculate utilized amount and percentage
                #utilized_amount = sod_limit - available_balance if sod_limit > 0 else 0
                result["utilized_amount"] = utilized_amount
                
                # Calculate percentage
                if utilized_amount > 0:
                    result["cash_used_percentage"] = (utilized_amount / available_balance) * 100
                else:
                    result["cash_used_percentage"] = 0
                    
            except json.JSONDecodeError:
                result["error"] = "Invalid JSON response"
                result["token_status"] = "unknown"
        elif resp.status_code == 401:
            result["error"] = "Invalid token"
            result["token_status"] = "invalid"
        else:
            result["error"] = f"HTTP {resp.status_code}"
            result["token_status"] = "unknown"
            
    except Exception as e:
        result["error"] = str(e)
        result["token_status"] = "error"
    
    return result

# ============ Redis Orders API Routes ============

@app.route("/api/redis-orders/brokers")
def get_redis_order_brokers():
    """Get unique brokers from Redis order keys.
    Keys format: OrderEngine:orderbook:BROKER:ACCOUNT:ORDER_ID
    """
    try:
        r = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            password=Config.REDIS_PASSWORD,
            db=Config.REDIS_DB,
            socket_timeout=5
        )
        
        # Get all order keys
        pattern = "OrderEngine:orderbook:*"
        keys = r.keys(pattern)
        
        brokers = set()
        for key in keys:
            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
            parts = key_str.split(':')
            if len(parts) >= 3:
                brokers.add(parts[2])  # BROKER is the 3rd part
        
        return jsonify({
            "success": True,
            "brokers": sorted(list(brokers))
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route("/api/redis-orders/accounts")
def get_redis_order_accounts():
    """Get unique accounts for a broker from Redis order keys."""
    broker = request.args.get("broker")
    
    if not broker:
        return jsonify({"success": True, "accounts": []})
    
    try:
        r = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            password=Config.REDIS_PASSWORD,
            db=Config.REDIS_DB,
            socket_timeout=5
        )
        
        # Get all order keys for this broker
        pattern = f"OrderEngine:orderbook:{broker}:*"
        keys = r.keys(pattern)
        
        accounts = set()
        for key in keys:
            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
            parts = key_str.split(':')
            if len(parts) >= 4:
                accounts.add(parts[3])  # ACCOUNT is the 4th part
        
        return jsonify({
            "success": True,
            "accounts": sorted(list(accounts))
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route("/api/redis-orders/filters")
def get_redis_order_filters():
    """Get filter options (exchanges, symbols, statuses) for a broker/account combination."""
    broker = request.args.get("broker")
    account = request.args.get("account")
    
    if not broker or not account:
        return jsonify({
            "success": True,
            "exchanges": [],
            "symbols": [],
            "statuses": []
        })
    
    try:
        r = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            password=Config.REDIS_PASSWORD,
            db=Config.REDIS_DB,
            socket_timeout=5
        )
        
        # Get all order keys for this broker and account
        pattern = f"OrderEngine:orderbook:{broker}:{account}:*"
        keys = r.keys(pattern)
        
        exchanges = set()
        symbols = set()
        statuses = set()
        
        for key in keys:
            try:
                key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                
                # Get hash data for this order
                order_data = r.hgetall(key)
                if order_data:
                    data = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                            v.decode('utf-8') if isinstance(v, bytes) else v 
                            for k, v in order_data.items()}
                    
                    if data.get('exchange'):
                        exchanges.add(data['exchange'])
                    if data.get('symbol'):
                        symbols.add(data['symbol'])
                    if data.get('status'):
                        statuses.add(data['status'])
            except Exception:
                continue
        
        return jsonify({
            "success": True,
            "exchanges": sorted(list(exchanges)),
            "symbols": sorted(list(symbols)),
            "statuses": sorted(list(statuses))
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route("/api/redis-orders")
def get_redis_orders():
    """Get orders from Redis based on filters.
    Keys format: OrderEngine:orderbook:BROKER:ACCOUNT:ORDER_ID
    """
    broker = request.args.get("broker")
    account = request.args.get("account")
    exchanges = request.args.getlist("exchange")
    symbols = request.args.getlist("symbol")
    statuses = request.args.getlist("status")
    
    if not broker or not account:
        return jsonify({
            "success": False,
            "error": "Both broker and account are required"
        }), 400
    
    try:
        r = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            password=Config.REDIS_PASSWORD,
            db=Config.REDIS_DB,
            socket_timeout=5
        )
        
        # Get all order keys for this broker and account
        pattern = f"OrderEngine:orderbook:{broker}:{account}:*"
        keys = r.keys(pattern)
        
        orders = []
        for key in keys:
            try:
                key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                
                # Get hash data for this order
                order_data = r.hgetall(key)
                if order_data:
                    data = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                            v.decode('utf-8') if isinstance(v, bytes) else v 
                            for k, v in order_data.items()}
                    
                    # Extract order_id from key
                    parts = key_str.split(':')
                    order_id = parts[-1] if len(parts) >= 5 else "unknown"
                    data['order_id'] = order_id
                    
                    # Apply filters
                    if exchanges and data.get('exchange') not in exchanges:
                        continue
                    if symbols and data.get('symbol') not in symbols:
                        continue
                    if statuses and data.get('status') not in statuses:
                        continue
                    
                    # Extract time-only from order_time and exch_timestamp
                    order_time = data.get('order_time', '')
                    if order_time:
                        # Try to extract just time part (HH:MM:SS)
                        time_match = None
                        # Handle various formats: "12:30:45", "2025-01-15 12:30:45", etc.
                        import re
                        time_match = re.search(r'(\d{1,2}:\d{2}:\d{2})', str(order_time))
                        if time_match:
                            data['order_time'] = time_match.group(1)
                    
                    exch_timestamp = data.get('exch_time', '')
                    if exch_timestamp:
                        import re
                        time_match = re.search(r'(\d{1,2}:\d{2}:\d{2})', str(exch_timestamp))
                        if time_match:
                            data['exch_time'] = time_match.group(1)
                    
                    orders.append(data)
            except Exception:
                continue
        
        # Sort orders by order_time descending (newest first)
        orders.sort(key=lambda x: x.get('order_time', ''), reverse=True)
        
        return jsonify({
            "success": True,
            "orders": orders,
            "total": len(orders)
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

# ============ Service Management Endpoints ============
@app.route('/api/services/start', methods=['POST'])
def start_service():
    """Start a service as a detached background process"""
    import subprocess
    import sys
    from datetime import datetime
    data = request.json
    service_name = data.get('service')
    
    if service_name not in SERVICE_COMMANDS:
        return jsonify({'success': False, 'error': f'Service {service_name} not found'}), 400
    
    try:
        service_config = SERVICE_COMMANDS[service_name]
        command = service_config['COMMAND']
        logs_path = service_config['LOGS_PATH']
        log_name = service_config['LOG_NAME']
        
        # Create log directory with today's date if it doesn't exist
        today = datetime.now().strftime('%Y-%m-%d')
        log_dir = os.path.join(logs_path, today)
        os.makedirs(log_dir, exist_ok=True)
        
        log_file_path = os.path.join(log_dir, log_name)
        
        # Open log file for appending
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"\n{'='*60}\n")
            log_file.write(f"Service started at {datetime.now().isoformat()}\n")
            log_file.write(f"{'='*60}\n")
        
        # Start the service as a completely detached background process
        subprocess.Popen(
                command,
                shell=True,
                stdout=open(log_file_path, 'a'),
                stderr=subprocess.STDOUT,
                start_new_session=True,
                preexec_fn=lambda: os.nice(10)  # Lower priority to avoid hogging resources
            )
        
        return jsonify({
            'success': True,
            'message': f'{service_name} started successfully as background process',
            'log_file': log_file_path
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/services/logs/<service_name>')
def get_service_logs(service_name):
    """Get logs for a service"""
    import glob
    from datetime import datetime
    
    if service_name not in SERVICE_COMMANDS:
        return jsonify({'success': False, 'error': f'Service {service_name} not found'}), 400
    
    try:
        service_config = SERVICE_COMMANDS[service_name]
        logs_path = service_config['LOGS_PATH']
        log_name = service_config['LOG_NAME']
        
        # Get today's date folder
        today = datetime.now().strftime('%Y-%m-%d')
        full_log_path = os.path.join(logs_path, today, log_name)
        
        # Try to read the log file
        if os.path.exists(full_log_path):
            with open(full_log_path, 'r') as f:
                content = f.read()
            return jsonify({
                'success': True,
                'content': content,
                'path': full_log_path,
                'service': service_name
            })
        else:
            # Check if folder exists and list available dates
            base_path = logs_path
            if os.path.exists(base_path):
                dates = sorted([d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))], reverse=True)
                if dates:
                    # Try the most recent date
                    recent_path = os.path.join(base_path, dates[0], log_name)
                    if os.path.exists(recent_path):
                        with open(recent_path, 'r') as f:
                            content = f.read()
                        return jsonify({
                            'success': True,
                            'content': content,
                            'path': recent_path,
                            'service': service_name,
                            'date': dates[0]
                        })
            
            return jsonify({
                'success': False,
                'error': f'Log file not found for {service_name}',
                'tried_path': full_log_path
            }), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/services/stop/<service_name>', methods=['POST'])
def stop_service(service_name):
    """Stop a running service"""
    import subprocess
    import psutil
    
    if service_name not in SERVICE_COMMANDS:
        return jsonify({'success': False, 'error': f'Service {service_name} not found'}), 400
    
    try:
        service_config = SERVICE_COMMANDS[service_name]
        command = service_config['COMMAND']
        
        # Extract the main executable/script from the command
        # For commands like: "/path/to/python /path/to/script.py args"
        # We want to find processes running this script
        
        stopped_count = 0
        
            # Unix/Linux/macOS: Use pkill or psutil
            # Try to find and kill processes running this command
        try:
                for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                    try:
                        # Check if this process is running our service command
                        if proc.info['cmdline']:
                            cmdline = ' '.join(proc.info['cmdline'])
                            # Check if all major parts of command match
                            if 'python' in cmdline and any(part in cmdline for part in command.split() if part.endswith('.py')):
                                os.killpg(os.getpgid(proc.info['pid']), 9)
                                stopped_count += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied, ProcessLookupError):
                        pass
        except Exception as e:
                # Fallback: use pkill
                script_match = None
                for part in command.split():
                    if part.endswith('.py'):
                        script_match = part
                        break
                
                if script_match:
                    script_name = os.path.basename(script_match)
                    result = subprocess.run(
                        f'pkill -9 -f "{script_name}"',
                        shell=True,
                        capture_output=True
                    )
                    stopped_count = 1 if result.returncode == 0 else 0
        
        if stopped_count > 0:
            # Log the stop action
            logs_path = service_config['LOGS_PATH']
            log_name = service_config['LOG_NAME']
            today = datetime.now().strftime('%Y-%m-%d')
            log_dir = os.path.join(logs_path, today)
            os.makedirs(log_dir, exist_ok=True)
            log_file_path = os.path.join(log_dir, log_name)
            
            with open(log_file_path, 'a') as log_file:
                log_file.write(f"\n{'='*60}\n")
                log_file.write(f"Service stopped at {datetime.now().isoformat()}\n")
                log_file.write(f"{'='*60}\n")
            
            return jsonify({
                'success': True,
                'message': f'{service_name} stopped successfully',
                'processes_stopped': stopped_count
            })
        else:
            return jsonify({
                'success': False,
                'error': f'Could not find or stop {service_name} process'
            }), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ============ Bot Manager API Routes ============
def get_order_range(config, etf_fmv):
    """
    Generate buy and sell order ranges based on FMV and config parameters.

    Output structure:
    {
        "buy": {
            "TICKER": {
                "99.00": {"QTY": 10, "TARGET": 100.25}
            }
        },
        "sell": {
            "TICKER": {
                "101.00": {"QTY": 10, "TARGET": 100.75}
            }
        }
    }
    """
    try:
        # ---------- Validation ----------
        tickers = config.get("tickers", [])
        if not tickers:
            raise ValueError("No tickers found in config")

        if not etf_fmv or etf_fmv <= 0:
            raise ValueError(f"Invalid ETF FMV: {etf_fmv}")

        # ---------- Init ----------
        order_range = {"buy": {}, "sell": {}}

        buy_enabled  = bool(config.get("action", {}).get("buy", False))
        sell_enabled = bool(config.get("action", {}).get("sell", False))

        start_qty_buy  = config.get("starting_quantity", {}).get("buy", 0)
        start_qty_sell = config.get("starting_quantity", {}).get("sell", 0)

        max_qty_buy  = config.get("max_quantity", {}).get("buy", 0)
        max_qty_sell = config.get("max_quantity", {}).get("sell", 0)

        max_orders_buy  = config.get("max_orders", {}).get("buy", 0)
        max_orders_sell = config.get("max_orders", {}).get("sell", 0)

        buy_mult  = config.get("multiplier", {}).get("buy", 0)
        sell_mult = config.get("multiplier", {}).get("sell", 0)
        

        if buy_mult < 0 or sell_mult < 0:
            raise ValueError("Multipliers cannot be negative")

        buy_margin      = config.get("margin", {}).get("buy", 0)
        sell_margin     = config.get("margin", {}).get("sell", 0)

        step_buy_pct    = config.get("step", {}).get("buy", 0)
        step_sell_pct   = config.get("step", {}).get("sell", 0)

        # ---------- Initial Prices ----------
        buy_price  = round(etf_fmv * (1 - buy_margin / 100), 2)
        sell_price = round(etf_fmv * (1 + sell_margin / 100), 2)

        step_buy_abs  = round(etf_fmv * step_buy_pct / 100, 2)
        step_sell_abs = round(etf_fmv * step_sell_pct / 100, 2)

        # ---------- Per-side state ----------
        curr_buy_qty  = start_qty_buy
        curr_sell_qty = start_qty_sell

        total_buy_qty  = 0
        total_sell_qty = 0

        buy_count  = 0
        sell_count = 0

        # ---------- Init tickers ----------
        for t in tickers:
            order_range["buy"][t]  = {}
            order_range["sell"][t] = {}

            order_range["config"]  = {}
            order_range["config"]["ETF FMV"]     = etf_fmv
            order_range["config"]["Start Buy"]   = start_qty_buy
            order_range["config"]["Start Sell"]  = start_qty_sell
            order_range["config"]["Max Buy"]     = max_qty_buy
            order_range["config"]["Max Sell"]    = max_qty_sell
            order_range["config"]["Margin Buy"]  = buy_margin
            order_range["config"]["Margin Sell"] = sell_margin
            
        # ---------- Main loop ----------
        max_iters = max(max_orders_buy, max_orders_sell)

        for i in range(max_iters):
            # --- BUY SIDE ---
            if buy_enabled and buy_count < max_orders_buy and total_buy_qty < max_qty_buy:
                if i > 0:
                    curr_buy_qty = int(curr_buy_qty * (1 + buy_mult))
                    buy_price   = round(buy_price - step_buy_abs, 2)

                if buy_price > 0 and curr_buy_qty > 0:
                    final_qty = min(curr_buy_qty, max_qty_buy - total_buy_qty)

                    if final_qty > 0:
                        target_offset = get_target_limit(buy_price, etf_fmv, "BUY", config)
                        final_target  = round(buy_price + target_offset, 2)

                        price_key = f"{buy_price:.2f}"

                        for t in tickers:
                            order_range["buy"][t][price_key] = {
                                "QTY": final_qty,
                                "TARGET": final_target
                            }

                        total_buy_qty += final_qty
                        buy_count += 1

            # --- SELL SIDE ---
            if sell_enabled and sell_count < max_orders_sell and total_sell_qty < max_qty_sell:
                if i > 0:
                    curr_sell_qty = int(curr_sell_qty * (1 + sell_mult))
                    sell_price   = round(sell_price + step_sell_abs, 2)

                if curr_sell_qty > 0:
                    final_qty = min(curr_sell_qty, max_qty_sell - total_sell_qty)

                    if final_qty > 0:
                        target_offset = get_target_limit(sell_price, etf_fmv, "SELL", config)
                        final_target  = round(sell_price + target_offset, 2)

                        price_key = f"{sell_price:.2f}"

                        for t in tickers:
                            order_range["sell"][t][price_key] = {
                                "QTY": final_qty,
                                "TARGET": final_target
                            }

                        total_sell_qty += final_qty
                        sell_count += 1

            if (
                (not buy_enabled or buy_count >= max_orders_buy or total_buy_qty >= max_qty_buy)
                and
                (not sell_enabled or sell_count >= max_orders_sell or total_sell_qty >= max_qty_sell)
            ):
                break

        return order_range

    except Exception as e:
        # IMPORTANT: never fail silently in trading code
        return None

def get_target_limit(entry_price, fmv_price, side, config, debug=False):
    """
    Calculate target profit limit based on 50-50 split of FMV-to-entry gap.
    
    Strategy:
    - Never sell below FMV or buy above FMV (constraint ensures favorable entry)
    - Split the gap between FMV and entry price 50-50 with the market
    - Keep 50% of gap as profit, give 50% to market for counter order
    - Enforce min/max profit boundaries
    
    Examples:
    - BUY @ 22.26, FMV @ 22.37: Gap = 0.11, Your profit = 0.055
      Counter SELL target = 22.26 + 0.055 = 22.315
    
    - SELL @ 22.37, FMV @ 22.26: Gap = 0.11, Your profit = 0.055
      Counter BUY target = 22.37 - 0.055 = 22.315
    
    Args:
        entry_price (float): Price at which the order was placed
        fmv_price (float): Fair Market Value of the ETF
        side (str): "BUY" (Buy) or "SELL" (Sell)
        config (dict): Configuration with target.minimum and target.maximum percentages

    Returns:
        float: Profit amount in rupees (positive for Buy, negative for Sell), or None on error
    """    
    try:
        # ===== INPUT VALIDATION =====
        if not isinstance(entry_price, (int, float)) or entry_price <= 0:
            return None
        if not isinstance(fmv_price, (int, float)) or fmv_price <= 0:
            return None
        if side not in ["BUY", "SELL"]:
            return None

        # ===== GET TARGET LIMITS FROM CONFIG =====
        target_limits  = config.get("target", {})
        min_target_pct = target_limits.get("minimum", 1)      # Default: 1%
        max_target_pct = target_limits.get("maximum", 2)      # Default: 2%
        
        min_target_value = round((min_target_pct / 100) * entry_price, 2)
        max_target_value = round((max_target_pct / 100) * entry_price, 2)

        # ===== CALCULATE 50-50 SPLIT OF FMV-TO-ENTRY GAP =====
        # Since we never buy above FMV or sell below FMV, the gap is always favorable
        gap               = abs(fmv_price - entry_price)
        your_profit_share = round(gap / 2, 2)
        
        # ===== CALCULATE DYNAMIC TARGET (MARGIN + 50% OF GAP) =====
        # Dynamic target combines margin requirements with half the FMV-to-entry gap
        buy_margin  = (config.get("margin", {}).get("buy", 0)/2)
        sell_margin = (config.get("margin", {}).get("sell", 0)/2)

        margin_value = 0
        if side == "BUY":
            margin_value = round((buy_margin / 100) * entry_price, 2)
        else:  # side == "S"
            margin_value = round((sell_margin / 100) * entry_price, 2)
        
        dynamic_target = round(margin_value + your_profit_share, 2)
        
        # ===== ENFORCE MIN/MAX PROFIT CONSTRAINTS =====
        # Apply min/max boundaries: final_target = min(max(dynamic, min), max)
        final_target = min(max(dynamic_target, min_target_value), max_target_value)

        # ===== APPLY SIDE LOGIC =====
        # For SELL side, return negative value (subtract from entry price)
        # For BUY side, return positive value (add to entry price)
        if side == "SELL":
            final_target = -final_target
            counter_price = round(entry_price + final_target, 2)  # entry - positive_target = entry - value
           
        else:  # side == "B"
            counter_price = round(entry_price + final_target, 2)  # entry + value
        return final_target
    
    except Exception as e:
        return None

def calculate_etf_fmv(config):
    """
    Calculate ETF Fair Market Value (FMV).
    
    FMV = ETF_close × (asset_open / asset_close) × (currency_open / currency_close)
    
    Returns:
        float: Calculated FMV, or None on error
    """
    try:
        # Extract values with proper error handling
        asset    = config.get("asset", {})
        currency = config.get("currency", {})
        
        open_price      = asset.get("open")
        close_price     = asset.get("close")
        currency_open   = currency.get("open", 1)
        currency_close  = currency.get("close", 1)
        etf_close       = config.get("ETF")

        # Validate required values
        if None in [open_price, close_price, etf_close]:
            return None
        
        # Validate numeric types
        if not all(isinstance(v, (int, float)) for v in [open_price, close_price, currency_open, currency_close, etf_close]):
            return None
        
        # Validate positive values
        if any(v <= 0 for v in [open_price, close_price, etf_close, currency_open, currency_close]):
            return None
        
        # Calculate components
        asset_ratio      = open_price / close_price
        currency_factor  = currency_open / currency_close

        
        # Calculate FMV
        fmv      = round(etf_close * asset_ratio * currency_factor, 2)
        
        return fmv
    
    except (KeyError, TypeError, ValueError) as e:
        return None

def read_bot_log(path):
    """Read bot log content from specified log file path"""
    p = Path(path)
    filename = p.stem
    try:
        account_in_file, ticker = filename.split("_",1) 
    except ValueError:
        raise ValueError(f"Invalid log file name format {path}, expected ACCOUNT_TICKER_*.json")
    account  = p.parent.name
    broker   = p.parent.parent.name  
    strategy = p.parent.parent.parent.name
    current_date = datetime.now().strftime('%Y-%m-%d')
    LOG_NAME     = f"{ticker}_{account}.log"
    LOG_PATH     = os.path.join(Config.LOGS_BASE_PATH, "STRATEGY_LOGS", strategy, broker, account,current_date, LOG_NAME)

    if os.path.exists(LOG_PATH):
        with open(LOG_PATH, 'r') as f:
            content = f.read()
        return {
            "success": True,
            "content": content,
            "path": LOG_PATH,
            "strategy": strategy,
            "broker": broker,
            "account": account,
            "ticker": ticker
        }
    else:
        raise FileNotFoundError(f"Log file not found at {LOG_PATH}")


@app.route("/api/bot/strategies")
def get_bot_strategies():
    """Get all available strategies from CONFIGS/STRATEGIES"""
    try:
        strategies_path = os.path.join(Config.CONFIG_BASE_PATH, "STRATEGIES")
        strategies = []
        
        if os.path.exists(strategies_path):
            # List all strategy folders (SHOONYA, DHAN, etc.)
            for broker_folder in os.listdir(strategies_path):
                broker_path = os.path.join(strategies_path, broker_folder)
                if os.path.isdir(broker_path):
                    strategies.append(broker_folder)
        
        return jsonify({"success": True, "strategies": sorted(strategies)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/bot/brokers")
def get_bot_brokers():
    """Get all brokers for a given strategy"""
    try:
        strategy = request.args.get("strategy", "")
        if not strategy:
            return jsonify({"success": False, "error": "Strategy parameter required"}), 400
        
        brokers_path = os.path.join(Config.CONFIG_BASE_PATH, "STRATEGIES", strategy)
        brokers = []
        
        if os.path.exists(brokers_path):
            # List all account folders (these are broker identifiers like FA394567)
            for account_folder in os.listdir(brokers_path):
                account_path = os.path.join(brokers_path, account_folder)
                if os.path.isdir(account_path):
                    brokers.append(account_folder)
        
        return jsonify({"success": True, "brokers": sorted(brokers)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/bot/accounts")
def get_bot_accounts():
    """Get all accounts for a given strategy and broker"""
    try:
        strategy = request.args.get("strategy", "")
        broker = request.args.get("broker", "")
        
        if not strategy or not broker:
            return jsonify({"success": False, "error": "Strategy and broker parameters required"}), 400
        
        accounts_path = os.path.join(Config.CONFIG_BASE_PATH, "STRATEGIES", strategy, broker)
        accounts = []
        
        if os.path.exists(accounts_path):
            # List all strategy type folders (e.g., ETF_FMV)
            for strategy_type in os.listdir(accounts_path):
                strategy_path = os.path.join(accounts_path, strategy_type)
                if os.path.isdir(strategy_path):
                    accounts.append(strategy_type)
        
        return jsonify({"success": True, "accounts": sorted(accounts)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/bot/config_details")
def get_bot_config_details():
    """Get details of a specific config file"""
    try:
        config_file = request.args.get("config_path", "")
        
        if not config_file:
            return jsonify({"success": False, "error": "Config path parameter required"}), 400
        
        config_details = {}
        
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    config_content = json.load(f)
                config_details = {
                    "name": os.path.basename(config_file),
                    "path": config_file,
                    "content": config_content
                }
            except json.JSONDecodeError:
                config_details = {
                    "name": os.path.basename(config_file),
                    "path": config_file,
                    "error": "Invalid JSON"
                }
        
        return jsonify({"success": True, "config": config_details})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/bot/configs")
def get_bot_configs():
    """Get all config files for a given strategy, broker, and account"""
    try:
        strategy = request.args.get("strategy", "")
        broker   = request.args.get("broker", "")
        account  = request.args.get("account", "")
        
        if not strategy or not broker or not account:
            return jsonify({"success": False, "error": "Strategy, broker, and account parameters required"}), 400
        
        configs_path = os.path.join(Config.CONFIG_BASE_PATH, "STRATEGIES", strategy, broker, account)
        configs = []
        
        if os.path.exists(configs_path):
            # List all JSON files in the directory
            for filename in sorted(os.listdir(configs_path)):
                if filename.endswith('.json'):
                    filepath = os.path.join(configs_path, filename)
                    try:
                        with open(filepath, 'r') as f:
                            config_content = json.load(f)
                        configs.append({
                            "name": filename,
                            "path": filepath,
                            "content": config_content
                        })
                    except json.JSONDecodeError:
                        configs.append({
                            "name": filename,
                            "path": filepath,
                            "error": "Invalid JSON"
                        })
        
        return jsonify({"success": True, "configs": configs})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/bot/logs")
def get_bot_logs():
    """Get bot log content for a given bot config path"""

    config_path = request.args.get("config_path")
    if not config_path:
        return jsonify({
            "success": False,
            "error": "Missing required parameter: config_path"
        }), 400

    try:
        logs = read_bot_log(config_path)
        return jsonify(logs), 200

    except ValueError as e:
        # Bad filename / invalid format
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

    except FileNotFoundError as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 404

    except PermissionError:
        return jsonify({
            "success": False,
            "error": "Permission denied while accessing log file"
        }), 403

    except Exception as e:
        # Real server error
        app.logger.exception("Unhandled error while reading bot logs")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500

@app.route("/api/bot/order_range")
def get_bot_order_range():
    """Get order range (min/max order amounts) for a given strategy, broker, and account"""
    try:
        config_file = request.args.get("config_path", "")
        
        if not config_file:
            return jsonify({"success": False, "error": "Config path parameter required"}), 400
        
        if os.path.exists(config_file):
           try:
                with open(config_file, 'r') as f:
                    config_content = json.load(f)
                etf_fmv     = calculate_etf_fmv(config_content)
                order_range = get_order_range(config_content, etf_fmv)

  
           except json.JSONDecodeError:
                config_file.append({
                "config": config_file,
                "error": "Invalid JSON"
            })
        
        return jsonify({"success": True, "order_ranges": order_range})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
