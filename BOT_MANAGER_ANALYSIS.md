# Bot Manager Tab - Detailed Analysis

## Overview
The Bot Manager is a comprehensive tab in the Heimdall order viewer that provides a hierarchical interface to manage trading bot configurations, view logs, and monitor bot status. It follows a cascading dropdown pattern: Strategy → Broker → Account → Configurations.

---

## 1. UI Architecture

### 1.1 Main Container Structure
**File:** [templates/index.html](templates/index.html#L2880-L2935)

```html
<!-- Bot Manager Tab -->
<div id="bot-manager-tab" class="tab-content">
    <div class="bot-manager-container">
        <!-- Filter Section -->
        <div class="bot-manager-filters">
            <!-- Dropdown Selectors -->
        </div>
        
        <!-- Configs Table -->
        <div id="bot-configs-container" style="display: none;">
            <!-- Rendered configurations -->
        </div>
        
        <!-- Empty State -->
        <div id="bot-empty-state" class="empty-state">
            <!-- Default message -->
        </div>
    </div>
</div>
```

### 1.2 Styling Details
**Location:** [templates/index.html](templates/index.html#L2032-L2145)

#### Container Styles
- `.bot-manager-container`: Flexbox column layout with 20px padding and 20px gap
- `.bot-manager-filters`: Dark background (#1a1a1a) with 1px border (#333), 20px padding, rounded corners
- Color scheme: Cyan (#00d4ff) for headers, grey tones for text

#### Table Styles
- `.bot-configs-table`: 100% width, 0.95rem font size
- **Header**: Sticky position (top: 0), gradient background (cyan to blue), z-index: 10
- **Rows**: Hover effect with cyan background (0.08 opacity)
- **Cells**: 12px vertical padding, 15px horizontal padding, 1px border-bottom

#### Component Styles
- `.bot-ticker-name`: Cyan color, monospace font, 0.95rem, font-weight 600
- `.bot-config-status`: Inline-flex with 6px gap, rounded corners, 0.85rem font
  - `.valid`: Green background (74, 222, 128) with 0.2 opacity
  - `.invalid`: Red background (255, 107, 107) with 0.2 opacity
- `.bot-margin-needed`: ₹ symbol prefix, fixed decimal places (2)

---

## 2. Filter Hierarchy

### 2.1 Cascading Dropdown System

**Strategy Selection** (onchange: `onBotStrategyChange()`)
- Initial state: Loaded on page load via `loadBotStrategies()`
- API endpoint: `/api/bot/strategies`
- Triggers: Broker dropdown loading
- Clears: Broker and Account selectors
- Hides: Config table and shows empty state

**Broker Selection** (onchange: `onBotBrokerChange()`)
- Initial state: Disabled until strategy selected
- API endpoint: `/api/bot/brokers?strategy={strategy}`
- Triggers: Account dropdown loading
- Clears: Account selector
- Hides: Config table

**Account Selection** (onchange: `onBotAccountChange()`)
- Initial state: Disabled until broker selected
- API endpoint: `/api/bot/accounts?strategy={strategy}&broker={broker}`
- Triggers: Config table loading and rendering
- Shows: Config table if configs exist, else empty state

### 2.2 Selection Logic Flow
```
User selects Strategy
  ↓
onBotStrategyChange() 
  → Disables broker/account
  → Hides configs table
  → Fetches brokers from API
  → Enables broker dropdown
  
User selects Broker
  ↓
onBotBrokerChange()
  → Disables account
  → Hides configs table
  → Fetches accounts from API
  → Enables account dropdown
  
User selects Account
  ↓
onBotAccountChange()
  → Fetches configs from API
  → Renders config table OR shows empty state
```

---

## 3. Configuration Table Structure

### 3.1 Table Columns
**Location:** [templates/index.html](templates/index.html#L2910-L2922)

| Column | Purpose | Content | Data Source |
|--------|---------|---------|-------------|
| **Ticker** | Asset identifier | Extracted from config filename | config.name.split('_')[1] |
| **Config Status** | JSON validation | ✓ Valid / ✕ Invalid JSON | config.error check |
| **Hearbeat** | Bot health indicator | ✓ Valid / ✕ Invalid | Same as Config Status |
| **Actions** | Control buttons | Start/Stop/View/Order Range | Function calls |
| **Margin Needed** | Required trading capital | ₹ amount (2 decimal places) | Calculated from config |
| **Logs** | Debug information | View Logs button | Bot log file access |

### 3.2 Data Rendering (`renderBotConfigs()`)
**Location:** [templates/index.html](templates/index.html#L5247-L5275)

```javascript
function renderBotConfigs(configs, account) {
    const configsBody = document.getElementById('bot-configs-body');
    
    configsBody.innerHTML = configs.map(config => {
        // Extract ticker from config filename (e.g., "ACCOUNT_TICKER.json" → "TICKER")
        const ticker = config.name.split('_')[1]?.replace('.json', '') || 'N/A';
        
        // Determine status (valid/invalid based on JSON parsing)
        const hasError = !!config.error;
        const status = hasError ? 'invalid' : 'valid';
        const statusText = hasError ? 'Invalid JSON' : 'Valid';
        
        // Generate row HTML with embedded function calls
        return `<tr>
            <td><span class="bot-ticker-name">${ticker}</span></td>
            <td><span class="bot-config-status ${status}">...</span></td>
            <td><span class="bot-config-status ${status}">...</span></td>
            <td>
                <div class="bot-config-actions">
                    <button onclick="startBot('${config.path}', '${ticker}')">Start</button>
                    <button onclick="stopBot('${config.path}', '${ticker}')">Stop</button>
                    <button onclick="viewBotConfig('${config.path}', '${config.name}')">View Config</button>
                    <button onclick="viewOrderRange('${config.path}', '${ticker}')">View Order Range</button>
                </div>
            </td>
            <td><span class="bot-margin-needed">₹ ${config.margin_needed || '0.00'}</span></td>
            <td><button onclick="viewBotLogs('${config.path}', '${ticker}')">View Logs</button></td>
        </tr>`;
    }).join('');
}
```

---

## 4. API Endpoints

### 4.1 Backend Routes
**File:** [app.py](app.py#L2230-L2433)

#### GET /api/bot/strategies
- **Purpose**: List all available trading strategies
- **Location**: `Config.CONFIG_BASE_PATH/STRATEGIES/`
- **Returns**: `{ success: bool, strategies: [string] }`
- **Implementation**: Scans directory for broker folders (SHOONYA, etc.)

#### GET /api/bot/brokers
- **Parameters**: `strategy` (required)
- **Purpose**: List brokers for a strategy
- **Returns**: `{ success: bool, brokers: [string] }`
- **Path**: `STRATEGIES/{strategy}/`
- **Implementation**: Lists subdirectories (broker identifiers like FA394567)

#### GET /api/bot/accounts
- **Parameters**: `strategy`, `broker` (both required)
- **Purpose**: List trading accounts/strategy types
- **Returns**: `{ success: bool, accounts: [string] }`
- **Path**: `STRATEGIES/{strategy}/{broker}/`
- **Implementation**: Lists strategy type folders (ETF_FMV, etc.)

#### GET /api/bot/configs
- **Parameters**: `strategy`, `broker`, `account` (all required)
- **Purpose**: List all config files with metadata
- **Returns**: `{ success: bool, configs: [ConfigObject] }`
- **Path**: `STRATEGIES/{strategy}/{broker}/{account}/`
- **Implementation**: 
  - Scans for `.json` files
  - Parses JSON and calculates margin needed
  - Returns error if JSON invalid

**ConfigObject Structure:**
```json
{
    "name": "ACCOUNT_TICKER.json",
    "path": "/full/path/to/config.json",
    "content": { /* parsed JSON */ },
    "margin_needed": 15000.50,
    "error": null || "Invalid JSON"
}
```

#### GET /api/bot/config_details
- **Parameters**: `config_path` (required)
- **Purpose**: Load full configuration file content
- **Returns**: `{ success: bool, config: ConfigObject }`
- **Implementation**: Reads and parses JSON file

#### GET /api/bot/logs
- **Parameters**: `config_path` (required)
- **Purpose**: Retrieve bot execution logs
- **Returns**: `{ success: bool, content: string, path, strategy, broker, account, ticker }`
- **Log Path Construction**:
  ```
  {LOGS_BASE_PATH}/STRATEGY_LOGS/{strategy}/{broker}/{account}/{YYYY-MM-DD}/{ticker}_{account}.log
  ```
- **Processing**: Filters out DEBUG level messages

**Function:** `read_bot_log(path)` [app.py](app.py#L2165-L2197)
- Extracts metadata from config filepath
- Constructs log file path
- Filters DEBUG messages
- Returns with metadata

#### GET /api/bot/order_range
- **Parameters**: `config_path` (required)
- **Purpose**: Get min/max order amounts from config
- **Returns**: `{ success: bool, order_ranges: { buy: {}, sell: {} } }`
- **Implementation**: Parses config, calculates ETF FMV, generates order ranges

#### POST /api/bot/start
- **Parameters**: JSON body with `config_path`, `ticker`
- **Purpose**: Start bot as background process
- **Returns**: `{ success: bool, error?: string }`
- **Status**: Implementation incomplete (last endpoint in file)

### 4.2 Margin Calculation Algorithm
**Function:** `get_margin_needed(config_content)` [app.py](app.py#L2199-L2227)

```python
def get_margin_needed(config_content):
    """
    Calculate total margin needed for BUY + SELL orders.
    Margin = 20% of (price * quantity)
    """
    margin_needed = 0.0
    
    # Get ETF FMV (Fair Market Value)
    etf_fmv = calculate_etf_fmv(config_content)
    
    # Get order range (min/max prices and quantities)
    order_range = get_order_range(config_content, etf_fmv)
    
    # Sum margin for all buy and sell orders
    for side in ("buy", "sell"):
        side_orders = order_range.get(side, {})
        for ticker_key, price_map in side_orders.items():
            for price_str, order in price_map.items():
                price = float(price_str)
                qty = int(order.get("QTY", 0))
                margin_needed += price * qty * 0.20  # 20% margin requirement
    
    return round(margin_needed, 2)
```

---

## 5. Modal Interactions

### 5.1 Config Details Modal
**Triggered by**: "View Config" button
**Function**: `viewBotConfig(path, name)` [templates/index.html](templates/index.html#L5280-L5338)

- **Modal ID**: `bot-config-modal`
- **Content**: Renders config JSON as nested table using `renderConfigTable()`
- **Display**: Flexbox modal overlay
- **Close**: Click X button or click outside modal
- **Features**:
  - Escapes HTML to prevent XSS
  - Renders nested objects recursively
  - Syntax-highlighted types (numbers, booleans, arrays)

### 5.2 Config Table Structure
**Function**: `renderConfigTable(obj, parentKey)` [templates/index.html](templates/index.html#L5338-L5428)

Recursively renders JSON objects as HTML tables:
- **Keys**: Full dot-notation path (e.g., "strategy.order_range.buy")
- **Values**: Type-specific formatting
  - `null/undefined`: Muted text
  - Numbers: Green color (#059669)
  - Booleans: Red color (#dc2626)
  - Arrays: Comma-joined strings
  - Objects: Nested table
  - Strings: Escaped HTML

### 5.3 Bot Log Modal
**Triggered by**: "View Logs" button
**Function**: `viewBotLogs(path, ticker)` [templates/index.html](templates/index.html#L5556-L5575)

**Modal ID**: `bot-log-modal`

**Flow**:
1. Store `currentLogConfigPath` and `currentLogTicker`
2. Create modal if not exists via `ensureBotLogModalExists()`
3. Display loading placeholder
4. Call `loadBotLogs()`

**Loading Process**: `loadBotLogs()` [templates/index.html](templates/index.html#L5577-L5605)
```javascript
async function loadBotLogs() {
    try {
        // Fetch logs from backend
        const response = await fetchJSON(
            `/api/bot/logs?config_path=${encodeURIComponent(currentLogConfigPath)}`
        );
        
        // Convert to string if array
        const logsText = Array.isArray(response.content)
            ? response.content.join('\n')
            : String(response.content);
        
        // Escape HTML (critical for security)
        const safeLogs = escapeHtml(logsText);
        
        // Apply syntax highlighting for log levels
        const logsHtml = highlightLogLevels(safeLogs);
        
        // Render and auto-scroll
        document.getElementById("bot-log-content").innerHTML = logsHtml;
        autoScrollLogs();
    } catch (err) {
        // Display error in log container
    }
}
```

**Log Level Highlighting**: `highlightLogLevels(logText)` [templates/index.html](templates/index.html#L5528-L5554)
- Splits text by newlines
- Adds CSS class based on log level:
  - `.log-error`: Contains "| ERROR"
  - `.log-warning`: Contains "| WARNING"
  - `.log-info`: Contains "| INFO"
  - `.log-debug`: Contains "| DEBUG"
- Escapes HTML entities
- Wraps each line in div

**CSS Styling** (log levels):
- `.log-error`: Red background (#991b1b), white text
- `.log-warning`: Yellow background (#b45309), dark text
- `.log-info`: Blue background (#0c4a6e), white text
- `.log-debug`: Gray background (#374151), light text
- `.log-line`: Monospace font, 13px, padding 8px 12px

### 5.4 Order Range Modal
**Triggered by**: "View Order Range" button
**Function**: `viewOrderRange()` (referenced but implementation details not shown)

- Displays min/max order amounts by price level
- Shows BUY and SELL orders separately
- Tables sorted by price

---

## 6. Action Handlers

### 6.1 Start Bot
**Function**: `startBot(path, ticker)` [templates/index.html](templates/index.html#L5441-L5468)

```javascript
async function startBot(path, ticker) {
    try {
        const response = await fetch('/api/bot/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                config_path: path,
                ticker: ticker
            })
        });

        const data = await response.json();

        if (!data.success) {
            alert(`❌ Failed to start bot: ${data.error}`);
            return;
        }

        alert(`✅ Bot started: ${ticker}`);
    } catch (err) {
        alert('❌ Network / server error while starting bot');
    }
}
```

**Backend Route**: POST `/api/bot/start` [app.py](app.py#L2433)
- Status: Incomplete implementation
- Parameters: `config_path`, `ticker`
- Should: Start bot as detached subprocess

### 6.2 Stop Bot
**Function**: `stopBot(path, ticker)` [templates/index.html](templates/index.html#L5469-L5472)
- Currently: Placeholder (alert only)
- Status: Not implemented

### 6.3 View Config
**Function**: `viewBotConfig(path, name)` [templates/index.html](templates/index.html#L5280-L5338)
- Opens modal with config details
- Renders JSON as nested table
- Error handling for invalid JSON

### 6.4 View Logs
**Function**: `viewBotLogs(path, ticker)` [templates/index.html](templates/index.html#L5556-L5575)
- Opens log modal
- Fetches and renders logs with syntax highlighting
- Auto-scrolls to latest entries

---

## 7. Utility Functions

### 7.1 Populate Select Dropdowns
**Function**: `populateSelect(selectId, options, placeholder)`
- Clears existing options
- Adds placeholder option first
- Adds options from array
- Used for Strategy, Broker, Account dropdowns

### 7.2 Fetch JSON Helper
**Function**: `fetchJSON(url)`
- Wrapper around fetch with JSON parsing
- Error handling
- Returns parsed response

### 7.3 Escape HTML
**Function**: `escapeHtml(text)`
- Replaces &, <, > with entities
- Prevents XSS attacks
- Applied to all user-facing content

### 7.4 Auto-scroll Logs
**Function**: `autoScrollLogs()`
- Scrolls log container to bottom
- Shows latest log entries
- Called after rendering logs

---

## 8. Data Flow Diagram

```
PAGE LOAD
    ↓
loadBotStrategies()
    ↓
Populate Strategy dropdown
    ↓
[User selects Strategy]
    ↓
onBotStrategyChange()
    ├→ Fetch /api/bot/brokers
    ├→ Populate Broker dropdown
    └→ Enable Broker select
    
[User selects Broker]
    ↓
onBotBrokerChange()
    ├→ Fetch /api/bot/accounts
    ├→ Populate Account dropdown
    └→ Enable Account select
    
[User selects Account]
    ↓
onBotAccountChange()
    ├→ Fetch /api/bot/configs
    ├→ renderBotConfigs() renders table
    └→ Show configs or empty state
    
[User clicks Action Button]
    ↓
startBot() / stopBot() / viewBotConfig() / viewBotLogs()
    ├→ Fetch additional data if needed
    ├→ Display modal or alert
    └→ Execute bot command
```

---

## 9. State Management

### 9.1 Global Variables
```javascript
let currentLogConfigPath = null;    // Track which config's logs to show
let currentLogTicker = null;        // Track bot ticker for log modal title
```

### 9.2 DOM Element Visibility States
- `.bot-configs-container`: Hidden until Account selected and configs loaded
- `.bot-empty-state`: Shown initially, hidden when configs loaded
- Various modals: Hidden by default, shown on action

### 9.3 Dropdown Disabled States
- **Broker**: Disabled until Strategy selected
- **Account**: Disabled until Broker selected

---

## 10. Security Considerations

### 10.1 XSS Prevention
1. **HTML Escaping**: All dynamic content (config values, logs, ticker names) passed through `escapeHtml()`
2. **Template Strings**: Using backticks for dynamic values in HTML generation
3. **Modal Injection**: Modals reused and content replaced, not full HTML injection

### 10.2 SQL Injection
- No direct SQL queries in Bot Manager section
- File path operations use safe path construction

### 10.3 CSP Headers
- Content-Security-Policy allows:
  - `'self'`: Same-origin resources
  - `'unsafe-inline'`: Inline styles (necessary for modal rendering)
  - `'unsafe-eval'`: Needed for complex rendering logic

---

## 11. Browser Compatibility

- **Modern JavaScript Features Used**:
  - `async/await`
  - Template literals (backticks)
  - `Object.entries()`
  - `Array.map()`, `.join()`
- **Fallbacks**: None currently implemented
- **Target Browsers**: Chrome, Firefox, Safari (modern versions)

---

## 12. Performance Considerations

### 12.1 Rendering
- **Lazy Loading**: Configs only loaded when Account selected
- **String Concatenation**: Used for large table generation (could optimize with DocumentFragment)
- **Table Size**: No pagination visible, could be issue with many configs

### 12.2 API Calls
- **Cascading Requests**: Required for dropdown population
- **Caching**: No caching implemented, fresh requests each selection
- **Error Handling**: Graceful fallback to empty state or error message

### 12.3 DOM Operations
- **Reusing Modals**: Better than creating new ones each time
- **innerHTML**: Used for rendering, potential repaint issues with large tables

---

## 13. Known Issues & TODOs

1. **Stop Bot Not Implemented**: Currently placeholder only
2. **Order Range Modal**: Function called but implementation not visible
3. **Backend /api/bot/start**: Incomplete implementation in app.py
4. **Heartbeat Column**: Displays same as Config Status (appears to be duplicate)
5. **No Pagination**: Could slow down with many configs
6. **No Search/Filter**: Cannot search configs in table
7. **Edit Config**: Not implemented (mentioned but no functionality)
8. **Margin Calculation**: Depends on missing helper functions (`calculate_etf_fmv`, `get_order_range`)

---

## 14. Component Relationships

```
Bot Manager Tab
├── Filter Section
│   ├── Strategy Select (→ onBotStrategyChange)
│   ├── Broker Select (→ onBotBrokerChange)
│   └── Account Select (→ onBotAccountChange)
├── Config Table
│   ├── Ticker Column
│   ├── Config Status Column
│   ├── Heartbeat Column
│   ├── Actions Column
│   │   ├── Start Button
│   │   ├── Stop Button
│   │   ├── View Config Button
│   │   └── View Order Range Button
│   ├── Margin Needed Column
│   └── Logs Column
│       └── View Logs Button
└── Modals
    ├── Config Details Modal
    ├── Log Viewer Modal
    └── Order Range Modal (referenced)

API Layer
├── /api/bot/strategies
├── /api/bot/brokers
├── /api/bot/accounts
├── /api/bot/configs
├── /api/bot/config_details
├── /api/bot/logs
├── /api/bot/order_range
└── /api/bot/start
```

---

## 15. File Structure Reference

### Frontend
- **Main Template**: [templates/index.html](templates/index.html)
  - HTML: Lines 2880-2935
  - CSS: Lines 2032-2145
  - JavaScript: Lines 5129-5650+

### Backend
- **API Routes**: [app.py](app.py)
  - Routes: Lines 2230-2433
  - Helper Functions: Lines 2165-2227

---

## Summary

The Bot Manager is a sophisticated, hierarchical configuration and monitoring interface that allows users to:

1. **Navigate** bot configurations through Strategy → Broker → Account selection
2. **View** detailed configuration files as nested tables in modal dialogs
3. **Monitor** bot health through status indicators and heartbeat info
4. **Control** bots with start/stop functionality
5. **Debug** using comprehensive log viewing with syntax highlighting
6. **Calculate** required trading capital (margin needed)

The implementation demonstrates good UX practices with cascading dropdowns, modal dialogs, and graceful error handling, though some features remain incomplete (Stop functionality, Order Range modal, backend bot start process).
