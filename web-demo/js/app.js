// ============================================================================
// NistMemSQL WASM Demo Application
// ============================================================================

// Global database instance
let db = null;

// DOM Elements
const elements = {
    status: document.getElementById('status'),
    statusText: document.getElementById('status-text'),
    sqlInput: document.getElementById('sql-input'),
    executeBtn: document.getElementById('execute-btn'),
    exampleBtn: document.getElementById('example-btn'),
    clearBtn: document.getElementById('clear-btn'),
    results: document.getElementById('results'),
    resultMeta: document.getElementById('result-meta'),
    version: document.getElementById('version')
};

// ============================================================================
// Initialization
// ============================================================================

/**
 * Initialize the WASM module and set up the UI
 */
async function init() {
    try {
        updateStatus('loading', 'Initializing WASM module...');

        // Import the WASM module
        // Note: The path will need to be adjusted based on where wasm-pack builds to
        const wasmModule = await import('../pkg/wasm_bindings.js');

        // Initialize WASM
        await wasmModule.default();

        // Create database instance
        db = new wasmModule.Database();

        // Get version
        const version = db.version();
        elements.version.textContent = version;

        updateStatus('ready', 'Ready! Enter a SQL query to get started.');
        elements.executeBtn.disabled = false;

        // Set up event listeners
        setupEventListeners();

        // Load default example
        loadExample();

    } catch (error) {
        console.error('Failed to initialize WASM:', error);
        updateStatus('error', `Failed to initialize: ${error.message}`);
        displayError('Initialization Error', error.message);
    }
}

/**
 * Set up UI event listeners
 */
function setupEventListeners() {
    // Execute button
    elements.executeBtn.addEventListener('click', executeQuery);

    // Example button
    elements.exampleBtn.addEventListener('click', loadExample);

    // Clear button
    elements.clearBtn.addEventListener('click', () => {
        elements.sqlInput.value = '';
        elements.sqlInput.focus();
    });

    // Keyboard shortcuts
    elements.sqlInput.addEventListener('keydown', (e) => {
        // Ctrl/Cmd + Enter to execute
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            e.preventDefault();
            executeQuery();
        }
    });
}

// ============================================================================
// Query Execution
// ============================================================================

/**
 * Execute the SQL query entered by the user
 */
async function executeQuery() {
    const sql = elements.sqlInput.value.trim();

    if (!sql) {
        displayError('Empty Query', 'Please enter a SQL query.');
        return;
    }

    if (!db) {
        displayError('Database Not Ready', 'Database is not initialized yet.');
        return;
    }

    try {
        // Clear previous results
        elements.resultMeta.textContent = '';

        // Determine query type and execute
        const sqlUpper = sql.toUpperCase();
        const isSelect = sqlUpper.startsWith('SELECT');

        if (isSelect) {
            // Execute SELECT query
            const result = db.query(sql);

            // Display results
            displayResults(result);

            // Update metadata
            const rowText = result.row_count === 1 ? 'row' : 'rows';
            elements.resultMeta.textContent = `${result.row_count} ${rowText} returned`;

        } else {
            // Execute DDL/DML statement
            const result = db.execute(sql);

            // Display success message
            displaySuccess(result.message);

            // Update metadata
            if (result.rows_affected > 0) {
                const rowText = result.rows_affected === 1 ? 'row' : 'rows';
                elements.resultMeta.textContent = `${result.rows_affected} ${rowText} affected`;
            }
        }

    } catch (error) {
        console.error('Query execution error:', error);
        displayError('Execution Error', error.toString());
    }
}

// ============================================================================
// Results Display
// ============================================================================

/**
 * Display query results as a table
 * @param {Object} result - Query result with columns and rows
 */
function displayResults(result) {
    if (!result || result.row_count === 0) {
        displayEmptyResults();
        return;
    }

    // Create table
    const table = document.createElement('table');
    table.className = 'results-table';

    // Create header
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');

    result.columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = col;
        headerRow.appendChild(th);
    });

    thead.appendChild(headerRow);
    table.appendChild(thead);

    // Create body
    const tbody = document.createElement('tbody');

    result.rows.forEach(rowData => {
        const row = document.createElement('tr');

        // Parse row data (it's a JSON string)
        let values;
        try {
            values = JSON.parse(rowData);
        } catch {
            // If parsing fails, treat as string array
            values = [rowData];
        }

        values.forEach(value => {
            const td = document.createElement('td');

            // Format value for display
            if (value === null || value === undefined) {
                td.innerHTML = '<code>NULL</code>';
                td.style.fontStyle = 'italic';
                td.style.opacity = '0.6';
            } else if (typeof value === 'string') {
                // Remove quotes from string values if present
                const cleaned = value.replace(/^["']|["']$/g, '');
                td.textContent = cleaned;
            } else {
                td.textContent = String(value);
            }

            row.appendChild(td);
        });

        tbody.appendChild(row);
    });

    table.appendChild(tbody);

    // Replace results container content
    elements.results.innerHTML = '';
    elements.results.appendChild(table);
}

/**
 * Display empty results message
 */
function displayEmptyResults() {
    elements.results.innerHTML = `
        <div class="empty-state">
            <p>Query returned no results.</p>
        </div>
    `;
}

/**
 * Display error message
 * @param {string} title - Error title
 * @param {string} message - Error message
 */
function displayError(title, message) {
    elements.results.innerHTML = `
        <div class="error-message">
            <h3>${escapeHtml(title)}</h3>
            <p>${escapeHtml(message)}</p>
        </div>
    `;
}

/**
 * Display success message for DDL/DML operations
 * @param {string} message - Success message
 */
function displaySuccess(message) {
    elements.results.innerHTML = `
        <div class="success-message">
            <p>${escapeHtml(message)}</p>
        </div>
    `;
}

/**
 * Update status indicator
 * @param {string} state - Status state: 'loading', 'ready', 'error'
 * @param {string} text - Status text
 */
function updateStatus(state, text) {
    elements.status.className = `status ${state}`;
    elements.statusText.textContent = text;
}

/**
 * Escape HTML to prevent XSS
 * @param {string} text - Text to escape
 * @returns {string} Escaped text
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// ============================================================================
// Example Queries
// ============================================================================

/**
 * Load an example query into the textarea
 */
function loadExample() {
    const exampleQuery = `-- Create a users table
CREATE TABLE users (
    id INTEGER,
    name VARCHAR,
    email VARCHAR,
    age INTEGER
);

-- Insert some data
INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30);
INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 25);
INSERT INTO users (id, name, email, age) VALUES (3, 'Charlie', 'charlie@example.com', 35);

-- Query the data
SELECT * FROM users WHERE age >= 30 ORDER BY name;`;

    elements.sqlInput.value = exampleQuery;
    elements.sqlInput.focus();

    // Clear results
    elements.results.innerHTML = `
        <div class="empty-state">
            <p>Example query loaded! Click "Execute" to run it.</p>
        </div>
    `;
    elements.resultMeta.textContent = '';
}

// ============================================================================
// Start Application
// ============================================================================

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}
