const sqlite3 = require('sqlite3').verbose();
const path = require('path');
let app;
try {
  // In Electron runtime this will be available; in plain Node tests it may not.
  app = require('electron').app;
} catch (e) {
  app = null;
}
const os = require('os');
const fs = require('fs');

let db;
let schemaReadyPromise = null;

function initDB() {
  return new Promise((resolve, reject) => {
    try {
      // Support running inside Electron and plain Node (tests).
      let userDataPath;
      if (app && typeof app.getPath === 'function') {
        userDataPath = app.getPath('userData');
      } else {
        // Fallback to a hidden folder in the user's home directory or an env override
        userDataPath = process.env.PROACTIVE_USER_DATA_DIR || path.join(os.homedir(), '.proactive');
      }
      if (!fs.existsSync(userDataPath)) {
        fs.mkdirSync(userDataPath, { recursive: true });
      }

      const dbPath = path.join(userDataPath, 'proactive_graph.db');
      console.log('Initializing SQLite database at:', dbPath);
      
      db = new sqlite3.Database(dbPath, (err) => {
        if (err) return reject(err);
      });

      db.serialize(() => {
        // Layer 1: Raw Events (Source of Truth)
        db.run(`CREATE TABLE IF NOT EXISTS events (
          id TEXT PRIMARY KEY,
          type TEXT NOT NULL,
          timestamp TEXT NOT NULL,
          source TEXT NOT NULL,
          text TEXT,
          metadata TEXT
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS event_entities (
          event_id TEXT,
          entity TEXT,
          FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE
        )`);

        // Layer 2-5: Graph Nodes (Episodes, Semantics, Insights, Core)
        db.run(`CREATE TABLE IF NOT EXISTS nodes (
          id TEXT PRIMARY KEY,
          type TEXT NOT NULL,
          data TEXT,
          embedding TEXT
        )`);

        // Edges connecting Nodes to Nodes, or Events to Nodes
        db.run(`CREATE TABLE IF NOT EXISTS edges (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          from_id TEXT NOT NULL,
          to_id TEXT NOT NULL,
          relation TEXT NOT NULL,
          data TEXT DEFAULT '{}'
        )`);

        // Migration: ensure data column exists in edges (in case it was created without it)
        db.all("PRAGMA table_info(edges)", (err, columns) => {
          if (!err && columns) {
            const hasDataCol = columns.some(col => col.name === 'data');
            if (!hasDataCol) {
              console.log('Migrating edges table: adding data column');
              db.run("ALTER TABLE edges ADD COLUMN data TEXT DEFAULT '{}'");
            }
          }
        });

        // Indices for fast retrieval (e.g., 20m suggestion loop & chat embeddings)
        db.run(`CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)`);
        db.run(`CREATE INDEX IF NOT EXISTS idx_event_entities_entity ON event_entities(entity)`);
        db.run(`CREATE INDEX IF NOT EXISTS idx_nodes_type ON nodes(type)`);
        db.run(`CREATE INDEX IF NOT EXISTS idx_edges_from ON edges(from_id)`);
        db.run(`CREATE INDEX IF NOT EXISTS idx_edges_to ON edges(to_id)`);

        // Retrieval & search tables used by the graph store / retrieval layer
        db.run(`CREATE TABLE IF NOT EXISTS retrieval_docs (
          doc_id TEXT PRIMARY KEY,
          source_type TEXT,
          node_id TEXT,
          event_id TEXT,
          app TEXT,
          timestamp TEXT,
          text TEXT,
          metadata TEXT
        )`);

        // Memory & graph-related tables
        db.run(`CREATE TABLE IF NOT EXISTS memory_nodes (
          id TEXT PRIMARY KEY,
          layer TEXT,
          subtype TEXT,
          title TEXT,
          summary TEXT,
          canonical_text TEXT,
          confidence REAL,
          status TEXT,
          source_refs TEXT,
          metadata TEXT,
          graph_version TEXT,
          created_at TEXT,
          updated_at TEXT,
          embedding TEXT,
          anchor_date TEXT,
          anchor_at TEXT
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS memory_edges (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          from_node_id TEXT,
          to_node_id TEXT,
          edge_type TEXT,
          weight REAL,
          trace_label TEXT,
          evidence_count INTEGER,
          metadata TEXT,
          created_at TEXT
        )`);

        // Small supporting artifacts
        db.run(`CREATE TABLE IF NOT EXISTS suggestion_artifacts (
          id TEXT PRIMARY KEY,
          suggestion_id TEXT,
          data TEXT
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS text_chunks (
          id TEXT PRIMARY KEY,
          doc_id TEXT,
          chunk_index INTEGER,
          text TEXT,
          metadata TEXT
        )`);

        // Migration: older DBs may have a narrow text_chunks schema.
        // Add the columns used by ingestion/retrieval if they are missing.
        db.all("PRAGMA table_info(text_chunks)", (err, columns) => {
          if (err || !columns) return;
          const existing = new Set(columns.map((col) => col.name));
          const required = [
            ['event_id', 'TEXT'],
            ['node_id', 'TEXT'],
            ['embedding', 'TEXT'],
            ['timestamp', 'TEXT'],
            ['date', 'TEXT'],
            ['app', 'TEXT'],
            ['data_source', 'TEXT']
          ];
          for (const [name, type] of required) {
            if (!existing.has(name)) {
              db.run(`ALTER TABLE text_chunks ADD COLUMN ${name} ${type}`);
            }
          }
        });

        db.run(`CREATE TABLE IF NOT EXISTS graph_versions (
          version TEXT PRIMARY KEY,
          status TEXT,
          started_at TEXT,
          completed_at TEXT,
          metadata TEXT
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS retrieval_runs (
          id TEXT PRIMARY KEY,
          query TEXT,
          mode TEXT,
          created_at TEXT,
          metadata TEXT
        )`);

        // Durable chat session storage (renderer can hydrate chat history on startup)
        db.run(`CREATE TABLE IF NOT EXISTS chat_sessions (
          id TEXT PRIMARY KEY,
          title TEXT,
          created_at TEXT,
          updated_at TEXT,
          metadata TEXT
        )`);

        db.run(`CREATE TABLE IF NOT EXISTS chat_messages (
          id TEXT PRIMARY KEY,
          session_id TEXT NOT NULL,
          role TEXT NOT NULL,
          content TEXT,
          retrieval TEXT,
          thinking_trace TEXT,
          ts INTEGER,
          created_at TEXT,
          FOREIGN KEY(session_id) REFERENCES chat_sessions(id) ON DELETE CASCADE
        )`);

        db.run(`CREATE INDEX IF NOT EXISTS idx_chat_sessions_updated_at ON chat_sessions(updated_at)`);
        db.run(`CREATE INDEX IF NOT EXISTS idx_chat_messages_session_ts ON chat_messages(session_id, ts)`);

        resolve();
      });
    } catch (e) {
      reject(e);
    }
  });
}

function getDB() {
  if (!db) throw new Error('Database not initialized');
  return db;
}

function runStatement(sql, params = []) {
  return new Promise((resolve, reject) => {
    try {
      db.run(sql, params, function(err) {
        if (err) reject(err);
        else resolve(this);
      });
    } catch (err) {
      reject(err);
    }
  });
}

function allStatement(sql, params = []) {
  return new Promise((resolve, reject) => {
    try {
      db.all(sql, params, (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    } catch (err) {
      reject(err);
    }
  });
}

async function ensureSchemaMigrations() {
  if (schemaReadyPromise) return schemaReadyPromise;
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const required = [
    ['event_id', 'TEXT'],
    ['node_id', 'TEXT'],
    ['embedding', 'TEXT'],
    ['timestamp', 'TEXT'],
    ['date', 'TEXT'],
    ['app', 'TEXT'],
    ['data_source', 'TEXT']
  ];

  schemaReadyPromise = (async () => {
    for (let attempt = 0; attempt < 3; attempt++) {
      const cols = await allStatement(`PRAGMA table_info(text_chunks)`).catch(() => []);
      const existing = new Set((cols || []).map((c) => c?.name).filter(Boolean));
      if (!cols.length) {
        await sleep(20);
        continue;
      }
      for (const [name, sqlType] of required) {
        if (!existing.has(name)) {
          await runStatement(`ALTER TABLE text_chunks ADD COLUMN ${name} ${sqlType}`).catch(() => {});
        }
      }

      const verifyCols = await allStatement(`PRAGMA table_info(text_chunks)`).catch(() => []);
      const verifySet = new Set((verifyCols || []).map((c) => c?.name).filter(Boolean));
      const stillMissing = required.filter(([name]) => !verifySet.has(name));
      if (!stillMissing.length) {
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_text_chunks_event_id ON text_chunks(event_id)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_text_chunks_timestamp ON text_chunks(timestamp)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_text_chunks_data_source ON text_chunks(data_source)`).catch(() => {});
        const memoryCols = await allStatement(`PRAGMA table_info(memory_nodes)`).catch(() => []);
        const memoryExisting = new Set((memoryCols || []).map((c) => c?.name).filter(Boolean));
        if (!memoryExisting.has('anchor_date')) {
          await runStatement(`ALTER TABLE memory_nodes ADD COLUMN anchor_date TEXT`).catch(() => {});
        }
        if (!memoryExisting.has('anchor_at')) {
          await runStatement(`ALTER TABLE memory_nodes ADD COLUMN anchor_at TEXT`).catch(() => {});
        }
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_memory_nodes_anchor_date ON memory_nodes(anchor_date)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_memory_nodes_anchor_at ON memory_nodes(anchor_at)`).catch(() => {});
        return;
      }

      await sleep(40);
    }

    throw new Error('text_chunks schema migration incomplete');
  })().catch((error) => {
    schemaReadyPromise = null;
    throw error;
  });
  return schemaReadyPromise;
}

// Helpers
// Ensure DB is initialized before running queries. This guards against
// cases where other modules call queries before `initDB()` finished.
async function ensureDB() {
  if (db) {
    await ensureSchemaMigrations();
    return;
  }
  // initDB may throw if the Electron app isn't ready; propagate that clearly
  await initDB();
  await ensureSchemaMigrations();
}

const runQuery = async (sql, params = []) => {
  try {
    await ensureDB();
  } catch (e) {
    return Promise.reject(new Error(`DB not available: ${e.message}`));
  }
  return new Promise((resolve, reject) => {
    try {
      db.run(sql, params, function(err) {
        if (err) reject(err); else resolve(this);
      });
    } catch (err) {
      reject(err);
    }
  });
};

const getQuery = async (sql, params = []) => {
  try {
    await ensureDB();
  } catch (e) {
    return Promise.reject(new Error(`DB not available: ${e.message}`));
  }
  return new Promise((resolve, reject) => {
    try {
      db.get(sql, params, (err, row) => {
        if (err) reject(err); else resolve(row);
      });
    } catch (err) {
      reject(err);
    }
  });
};

const allQuery = async (sql, params = []) => {
  try {
    await ensureDB();
  } catch (e) {
    return Promise.reject(new Error(`DB not available: ${e.message}`));
  }
  return new Promise((resolve, reject) => {
    try {
      db.all(sql, params, (err, rows) => {
        if (err) reject(err); else resolve(rows);
      });
    } catch (err) {
      reject(err);
    }
  });
};

module.exports = {
  initDB,
  getDB,
  runQuery,
  getQuery,
  allQuery
};
