const sqlite3 = (process.env.NODE_ENV === 'production') ? require('sqlite3') : require('sqlite3').verbose();
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
let currentDbPath = null;

const SQLITE_BUSY_RETRY_LIMIT = 8;
const SQLITE_BUSY_BASE_DELAY_MS = 30;

function isSqliteBusyError(err) {
  if (!err) return false;
  return err.code === 'SQLITE_BUSY' || /database is locked|SQLITE_BUSY/i.test(String(err.message || ''));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function safeWarn(...args) {
  try {
    console.warn(...args);
  } catch (_) {
    // ignore EPIPE / closed stream writes
  }
}

async function withSqliteBusyRetry(operation) {
  let lastError = null;
  for (let attempt = 0; attempt <= SQLITE_BUSY_RETRY_LIMIT; attempt++) {
    try {
      return await operation();
    } catch (err) {
      if (!isSqliteBusyError(err) || attempt === SQLITE_BUSY_RETRY_LIMIT) {
        throw err;
      }
      lastError = err;
      const delayMs = SQLITE_BUSY_BASE_DELAY_MS * (attempt + 1);
      await sleep(delayMs);
    }
  }
  throw lastError || new Error('SQLITE_BUSY: database is locked');
}

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
      currentDbPath = dbPath;
      console.log('Initializing SQLite database at:', dbPath);
      
      db = new sqlite3.Database(
        dbPath,
        sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE | sqlite3.OPEN_FULLMUTEX,
        (err) => {
          if (err) return reject(err);
        }
      );

      db.configure('busyTimeout', 8000);
      const pragmaSql = `
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA foreign_keys = ON;
        PRAGMA busy_timeout = 8000;
      `;
      const applyPragmas = (attempt = 0) => {
        db.exec(pragmaSql, (pragmaErr) => {
          if (!pragmaErr) return;
          if (isSqliteBusyError(pragmaErr) && attempt < SQLITE_BUSY_RETRY_LIMIT) {
            const delayMs = SQLITE_BUSY_BASE_DELAY_MS * (attempt + 1);
            setTimeout(() => applyPragmas(attempt + 1), delayMs);
            return;
          }
          safeWarn('[DB] Failed to apply PRAGMAs:', pragmaErr.message);
        });
      };
      applyPragmas();

      const safeSchemaRun = (sql, attempt = 0) => {
        db.run(sql, (err) => {
          if (!err) return;
          const transientNoSuchTable = /no such table/i.test(String(err?.message || ''));
          if ((isSqliteBusyError(err) || transientNoSuchTable) && attempt < SQLITE_BUSY_RETRY_LIMIT) {
            const delayMs = SQLITE_BUSY_BASE_DELAY_MS * (attempt + 1);
            setTimeout(() => safeSchemaRun(sql, attempt + 1), delayMs);
            return;
          }
          safeWarn('[DB] Schema statement failed:', err.message);
        });
      };

      db.serialize(() => {
        // Layer 1: Raw Events (Source of Truth)
        safeSchemaRun(`CREATE TABLE IF NOT EXISTS events (
          id TEXT PRIMARY KEY,
          type TEXT NOT NULL,
          timestamp TEXT NOT NULL,
          source TEXT NOT NULL,
          text TEXT,
          metadata TEXT,
          ocr_hash TEXT
        )`);

        safeSchemaRun(`CREATE TABLE IF NOT EXISTS event_entities (
          event_id TEXT,
          entity TEXT,
          FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE
        )`);

        // Layer 2-5: Graph Nodes (Episodes, Semantics, Insights, Core)
        safeSchemaRun(`CREATE TABLE IF NOT EXISTS nodes (
          id TEXT PRIMARY KEY,
          type TEXT NOT NULL,
          data TEXT,
          embedding TEXT
        )`);

        // Edges connecting Nodes to Nodes, or Events to Nodes
        safeSchemaRun(`CREATE TABLE IF NOT EXISTS edges (
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
              safeSchemaRun("ALTER TABLE edges ADD COLUMN data TEXT DEFAULT '{}'");
            }
          }
        });

        // Indices for fast retrieval (e.g., 20m suggestion loop & chat embeddings)
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_event_entities_entity ON event_entities(entity)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_nodes_type ON nodes(type)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_edges_from ON edges(from_id)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_edges_to ON edges(to_id)`);

        // Retrieval & search tables used by the graph store / retrieval layer
        safeSchemaRun(`CREATE TABLE IF NOT EXISTS retrieval_docs (
          doc_id TEXT PRIMARY KEY,
          source_type TEXT,
          node_id TEXT,
          event_id TEXT,
          app TEXT,
          timestamp TEXT,
          text TEXT,
          metadata TEXT
        )`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_retrieval_docs_app ON retrieval_docs(app)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_retrieval_docs_timestamp ON retrieval_docs(timestamp)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_retrieval_docs_source_type ON retrieval_docs(source_type)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_retrieval_docs_node_id ON retrieval_docs(node_id)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_retrieval_docs_event_id ON retrieval_docs(event_id)`);

        safeSchemaRun(`CREATE VIRTUAL TABLE IF NOT EXISTS retrieval_docs_fts USING fts5(
          doc_id UNINDEXED,
          text,
          tokenize = 'porter unicode61 remove_diacritics 2'
        )`);

        safeSchemaRun(`CREATE TRIGGER IF NOT EXISTS retrieval_docs_ai AFTER INSERT ON retrieval_docs BEGIN
          INSERT INTO retrieval_docs_fts(doc_id, text) VALUES (new.doc_id, COALESCE(new.text, ''));
        END`);

        safeSchemaRun(`CREATE TRIGGER IF NOT EXISTS retrieval_docs_au AFTER UPDATE ON retrieval_docs BEGIN
          DELETE FROM retrieval_docs_fts WHERE doc_id = old.doc_id;
          INSERT INTO retrieval_docs_fts(doc_id, text) VALUES (new.doc_id, COALESCE(new.text, ''));
        END`);

        safeSchemaRun(`CREATE TRIGGER IF NOT EXISTS retrieval_docs_ad AFTER DELETE ON retrieval_docs BEGIN
          DELETE FROM retrieval_docs_fts WHERE doc_id = old.doc_id;
        END`);

        // Memory & graph-related tables
        safeSchemaRun(`CREATE TABLE IF NOT EXISTS memory_nodes (
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

        safeSchemaRun(`CREATE TABLE IF NOT EXISTS memory_edges (
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
        safeSchemaRun(`CREATE TABLE IF NOT EXISTS suggestion_artifacts (
          id TEXT PRIMARY KEY,
          suggestion_id TEXT,
          data TEXT,
          type TEXT,
          title TEXT,
          body TEXT,
          trigger_summary TEXT,
          source_node_ids TEXT,
          source_edge_paths TEXT,
          confidence REAL,
          status TEXT DEFAULT 'active',
          metadata TEXT,
          created_at TEXT
        )`);

        safeSchemaRun(`CREATE TABLE IF NOT EXISTS relationship_contacts (
          id TEXT PRIMARY KEY,
          display_name TEXT NOT NULL,
          company TEXT,
          role TEXT,
          strength_score REAL DEFAULT 0,
          warmth_score REAL DEFAULT 0,
          depth_score REAL DEFAULT 0,
          network_centrality REAL DEFAULT 0,
          last_interaction_at TEXT,
          interaction_count_30d INTEGER DEFAULT 0,
          relationship_tier TEXT,
          status TEXT DEFAULT 'warm',
          relationship_summary TEXT,
          metadata TEXT,
          created_at TEXT,
          updated_at TEXT
        )`);

        safeSchemaRun(`CREATE TABLE IF NOT EXISTS relationship_contact_identifiers (
          id TEXT PRIMARY KEY,
          contact_id TEXT NOT NULL,
          identifier_type TEXT NOT NULL,
          identifier_value TEXT NOT NULL,
          normalized_value TEXT NOT NULL,
          source_label TEXT,
          confidence REAL DEFAULT 1,
          created_at TEXT,
          updated_at TEXT,
          FOREIGN KEY(contact_id) REFERENCES relationship_contacts(id) ON DELETE CASCADE
        )`);

        safeSchemaRun(`CREATE TABLE IF NOT EXISTS relationship_mentions (
          id TEXT PRIMARY KEY,
          contact_id TEXT NOT NULL,
          event_id TEXT,
          memory_node_id TEXT,
          retrieval_doc_id TEXT,
          timestamp TEXT,
          source_app TEXT,
          context_snippet TEXT,
          confidence REAL DEFAULT 0.7,
          mention_type TEXT,
          metadata TEXT,
          created_at TEXT,
          FOREIGN KEY(contact_id) REFERENCES relationship_contacts(id) ON DELETE CASCADE
        )`);

        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_relationship_contacts_status ON relationship_contacts(status)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_relationship_contacts_last_interaction ON relationship_contacts(last_interaction_at)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_relationship_contacts_strength ON relationship_contacts(strength_score)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_relationship_identifiers_contact ON relationship_contact_identifiers(contact_id)`);
        safeSchemaRun(`CREATE UNIQUE INDEX IF NOT EXISTS idx_relationship_identifiers_unique ON relationship_contact_identifiers(identifier_type, normalized_value)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_relationship_mentions_contact ON relationship_mentions(contact_id)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_relationship_mentions_event ON relationship_mentions(event_id)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_relationship_mentions_timestamp ON relationship_mentions(timestamp)`);

        safeSchemaRun(`CREATE TABLE IF NOT EXISTS text_chunks (
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
              safeSchemaRun(`ALTER TABLE text_chunks ADD COLUMN ${name} ${type}`);
            }
          }
        });

        safeSchemaRun(`CREATE TABLE IF NOT EXISTS graph_versions (
          version TEXT PRIMARY KEY,
          status TEXT,
          started_at TEXT,
          completed_at TEXT,
          metadata TEXT
        )`);

        safeSchemaRun(`CREATE TABLE IF NOT EXISTS retrieval_runs (
          id TEXT PRIMARY KEY,
          query TEXT,
          mode TEXT,
          created_at TEXT,
          metadata TEXT
        )`);

        // Durable chat session storage (renderer can hydrate chat history on startup)
        safeSchemaRun(`CREATE TABLE IF NOT EXISTS chat_sessions (
          id TEXT PRIMARY KEY,
          title TEXT,
          created_at TEXT,
          updated_at TEXT,
          metadata TEXT
        )`);

        safeSchemaRun(`CREATE TABLE IF NOT EXISTS chat_messages (
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

        safeSchemaRun(`CREATE TABLE IF NOT EXISTS kv_cache (
          key TEXT PRIMARY KEY,
          value TEXT,
          type TEXT,
          created_at TEXT
        )`);

        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_chat_sessions_updated_at ON chat_sessions(updated_at)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_chat_messages_session_ts ON chat_messages(session_id, ts)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_kv_cache_type ON kv_cache(type)`);

        safeSchemaRun(`CREATE TABLE IF NOT EXISTS scheduled_automations (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          description TEXT,
          prompt TEXT NOT NULL,
          interval_minutes INTEGER NOT NULL DEFAULT 60,
          enabled INTEGER NOT NULL DEFAULT 1,
          last_run_at TEXT,
          next_run_at TEXT,
          created_at TEXT,
          metadata TEXT
        )`);

        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_scheduled_automations_next_run ON scheduled_automations(next_run_at, enabled)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_memory_edges_from ON memory_edges(from_node_id)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_memory_edges_to ON memory_edges(to_node_id)`);
        safeSchemaRun(`CREATE INDEX IF NOT EXISTS idx_memory_nodes_layer ON memory_nodes(layer)`);

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
  return withSqliteBusyRetry(() => {
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
  });
}

function allStatement(sql, params = []) {
  return withSqliteBusyRetry(() => {
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
  });
}

async function ensureSchemaMigrations() {
  if (schemaReadyPromise) return schemaReadyPromise;
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
        await runStatement(`
          UPDATE memory_nodes
          SET anchor_at = COALESCE(
            NULLIF(anchor_at, ''),
            NULLIF(json_extract(metadata, '$.anchor_at'), ''),
            NULLIF(json_extract(metadata, '$.occurred_at'), ''),
            NULLIF(json_extract(metadata, '$.event_time'), ''),
            NULLIF(json_extract(metadata, '$.timestamp'), ''),
            NULLIF(created_at, ''),
            NULLIF(updated_at, '')
          )
          WHERE anchor_at IS NULL OR anchor_at = ''
        `).catch(() => {});
        await runStatement(`
          UPDATE memory_nodes
          SET anchor_date = substr(COALESCE(
            NULLIF(anchor_at, ''),
            NULLIF(json_extract(metadata, '$.anchor_at'), ''),
            NULLIF(json_extract(metadata, '$.occurred_at'), ''),
            NULLIF(json_extract(metadata, '$.event_time'), ''),
            NULLIF(json_extract(metadata, '$.timestamp'), ''),
            NULLIF(created_at, ''),
            NULLIF(updated_at, '')
          ), 1, 10)
          WHERE anchor_date IS NULL OR anchor_date = ''
        `).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_memory_nodes_anchor_date ON memory_nodes(anchor_date)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_memory_nodes_anchor_at ON memory_nodes(anchor_at)`).catch(() => {});

        await runStatement(`CREATE VIRTUAL TABLE IF NOT EXISTS retrieval_docs_fts USING fts5(
          doc_id UNINDEXED,
          text,
          tokenize = 'porter unicode61 remove_diacritics 2'
        )`).catch(() => {});
        await runStatement(`CREATE TRIGGER IF NOT EXISTS retrieval_docs_ai AFTER INSERT ON retrieval_docs BEGIN
          INSERT INTO retrieval_docs_fts(doc_id, text) VALUES (new.doc_id, COALESCE(new.text, ''));
        END`).catch(() => {});
        await runStatement(`CREATE TRIGGER IF NOT EXISTS retrieval_docs_au AFTER UPDATE ON retrieval_docs BEGIN
          DELETE FROM retrieval_docs_fts WHERE doc_id = old.doc_id;
          INSERT INTO retrieval_docs_fts(doc_id, text) VALUES (new.doc_id, COALESCE(new.text, ''));
        END`).catch(() => {});
        await runStatement(`CREATE TRIGGER IF NOT EXISTS retrieval_docs_ad AFTER DELETE ON retrieval_docs BEGIN
          DELETE FROM retrieval_docs_fts WHERE doc_id = old.doc_id;
        END`).catch(() => {});
        const ftsCheck = await allStatement(`SELECT doc_id FROM retrieval_docs_fts LIMIT 1`).catch(() => []);
        if (ftsCheck.length === 0) {
          await runStatement(`INSERT INTO retrieval_docs_fts(doc_id, text) SELECT doc_id, COALESCE(text, '') FROM retrieval_docs`).catch(() => {});
        }
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_retrieval_docs_app ON retrieval_docs(app)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_retrieval_docs_timestamp ON retrieval_docs(timestamp)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_retrieval_docs_source_type ON retrieval_docs(source_type)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_retrieval_docs_node_id ON retrieval_docs(node_id)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_retrieval_docs_event_id ON retrieval_docs(event_id)`).catch(() => {});

        // Migration for events table (ocr_hash, sentiment_score, session_id, status)
        const eventCols = await allStatement(`PRAGMA table_info(events)`).catch(() => []);
        const eventExisting = new Set((eventCols || []).map((c) => c?.name).filter(Boolean));
        if (!eventExisting.has('ocr_hash')) {
          await runStatement(`ALTER TABLE events ADD COLUMN ocr_hash TEXT`).catch(() => {});
        }
        if (!eventExisting.has('sentiment_score')) {
          await runStatement(`ALTER TABLE events ADD COLUMN sentiment_score REAL`).catch(() => {});
        }
        if (!eventExisting.has('session_id')) {
          await runStatement(`ALTER TABLE events ADD COLUMN session_id TEXT`).catch(() => {});
        }
        if (!eventExisting.has('status')) {
          await runStatement(`ALTER TABLE events ADD COLUMN status TEXT DEFAULT 'active'`).catch(() => {});
        }
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_events_ocr_hash ON events(ocr_hash)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_events_sentiment_score ON events(sentiment_score)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_events_session_id ON events(session_id)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_events_status ON events(status)`).catch(() => {});

        // Migration for memory_nodes table (importance, connection_count, last_reheated)
        const memoryNodeCols = await allStatement(`PRAGMA table_info(memory_nodes)`).catch(() => []);
        const memoryNodeExisting = new Set((memoryNodeCols || []).map((c) => c?.name).filter(Boolean));
        if (!memoryNodeExisting.has('importance')) {
          await runStatement(`ALTER TABLE memory_nodes ADD COLUMN importance INTEGER DEFAULT 5`).catch(() => {});
        }
        if (!memoryNodeExisting.has('connection_count')) {
          await runStatement(`ALTER TABLE memory_nodes ADD COLUMN connection_count INTEGER DEFAULT 0`).catch(() => {});
        }
        if (!memoryNodeExisting.has('last_reheated')) {
          await runStatement(`ALTER TABLE memory_nodes ADD COLUMN last_reheated TEXT`).catch(() => {});
        }
        // Initialize last_reheated from updated_at for existing nodes
        await runStatement(`
          UPDATE memory_nodes
          SET last_reheated = COALESCE(NULLIF(last_reheated, ''), updated_at)
          WHERE last_reheated IS NULL OR last_reheated = ''
        `).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_memory_nodes_importance ON memory_nodes(importance)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_memory_nodes_connection_count ON memory_nodes(connection_count)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_memory_nodes_last_reheated ON memory_nodes(last_reheated)`).catch(() => {});

        const suggestionCols = await allStatement(`PRAGMA table_info(suggestion_artifacts)`).catch(() => []);
        const suggestionExisting = new Set((suggestionCols || []).map((c) => c?.name).filter(Boolean));
        const suggestionRequired = [
          ['suggestion_id', 'TEXT'],
          ['data', 'TEXT'],
          ['type', 'TEXT'],
          ['title', 'TEXT'],
          ['body', 'TEXT'],
          ['trigger_summary', 'TEXT'],
          ['source_node_ids', 'TEXT'],
          ['source_edge_paths', 'TEXT'],
          ['confidence', 'REAL'],
          ['status', "TEXT DEFAULT 'active'"],
          ['metadata', 'TEXT'],
          ['created_at', 'TEXT']
        ];
        for (const [name, sqlType] of suggestionRequired) {
          if (!suggestionExisting.has(name)) {
            await runStatement(`ALTER TABLE suggestion_artifacts ADD COLUMN ${name} ${sqlType}`).catch(() => {});
          }
        }
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_suggestion_artifacts_status ON suggestion_artifacts(status)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_suggestion_artifacts_created_at ON suggestion_artifacts(created_at)`).catch(() => {});

        await runStatement(`CREATE TABLE IF NOT EXISTS relationship_contacts (
          id TEXT PRIMARY KEY,
          display_name TEXT NOT NULL,
          company TEXT,
          role TEXT,
          strength_score REAL DEFAULT 0,
          warmth_score REAL DEFAULT 0,
          depth_score REAL DEFAULT 0,
          network_centrality REAL DEFAULT 0,
          last_interaction_at TEXT,
          interaction_count_30d INTEGER DEFAULT 0,
          relationship_tier TEXT,
          status TEXT DEFAULT 'warm',
          relationship_summary TEXT,
          metadata TEXT,
          created_at TEXT,
          updated_at TEXT
        )`).catch(() => {});

        // Migration for relationship_contacts
        const relContactCols = await allStatement(`PRAGMA table_info(relationship_contacts)`).catch(() => []);
        const relContactExisting = new Set((relContactCols || []).map((c) => c?.name).filter(Boolean));
        const relContactRequired = [
          ['warmth_score', 'REAL DEFAULT 0'],
          ['depth_score', 'REAL DEFAULT 0'],
          ['network_centrality', 'REAL DEFAULT 0'],
          ['relationship_summary', 'TEXT']
        ];
        for (const [name, sqlType] of relContactRequired) {
          if (!relContactExisting.has(name)) {
            await runStatement(`ALTER TABLE relationship_contacts ADD COLUMN ${name} ${sqlType}`).catch(() => {});
          }
        }

        await runStatement(`CREATE TABLE IF NOT EXISTS relationship_contact_identifiers (
          id TEXT PRIMARY KEY,
          contact_id TEXT NOT NULL,
          identifier_type TEXT NOT NULL,
          identifier_value TEXT NOT NULL,
          normalized_value TEXT NOT NULL,
          source_label TEXT,
          confidence REAL DEFAULT 1,
          created_at TEXT,
          updated_at TEXT,
          FOREIGN KEY(contact_id) REFERENCES relationship_contacts(id) ON DELETE CASCADE
        )`).catch(() => {});
        await runStatement(`CREATE TABLE IF NOT EXISTS relationship_mentions (
          id TEXT PRIMARY KEY,
          contact_id TEXT NOT NULL,
          event_id TEXT,
          memory_node_id TEXT,
          retrieval_doc_id TEXT,
          timestamp TEXT,
          source_app TEXT,
          context_snippet TEXT,
          confidence REAL DEFAULT 0.7,
          mention_type TEXT,
          metadata TEXT,
          created_at TEXT,
          FOREIGN KEY(contact_id) REFERENCES relationship_contacts(id) ON DELETE CASCADE
        )`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_relationship_contacts_status ON relationship_contacts(status)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_relationship_contacts_last_interaction ON relationship_contacts(last_interaction_at)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_relationship_contacts_strength ON relationship_contacts(strength_score)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_relationship_identifiers_contact ON relationship_contact_identifiers(contact_id)`).catch(() => {});
        await runStatement(`CREATE UNIQUE INDEX IF NOT EXISTS idx_relationship_identifiers_unique ON relationship_contact_identifiers(identifier_type, normalized_value)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_relationship_mentions_contact ON relationship_mentions(contact_id)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_relationship_mentions_event ON relationship_mentions(event_id)`).catch(() => {});
        await runStatement(`CREATE INDEX IF NOT EXISTS idx_relationship_mentions_timestamp ON relationship_mentions(timestamp)`).catch(() => {});

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

let dbInitialized = false;

// Helpers
// Ensure DB is initialized before running queries. This guards against
// cases where other modules call queries before `initDB()` finished.
async function ensureDB() {
  if (dbInitialized) return;
  if (db) {
    await ensureSchemaMigrations();
    dbInitialized = true;
    return;
  }
  // initDB may throw if the Electron app isn't ready; propagate that clearly
  await initDB();
  await ensureSchemaMigrations();
  dbInitialized = true;
}

const runQuery = async (sql, params = []) => {
  if (!db) {
    return Promise.reject(new Error(`DB not available`));
  }
  return withSqliteBusyRetry(() => {
    return new Promise((resolve, reject) => {
      try {
        db.run(sql, params, function(err) {
          if (err) reject(err); else resolve(this);
        });
      } catch (err) {
        reject(err);
      }
    });
  });
};

const getQuery = async (sql, params = []) => {
  if (!db) {
    return Promise.reject(new Error(`DB not available`));
  }
  return withSqliteBusyRetry(() => {
    return new Promise((resolve, reject) => {
      try {
        db.get(sql, params, (err, row) => {
          if (err) reject(err); else resolve(row);
        });
      } catch (err) {
        reject(err);
      }
    });
  });
};

const allQuery = async (sql, params = []) => {
  if (!db) {
    return Promise.reject(new Error(`DB not available`));
  }
  return withSqliteBusyRetry(() => {
    return new Promise((resolve, reject) => {
      try {
        db.all(sql, params, (err, rows) => {
          if (err) reject(err); else resolve(rows);
        });
      } catch (err) {
        reject(err);
      }
    });
  });
};

function getDbPath() {
  return currentDbPath;
}

module.exports = {
  initDB,
  getDB,
  getDbPath,
  runQuery,
  getQuery,
  allQuery,
  ensureDB
};
