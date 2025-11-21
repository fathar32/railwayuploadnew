const express = require('express');
const multer = require('multer');
const csvParser = require('csv-parser');
const { Pool } = require('pg');
const stream = require('stream');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const upload = multer({ storage: multer.memoryStorage() });

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error('ERROR: DATABASE_URL environment variable missing.');
  process.exit(1);
}

const pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } });

async function ensureTable() {
  const createSql = `
    CREATE TABLE IF NOT EXISTS csv_uploads (
      id BIGSERIAL PRIMARY KEY,
      uploaded_at TIMESTAMPTZ DEFAULT now(),
      data JSONB
    );
  `;
  await pool.query(createSql);
}

app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false });
  }
});

app.post('/upload-csv', upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'Missing file field (file)' });

  try {
    await ensureTable();

    const rows = [];
    const bufferStream = new stream.PassThrough();
    bufferStream.end(req.file.buffer);

    await new Promise((resolve, reject) => {
      bufferStream
        .pipe(csvParser())
        .on('data', (data) => rows.push(data))
        .on('end', resolve)
        .on('error', reject);
    });

    if (rows.length === 0) return res.status(400).json({ error: 'CSV kosong atau tidak valid' });

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const values = [];
      const params = [];
      let idx = 1;

      for (const row of rows) {
        params.push(`$${idx++}`);
        values.push(JSON.stringify(row));
      }

      const insertSql = `INSERT INTO csv_uploads (data) VALUES ${params.map(p => `(${p}::jsonb)`).join(',')}`;
      await client.query(insertSql, values);
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }

    res.json({ ok: true, rows_inserted: rows.length, message: `Upload berhasil! ${rows.length} baris disimpan.` });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/uploads', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT id, uploaded_at, data FROM csv_uploads ORDER BY uploaded_at DESC LIMIT 50');
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Server berjalan di port ${PORT}`));
