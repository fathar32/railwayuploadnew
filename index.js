import express from "express";
import multer from "multer";
import { Pool } from "pg";
import Papa from "papaparse";
import { z } from "zod";

const upload = multer();
const app = express();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const TABLE_NAME = "berkas_verifikasi"; // <-- pakai tabel ini selalu

// validasi CSV pakai Zod
const CSVSchema = z.object({
  nomor_surat: z.string(),
  nama_pegawai: z.string(),
  nip: z.string(),
  status_verifikasi: z.string(),
  created_at: z.string(),
  jabatan: z.string(),
  perihal: z.string(),
});

async function tableExists() {
  const res = await pool.query(
    `SELECT to_regclass('public.${TABLE_NAME}') AS exists;`
  );
  return res.rows[0].exists !== null;
}

async function getTableColumns() {
  const res = await pool.query(
    `SELECT column_name FROM information_schema.columns
     WHERE table_name = '${TABLE_NAME}';`
  );
  return res.rows.map(r => r.column_name);
}

async function createTableIfNotExists() {
  const exists = await tableExists();
  if (exists) return;

  await pool.query(`
    CREATE TABLE ${TABLE_NAME} (
      nomor_surat TEXT,
      nama_pegawai TEXT,
      nip TEXT,
      status_verifikasi TEXT,
      created_at TEXT,
      jabatan TEXT,
      perihal TEXT
    );
  `);
}

app.post("/upload-csv", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: "Tidak ada file CSV" });

    const csvText = req.file.buffer.toString("utf-8");

    const { data } = Papa.parse(csvText, {
      header: true,
      skipEmptyLines: true
    });

    // Validasi tiap baris
    const validatedRows = [];
    for (const row of data) {
      validatedRows.push(CSVSchema.parse(row));
    }

    // Buat table jika belum ada
    await createTableIfNotExists();

    // Insert
    for (const row of validatedRows) {
      const cols = Object.keys(row);
      const values = Object.values(row).map(v => v === "" ? null : v);

      const placeholders = values.map((_, i) => `$${i+1}`).join(",");

      await pool.query(
        `INSERT INTO ${TABLE_NAME}(${cols.join(",")})
         VALUES (${placeholders})`,
        values
      );
    }

    return res.json({ message: "CSV berhasil diupload & ditambahkan ke tabel!" });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
});

app.listen(8080, () => console.log("Server berjalan di port 8080"));
