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

const TABLE_NAME = "berkas_verifikasi";

// Validasi CSV pakai Zod
const CSVSchema = z.object({
  nomor_surat: z.string(),
  nama_pegawai: z.string(),
  nip: z.string(),
  status_verifikasi: z.string(),
  created_at: z.string().optional(), // tidak dipakai, dihapus sebelum insert
  jabatan: z.string(),
  perihal: z.string(),
});

// Cek apakah tabel ada
async function tableExists() {
  const res = await pool.query(
    `SELECT to_regclass('public.${TABLE_NAME}') AS exists;`
  );
  return res.rows[0].exists !== null;
}

// Membuat tabel jika belum ada
async function createTableIfNotExists() {
  const exists = await tableExists();
  if (exists) return;

  await pool.query(`
    CREATE TABLE ${TABLE_NAME} (
      id SERIAL PRIMARY KEY,
      nomor_surat TEXT,
      nama_pegawai TEXT,
      nip TEXT,
      status_verifikasi TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      jabatan TEXT,
      perihal TEXT
    );
  `);

  console.log("TABLE CREATED:", TABLE_NAME);
}

// Endpoint upload CSV
app.post("/upload-csv", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: "Tidak ada file CSV" });

    const csvText = req.file.buffer.toString("utf-8");

    const { data } = Papa.parse(csvText, {
      header: true,
      skipEmptyLines: true
    });

    const validatedRows = [];
    for (const row of data) {
      const parsed = CSVSchema.parse(row);

      // Buang kolom otomatis
      delete parsed.id;
      delete parsed.created_at;

      validatedRows.push(parsed);
    }

    await createTableIfNotExists();

    // Insert data
    for (const row of validatedRows) {
      const cols = Object.keys(row);
      const values = Object.values(row).map(v => (v === "" ? null : v));

      const placeholders = values.map((_, i) => `$${i + 1}`).join(",");

      await pool.query(
        `INSERT INTO ${TABLE_NAME} (${cols.join(",")})
         VALUES (${placeholders})`,
        values
      );
    }

    return res.json({
      message: "CSV berhasil diupload & ditambahkan ke database!"
    });

  } catch (err) {
    console.error("UPLOAD ERROR:", err);
    return res.status(500).json({ error: err.message });
  }
});

app.listen(8080, () => console.log("Server berjalan di port 8080"));
