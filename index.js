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

// Validasi CSV dengan Zod
const CSVSchema = z.object({
  nomor_surat: z.string().optional(),
  nama_pegawai: z.string().optional(),
  nip: z.string().optional(),
  status_verifikasi: z.string().optional(),
  created_at: z.string().optional(),
  jabatan: z.string().optional(),
  perihal: z.string().optional(),
});

// Cek apakah tabel ada
async function tableExists() {
  const res = await pool.query(
    `SELECT to_regclass('public.${TABLE_NAME}') AS exists;`
  );
  return res.rows[0].exists !== null;
}

// Ambil kolom tabel di database
async function getTableColumns() {
  const res = await pool.query(`
    SELECT column_name 
    FROM information_schema.columns
    WHERE table_name = '${TABLE_NAME}';
  `);
  return res.rows.map(r => r.column_name);
}

// Auto buat tabel jika belum ada
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

  console.log("Tabel dibuat:", TABLE_NAME);
}

// Tambah kolom otomatis jika CSV punya kolom baru
async function addMissingColumns(csvColumns, tableColumns) {
  for (const col of csvColumns) {
    if (!tableColumns.includes(col)) {
      console.log(`Menambahkan kolom baru: ${col}`);
      await pool.query(`ALTER TABLE ${TABLE_NAME} ADD COLUMN ${col} TEXT;`);
    }
  }
}

app.post("/upload-csv", upload.single("file"), async (req, res) => {
  try {
    if (!req.file)
      return res.status(400).json({ error: "Tidak ada file CSV" });

    const csvText = req.file.buffer.toString("utf-8");

    const { data } = Papa.parse(csvText, {
      header: true,
      skipEmptyLines: true
    });

    // Validasi
    const validatedRows = [];
    for (const row of data) {
      if (Object.values(row).every(v => v === "")) continue; // skip baris kosong
      validatedRows.push(CSVSchema.parse(row));
    }

    // Buat tabel jika belum ada
    await createTableIfNotExists();

    // Ambil kolom tabel
    let tableColumns = await getTableColumns();

    // Cek kolom baru dari CSV
    const csvColumns = Object.keys(validatedRows[0] || {});
    await addMissingColumns(csvColumns, tableColumns);

    // Refresh ulang kolom tabel
    tableColumns = await getTableColumns();

    // Insert per row
    for (const row of validatedRows) {
      const values = tableColumns.map(col => {
        let val = row[col] || null;

        // Auto convert created_at
        if (col === "created_at" && val) {
          const d = new Date(val);
          if (!isNaN(d)) {
            val = d.toISOString(); // simpan dalam format standard
          }
        }

        return val;
      });

      const placeholders = values.map((_, i) => `$${i + 1}`).join(",");

      await pool.query(
        `INSERT INTO ${TABLE_NAME} (${tableColumns.join(",")})
         VALUES (${placeholders})`,
        values
      );
    }

    return res.json({
      message: "CSV berhasil diupload, divalidasi, dan dimasukkan!"
    });

  } catch (err) {
    console.error("UPLOAD ERROR:", err);
    return res.status(500).json({
      error: err.message,
      detail: err.stack
    });
  }
});

app.listen(8080, () => console.log("Server berjalan di port 8080"));
