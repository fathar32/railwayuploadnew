const express = require("express");
const multer = require("multer");
const Papa = require("papaparse");
const { Pool } = require("pg");
const cors = require("cors");
const { z } = require("zod");

require("dotenv").config();

const upload = multer();
const app = express();
app.use(cors());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const TABLE_NAME = "berkas_verifikasi";

// Zod validation
const CSVSchema = z.object({
  nomor_surat: z.string(),
  nama_pegawai: z.string(),
  nip: z.string(),
  status_verifikasi: z.string(),
  created_at: z.string().optional(),
  jabatan: z.string(),
  perihal: z.string(),
});

// Normalizer untuk kolom
const normalizeValue = (key, value) => {
  if (value === "" || value === null || value === undefined) return null;

  if (key === "created_at") {
    const d = new Date(value);
    return isNaN(d.getTime()) ? null : d.toISOString();
  }

  return value;
};

app.post("/upload-csv", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: "Tidak ada file CSV" });

    const csvText = req.file.buffer.toString("utf-8");
    const { data } = Papa.parse(csvText, {
      header: true,
      skipEmptyLines: true
    });

    const validatedRows = data.map((row) => CSVSchema.parse(row));

    for (const row of validatedRows) {
      const columns = Object.keys(row);
      const values = columns.map((key) => normalizeValue(key, row[key]));

      const placeholders = values.map((_, i) => `$${i + 1}`).join(",");

      await pool.query(
        `INSERT INTO ${TABLE_NAME} (${columns.join(",")}) 
         VALUES (${placeholders})`,
        values
      );
    }

    res.json({ message: "CSV berhasil diupload & ditambahkan ke tabel!" });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(8080, () => console.log("Server berjalan di port 8080"));
