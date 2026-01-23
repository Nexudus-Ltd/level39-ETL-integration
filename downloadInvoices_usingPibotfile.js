/**
 * Nexudus Invoices → Data Excel (ETL safe)
 */

require("dotenv").config();

const axios = require("axios");
const fs = require("fs-extra");
const XLSX = require("xlsx");
const path = require("path");

const OUTPUT_DIR = path.join(__dirname, "output");
const DATA_FILE = path.join(OUTPUT_DIR, "ETL_MarioDemo_Data.xlsx");

const REPORT_URL = "https://reports.nexudus.com/ReportCenter/Invoices";

async function downloadReport() {
  const response = await axios.get(REPORT_URL, {
    params: {
      businessId: process.env.BUSINESS_ID,
      reportName: "Invoices/InvoicesAccount",
      start: "2026-01-02T00:00:00",
      end: "2026-01-31T23:59:59",
      format: "Excel",
      portrait: false,
      rnd: new Date().toISOString()
    },
    responseType: "arraybuffer",
    headers: {
      Authorization: `Bearer ${process.env.NEXUDUS_TOKEN}`
    }
  });

  return response.data;
}

async function writeDataFile(excelBuffer) {
  await fs.ensureDir(OUTPUT_DIR);

  // Read Nexudus workbook
  const sourceWorkbook = XLSX.read(excelBuffer, { type: "buffer" });
  const sheetName = sourceWorkbook.SheetNames[0];
  const sheet = sourceWorkbook.Sheets[sheetName];

  // Create clean data-only workbook
  const dataWorkbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(dataWorkbook, sheet, "Invoices");

  XLSX.writeFile(dataWorkbook, DATA_FILE);
}

(async () => {
  try {
    console.log("Downloading Nexudus invoice report...");
    const buffer = await downloadReport();

    console.log("Writing ETL data file...");
    await writeDataFile(buffer);

    console.log("✔ Data file ready:");
    console.log(DATA_FILE);
  } catch (err) {
    console.error("✖ Failed:", err.message);
  }
})();
