/**
 * Nexudus Invoices → ETL Excel Copier
 * ----------------------------------
 * Authenticates via /api/token
 * Copies Nexudus invoice report into:
 *   ETL_MarioDemo.xlsx
 *   Sheet: "Membership invoices"
 */

require("dotenv").config();

const axios = require("axios");
const fs = require("fs-extra");
const XLSX = require("xlsx");
const path = require("path");

// --------------------------------------------------
// CONFIG
// --------------------------------------------------
const OUTPUT_DIR = path.join(__dirname, "output");
const DEST_FILE = path.join(OUTPUT_DIR, "ETL_MarioDemo.xlsx");
const DEST_SHEET_NAME = "Membership invoices";

const TOKEN_URL = "https://spaces.nexudus.com/api/token";
const REPORT_URL = "https://reports.nexudus.com/ReportCenter/Invoices";

// --------------------------------------------------
// Get Nexudus access token
// --------------------------------------------------
async function getNexudusToken() {
  const response = await axios.post(
    TOKEN_URL,
    new URLSearchParams({
      grant_type: "password",
      username: process.env.NEXUDUS_USERNAME,
      password: process.env.NEXUDUS_PASSWORD
    }).toString(),
    {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
      }
    }
  );

  if (!response.data || !response.data.access_token) {
    throw new Error("Failed to obtain Nexudus access token");
  }

  return response.data.access_token;
}

// --------------------------------------------------
// Download Nexudus Excel report
// --------------------------------------------------
async function downloadReport(token) {
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
      Authorization: `Bearer ${token}`
    }
  });

  return response.data;
}

// --------------------------------------------------
// Copy data into destination Excel
// --------------------------------------------------
async function writeToDestinationExcel(excelBuffer) {
  await fs.ensureDir(OUTPUT_DIR);

  // Load Nexudus workbook (source)
  const sourceWorkbook = XLSX.read(excelBuffer, { type: "buffer" });
  const sourceSheetName = sourceWorkbook.SheetNames[0];
  const sourceSheet = sourceWorkbook.Sheets[sourceSheetName];

  // Load or create destination workbook
  let destWorkbook;
  if (fs.existsSync(DEST_FILE)) {
    destWorkbook = XLSX.readFile(DEST_FILE);
  } else {
    destWorkbook = XLSX.utils.book_new();
  }

  // Remove destination sheet if it already exists
  if (destWorkbook.SheetNames.includes(DEST_SHEET_NAME)) {
    delete destWorkbook.Sheets[DEST_SHEET_NAME];
    destWorkbook.SheetNames = destWorkbook.SheetNames.filter(
      name => name !== DEST_SHEET_NAME
    );
  }

  // Append fresh data
  XLSX.utils.book_append_sheet(
    destWorkbook,
    sourceSheet,
    DEST_SHEET_NAME
  );

  XLSX.writeFile(destWorkbook, DEST_FILE);
}

// --------------------------------------------------
// Main
// --------------------------------------------------
(async function run() {
  try {
    console.log("Authenticating with Nexudus...");
    const token = await getNexudusToken();

    console.log("Downloading Nexudus invoice report...");
    const reportBuffer = await downloadReport(token);

    console.log("Updating ETL_MarioDemo.xlsx...");
    await writeToDestinationExcel(reportBuffer);

    console.log("✔ Data copied successfully:");
    console.log(`  File: ${DEST_FILE}`);
    console.log(`  Sheet: ${DEST_SHEET_NAME}`);
  } catch (err) {
    console.error("✖ Process failed:", err.message);
    process.exit(1);
  }
})();
