/**
 * Nexudus Invoices → ETL Excel Copier → S3 Versioned Upload
 * ---------------------------------------------------------
 * Reads a template, replaces sheet with Nexudus data,
 * saves locally, then uploads to S3 with versioning.
 * 
 * Fetches the previous 30 days of invoice data.
 */

require("dotenv").config();

const axios = require("axios");
const fs = require("fs-extra");
const path = require("path");
const XLSX = require("xlsx");
const AWS = require("aws-sdk");

// --------------------------------------------------
// CONFIG
// --------------------------------------------------
const TEMPLATE_FILE = path.join(__dirname, "template/ETL_MarioDemo.xlsx");
const OUTPUT_DIR = path.join(__dirname, "output");
const OUTPUT_FILE = path.join(OUTPUT_DIR, "ETL_MarioDemo.xlsx");
const DEST_SHEET_NAME = "Membership invoices";

const NEXUDUS_REPORT_URL = "https://reports.nexudus.com/ReportCenter/Invoices";
const NEXUDUS_TOKEN_URL = "https://spaces.nexudus.com/api/token";

// S3
const S3_BUCKET = "level39-etl-mario";
const S3_KEY = "output/ETL_MarioDemo.xlsx";

AWS.config.update({
  region: process.env.AWS_REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
});
const s3 = new AWS.S3();

// --------------------------------------------------
// Calculate 30-day date range
// --------------------------------------------------
function getDateRange() {
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - 30);
  
  // Format as ISO string for Nexudus API
  return {
    start: startDate.toISOString().split('.')[0], // 2026-01-28T00:00:00
    end: endDate.toISOString().split('.')[0]      // 2026-01-28T23:59:59
  };
}

// --------------------------------------------------
// Get Nexudus Access Token
// --------------------------------------------------
async function getNexudusToken() {
  const response = await axios.post(
    NEXUDUS_TOKEN_URL,
    new URLSearchParams({
      grant_type: "password",
      username: process.env.NEXUDUS_USERNAME,
      password: process.env.NEXUDUS_PASSWORD
    }).toString(),
    {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Accept: "application/json"
      }
    }
  );

  if (!response.data?.access_token) {
    throw new Error("Failed to get Nexudus token");
  }

  return response.data.access_token;
}

// --------------------------------------------------
// Download Nexudus report
// --------------------------------------------------
async function downloadReport(token) {
  const { start, end } = getDateRange();
  
  console.log(`📅 Fetching invoices from ${start} to ${end}`);
  
  const response = await axios.get(NEXUDUS_REPORT_URL, {
    params: {
      businessId: process.env.BUSINESS_ID,
      reportName: "Invoices/InvoicesAccount",
      start: start,
      end: end,
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
// Write report to template & save locally
// --------------------------------------------------
async function writeToTemplate(excelBuffer) {
  await fs.ensureDir(OUTPUT_DIR);

  // Load Nexudus workbook
  const sourceWorkbook = XLSX.read(excelBuffer, { type: "buffer" });
  const sourceSheetName = sourceWorkbook.SheetNames[0];
  const sourceSheet = sourceWorkbook.Sheets[sourceSheetName];

  // Load template workbook
  if (!fs.existsSync(TEMPLATE_FILE)) {
    throw new Error(`Template file not found: ${TEMPLATE_FILE}`);
  }
  const destWorkbook = XLSX.readFile(TEMPLATE_FILE);

  // Remove existing sheet if exists
  if (destWorkbook.SheetNames.includes(DEST_SHEET_NAME)) {
    delete destWorkbook.Sheets[DEST_SHEET_NAME];
    destWorkbook.SheetNames = destWorkbook.SheetNames.filter(
      name => name !== DEST_SHEET_NAME
    );
  }

  // Append fresh sheet
  XLSX.utils.book_append_sheet(destWorkbook, sourceSheet, DEST_SHEET_NAME);

  // Save locally
  XLSX.writeFile(destWorkbook, OUTPUT_FILE);
  console.log("✔ Local ETL file updated:", OUTPUT_FILE);
}

// --------------------------------------------------
// Upload to S3 (versioned)
// --------------------------------------------------
async function uploadToS3() {
  const result = await s3.putObject({
    Bucket: S3_BUCKET,
    Key: S3_KEY,
    Body: fs.readFileSync(OUTPUT_FILE)
  }).promise();

  console.log("✔ File uploaded to S3:", `s3://${S3_BUCKET}/${S3_KEY}`);
  console.log("📦 VersionId:", result.VersionId);
}

// --------------------------------------------------
// Main
// --------------------------------------------------
(async function run() {
  try {
    console.log("=== 🚀 Starting ETL Job ===");
    console.log("⏰ Run time:", new Date().toISOString());
    
    console.log("🔑 Authenticating with Nexudus...");
    const token = await getNexudusToken();

    console.log("📥 Downloading Nexudus report...");
    const reportBuffer = await downloadReport(token);

    console.log("📝 Updating template Excel...");
    await writeToTemplate(reportBuffer);

    console.log("☁️  Uploading to S3...");
    await uploadToS3();

    console.log("✅ ETL completed successfully");
    console.log("============================");
  } catch (err) {
    console.error("❌ ETL failed:", err.message);
    console.error(err.stack);
    process.exit(1);
  }
})();