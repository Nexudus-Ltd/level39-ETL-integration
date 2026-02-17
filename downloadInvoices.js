/**
 * Nexudus Invoices → ETL Excel Copier → S3 Versioned Upload
 * ---------------------------------------------------------
 * Reads a template, replaces sheet with Nexudus data,
 * saves locally with timestamped filename, then uploads to S3 with versioning.
 * 
 * Creates a new file each run: ETL_MarioDemo_YYYY-MM-DD_HHmmss.xlsx
 * Fetches the previous day's invoice data.
 * Designed to run daily via GitHub Actions workflow.
 */
require("dotenv").config();
const axios = require("axios");
const fs = require("fs-extra");
const path = require("path");
const XLSX = require("xlsx");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");

// --------------------------------------------------
// CONFIG
// --------------------------------------------------
const TEMPLATE_FILE = path.join(__dirname, "template/ETL_MarioDemo.xlsx");
const OUTPUT_DIR = path.join(__dirname, "output");
const DEST_SHEET_NAME = "Membership invoices";
const NEXUDUS_REPORT_URL = "https://reports.nexudus.com/ReportCenter/Invoices";
const NEXUDUS_TOKEN_URL = "https://spaces.nexudus.com/api/token";

// S3
const S3_BUCKET = process.env.S3_BUCKET || "level39-etl-mario";
const S3_KEY_PREFIX = "output/"; // Files will be saved as: output/ETL_MarioDemo_YYYY-MM-DD_HHmmss.xlsx

const s3Client = new S3Client({
  region: process.env.AWS_REGION || "us-east-1"
});

// --------------------------------------------------
// Generate timestamped filename
// --------------------------------------------------
function generateOutputFilename() {
  const now = new Date();
  const year = now.getUTCFullYear();
  const month = String(now.getUTCMonth() + 1).padStart(2, "0");
  const day = String(now.getUTCDate()).padStart(2, "0");
  const hours = String(now.getUTCHours()).padStart(2, "0");
  const minutes = String(now.getUTCMinutes()).padStart(2, "0");
  const seconds = String(now.getUTCSeconds()).padStart(2, "0");
  
  const timestamp = `${year}-${month}-${day}_${hours}${minutes}${seconds}`;
  const filename = `ETL_MarioDemo_${timestamp}.xlsx`;
  
  return {
    local: path.join(OUTPUT_DIR, filename),
    s3: `${S3_KEY_PREFIX}${filename}`,
    displayName: filename
  };
}

// --------------------------------------------------
// Calculate yesterday's date range (00:00:00 - 23:59:59 UTC)
// --------------------------------------------------
function getDateRange() {
  const yesterday = new Date();
  yesterday.setUTCDate(yesterday.getUTCDate() - 1);

  const startDate = new Date(yesterday);
  startDate.setUTCHours(0, 0, 0, 0);

  const endDate = new Date(yesterday);
  endDate.setUTCHours(23, 59, 59, 999);

  return {
    start: startDate.toISOString().split(".")[0], // 2026-02-11T00:00:00
    end: endDate.toISOString().split(".")[0]      // 2026-02-11T23:59:59
  };
}

// --------------------------------------------------
// Get Nexudus Access Token
// --------------------------------------------------
async function getNexudusToken() {
  try {
    console.log("🔑 Requesting Nexudus authentication token...");
    
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
        },
        timeout: 10000
      }
    );

    if (!response.data?.access_token) {
      throw new Error("No access token received from Nexudus");
    }

    console.log("✔ Nexudus token obtained successfully");
    return response.data.access_token;
  } catch (err) {
    if (err.response?.status === 401) {
      throw new Error("Nexudus authentication failed - invalid credentials");
    } else if (err.code === "ECONNREFUSED" || err.code === "ETIMEDOUT") {
      throw new Error("Cannot reach Nexudus API - network issue or service unavailable");
    }
    throw new Error(`Nexudus token request failed: ${err.message}`);
  }
}

// --------------------------------------------------
// Download Nexudus report
// --------------------------------------------------
async function downloadReport(token) {
  try {
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
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      },
      timeout: 30000
    });

    if (!response.data || response.data.length === 0) {
      throw new Error("Nexudus report returned empty data");
    }

    console.log(`✔ Report downloaded successfully (${response.data.length} bytes)`);
    return response.data;
  } catch (err) {
    if (err.response?.status === 404) {
      throw new Error("Nexudus report endpoint not found - check API URL");
    } else if (err.response?.status === 403) {
      throw new Error("Access denied to Nexudus report - check business ID and permissions");
    } else if (err.code === "ETIMEDOUT") {
      throw new Error("Nexudus report request timed out");
    }
    throw new Error(`Failed to download Nexudus report: ${err.message}`);
  }
}

// --------------------------------------------------
// Write report to template & save locally
// --------------------------------------------------
async function writeToTemplate(excelBuffer, outputFile) {
  try {
    await fs.ensureDir(OUTPUT_DIR);
    
    console.log("📝 Processing Excel data...");

    // Load Nexudus workbook
    const sourceWorkbook = XLSX.read(excelBuffer, { type: "buffer" });
    
    if (!sourceWorkbook.SheetNames || sourceWorkbook.SheetNames.length === 0) {
      throw new Error("Nexudus workbook contains no sheets");
    }

    const sourceSheetName = sourceWorkbook.SheetNames[0];
    const sourceSheet = sourceWorkbook.Sheets[sourceSheetName];

    if (!sourceSheet) {
      throw new Error(`Cannot access sheet "${sourceSheetName}" in Nexudus workbook`);
    }

    console.log(`  ├─ Source sheet: "${sourceSheetName}" (${Object.keys(sourceSheet).length} cells)`);

    // Load template workbook
    if (!fs.existsSync(TEMPLATE_FILE)) {
      throw new Error(`Template file not found: ${TEMPLATE_FILE}`);
    }

    const destWorkbook = XLSX.readFile(TEMPLATE_FILE);
    console.log(`  ├─ Template loaded with sheets: ${destWorkbook.SheetNames.join(", ")}`);

    // Remove existing sheet if exists
    if (destWorkbook.SheetNames.includes(DEST_SHEET_NAME)) {
      console.log(`  ├─ Removing existing sheet: "${DEST_SHEET_NAME}"`);
      delete destWorkbook.Sheets[DEST_SHEET_NAME];
      destWorkbook.SheetNames = destWorkbook.SheetNames.filter(
        name => name !== DEST_SHEET_NAME
      );
    }

    // Append fresh sheet
    XLSX.utils.book_append_sheet(destWorkbook, sourceSheet, DEST_SHEET_NAME);
    console.log(`  └─ New sheet added: "${DEST_SHEET_NAME}"`);

    // Save locally with timestamped filename
    XLSX.writeFile(destWorkbook, outputFile);
    console.log(`✔ Local ETL file saved: ${outputFile}`);
  } catch (err) {
    throw new Error(`Failed to process Excel template: ${err.message}`);
  }
}

// --------------------------------------------------
// Upload to S3 (versioned)
// --------------------------------------------------
async function uploadToS3(localFile, s3Key, displayName) {
  try {
    console.log("☁️  Uploading to S3...");

    const fileContent = fs.readFileSync(localFile);

    const result = await s3Client.send(
      new PutObjectCommand({
        Bucket: S3_BUCKET,
        Key: s3Key,
        Body: fileContent
      })
    );

    console.log(`✔ File uploaded to S3: s3://${S3_BUCKET}/${s3Key}`);
    if (result.VersionId) {
      console.log(`📦 Version ID: ${result.VersionId}`);
    } else {
      console.log("⚠️  Note: S3 versioning may not be enabled on this bucket");
    }
  } catch (err) {
    if (err.code === "NoSuchBucket") {
      throw new Error(`S3 bucket not found: ${S3_BUCKET}`);
    } else if (err.code === "AccessDenied") {
      throw new Error("Access denied to S3 bucket - check AWS credentials and permissions");
    }
    throw new Error(`Failed to upload to S3: ${err.message}`);
  }
}

// --------------------------------------------------
// Main ETL Job
// --------------------------------------------------
async function runETL() {
  try {
    console.log("=".repeat(50));
    console.log("🚀 Starting Nexudus ETL Job");
    console.log("=".repeat(50));
    console.log(`⏰ Run time: ${new Date().toISOString()}`);
    console.log();

    // Generate timestamped output filenames
    const fileNames = generateOutputFilename();
    console.log(`📁 Output file: ${fileNames.displayName}`);
    console.log();

    // Validate environment variables
    const requiredEnvVars = [
      "NEXUDUS_USERNAME",
      "NEXUDUS_PASSWORD",
      "BUSINESS_ID",
      "AWS_ACCESS_KEY_ID",
      "AWS_SECRET_ACCESS_KEY"
    ];

    const missingEnvVars = requiredEnvVars.filter(envVar => !process.env[envVar]);
    if (missingEnvVars.length > 0) {
      throw new Error(`Missing required environment variables: ${missingEnvVars.join(", ")}`);
    }

    // Execute ETL pipeline
    const token = await getNexudusToken();
    const reportBuffer = await downloadReport(token);
    await writeToTemplate(reportBuffer, fileNames.local);
    await uploadToS3(fileNames.local, fileNames.s3, fileNames.displayName);

    console.log();
    console.log("=".repeat(50));
    console.log("✅ ETL Job Completed Successfully");
    console.log("=".repeat(50));
  } catch (err) {
    console.error();
    console.error("=".repeat(50));
    console.error("❌ ETL Job Failed");
    console.error("=".repeat(50));
    console.error(`⏰ Error time: ${new Date().toISOString()}`);
    console.error(`📌 Error: ${err.message}`);
    console.error("=".repeat(50));
    
    // Log full stack trace for debugging
    if (process.env.DEBUG === "true") {
      console.error("\nFull stack trace:");
      console.error(err.stack);
    }

    // Exit with error code for GitHub Actions to detect failure
    process.exit(1);
  }
}

// --------------------------------------------------
// Run ETL
// --------------------------------------------------
runETL();