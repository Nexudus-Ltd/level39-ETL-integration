# Level39 ETL Integration

Automated daily ETL job that:
- Fetches Nexudus invoice data from the **previous 30 days**
- Updates an Excel template with the data
- Uploads the result to AWS S3 with versioning
- Runs automatically every day at **10:00 AM UTC**

## 🚀 Setup Instructions

### 1. Set Up GitHub Secrets

Go to your repository → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Add the following secrets:

| Secret Name | Description |
|-------------|-------------|
| `NEXUDUS_USERNAME` | Your Nexudus username |
| `NEXUDUS_PASSWORD` | Your Nexudus password |
| `BUSINESS_ID` | Your Nexudus business ID |
| `AWS_REGION` | AWS region (e.g., `us-east-1`) |
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |

### 2. Enable GitHub Actions

1. Go to the **Actions** tab in your repository
2. Enable workflows if prompted
3. The workflow will automatically run daily at 10:00 AM UTC

### 3. Manual Testing

To test the workflow before waiting for the scheduled run:

1. Go to **Actions** tab
2. Click on "Daily ETL Job - Nexudus Invoices"
3. Click **Run workflow** → **Run workflow**

### 4. Timezone Adjustment

The workflow runs at **10:00 AM UTC** by default. To change this:

Edit `.github/workflows/daily-etl.yml` and modify the cron expression:

```yaml
schedule:
  - cron: '0 10 * * *'  # Hour is in UTC (0-23)
```

**Common timezone conversions:**
- 10 AM UTC = 10 AM London (GMT)
- 10 AM UTC = 5 AM New York (EST)
- 10 AM UTC = 2 AM Los Angeles (PST)

To run at 10 AM London time (BST/GMT+1 in summer):
```yaml
  - cron: '0 9 * * *'  # 9 AM UTC = 10 AM BST
```

### 5. View Results

After each run:
1. Check the **Actions** tab for logs
2. Download the output file from **Artifacts** (kept for 30 days)
3. Check your S3 bucket: `s3://level39-etl-mario/output/ETL_MarioDemo.xlsx`

## 📁 Project Structure

```
.
├── .github/
│   └── workflows/
│       └── daily-etl.yml          # GitHub Actions workflow
├── template/
│   └── ETL_MarioDemo.xlsx         # Excel template
├── output/
│   └── ETL_MarioDemo.xlsx         # Generated output (local)
├── downloadInvoices.js            # Main ETL script (30-day dynamic range)
├── downloadInvoices_usingPibotfile.js
├── package.json
└── .env                           # Local environment variables (not used in GitHub Actions)
```

## 🔧 Local Development

To run locally:

1. Create a `.env` file with your credentials:
```env
NEXUDUS_USERNAME=your_username
NEXUDUS_PASSWORD=your_password
BUSINESS_ID=your_business_id
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
```

2. Install dependencies:
```bash
npm install
```

3. Run the script:
```bash
node downloadInvoices.js
```

## 📊 What the Script Does

1. **Calculates date range**: Automatically gets the last 30 days from today
2. **Authenticates**: Gets Nexudus access token
3. **Downloads report**: Fetches invoice data in Excel format
4. **Updates template**: Replaces the "Membership invoices" sheet
5. **Uploads to S3**: Saves to `s3://level39-etl-mario/output/ETL_MarioDemo.xlsx`

## 🐛 Troubleshooting

**Job not running?**
- Check if GitHub Actions is enabled
- Verify all secrets are set correctly
- Check the Actions tab for error messages

**Authentication failed?**
- Verify `NEXUDUS_USERNAME` and `NEXUDUS_PASSWORD` secrets
- Check if credentials work in Nexudus web interface

**S3 upload failed?**
- Verify AWS credentials
- Check if bucket exists and is in the correct region
- Ensure IAM user has `s3:PutObject` permission

**Template not found?**
- Ensure `template/ETL_MarioDemo.xlsx` exists in the repository
- The file must be committed to git

## 📝 Notes

- The workflow keeps artifacts for 30 days
- S3 versioning is enabled, so previous versions are preserved
- Failed runs will show in the Actions tab with error logs
- You can view/download past runs and their outputs
