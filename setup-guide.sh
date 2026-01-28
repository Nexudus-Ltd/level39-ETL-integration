#!/bin/bash

# Quick Setup Script for Daily ETL Automation
# ============================================

echo "📋 Setting up automated daily ETL job..."
echo ""

# 1. Create the .github/workflows directory
echo "1️⃣ Creating GitHub Actions workflow directory..."
mkdir -p .github/workflows

# 2. Copy the files (you'll need to download these from Claude first)
echo "2️⃣ Files you need to add:"
echo "   - .github/workflows/daily-etl.yml (the scheduler)"
echo "   - downloadInvoices.js (updated with 30-day range)"
echo "   - README.md (setup instructions)"
echo ""

# 3. Verify template exists
echo "3️⃣ Checking for template file..."
if [ -f "template/ETL_MarioDemo.xlsx" ]; then
    echo "   ✓ Template found"
else
    echo "   ⚠️  Warning: template/ETL_MarioDemo.xlsx not found!"
    echo "   Make sure to add it before running the job"
fi
echo ""

# 4. Git commands
echo "4️⃣ Git commands to run:"
echo ""
echo "   git add .github/workflows/daily-etl.yml"
echo "   git add downloadInvoices.js"
echo "   git add README.md"
echo "   git add template/ETL_MarioDemo.xlsx  # if not already added"
echo "   git commit -m 'Add automated daily ETL job with 30-day date range'"
echo "   git push origin main"
echo ""

# 5. Next steps
echo "5️⃣ Next steps after pushing:"
echo ""
echo "   a) Go to: https://github.com/marionexudus/level39-ETL-integration/settings/secrets/actions"
echo "   b) Add these secrets:"
echo "      - NEXUDUS_USERNAME"
echo "      - NEXUDUS_PASSWORD"
echo "      - BUSINESS_ID"
echo "      - AWS_REGION"
echo "      - AWS_ACCESS_KEY_ID"
echo "      - AWS_SECRET_ACCESS_KEY"
echo ""
echo "   c) Test the workflow:"
echo "      - Go to Actions tab"
echo "      - Click 'Daily ETL Job - Nexudus Invoices'"
echo "      - Click 'Run workflow'"
echo ""

echo "✅ Setup guide complete!"
echo ""
echo "The job will run automatically every day at 10:00 AM UTC"
echo "It will fetch the previous 30 days of invoice data"
