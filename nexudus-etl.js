const AWS = require('aws-sdk');

async function runETL() {
  try {
    console.log('🚀 Starting Nexudus ETL Job');
    console.log(`⏰ Run time: ${new Date().toISOString()}`);
    console.log('');
    
    // Validate environment variables
    const requiredVars = [
      'NEXUDUS_USERNAME',
      'NEXUDUS_PASSWORD',
      'BUSINESS_ID',
      'AWS_ACCESS_KEY_ID',
      'AWS_SECRET_ACCESS_KEY',
      'AWS_REGION',
      'S3_BUCKET'
    ];
    
    const missingVars = requiredVars.filter(v => !process.env[v]);
    if (missingVars.length > 0) {
      throw new Error(`Missing environment variables: ${missingVars.join(', ')}`);
    }

    // Get Nexudus token
    console.log('🔑 Requesting Nexudus authentication token...');
    const tokenResponse = await fetch('https://spaces.nexudus.com/api/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
      },
      body: new URLSearchParams({
        grant_type: 'password',
        username: process.env.NEXUDUS_USERNAME,
        password: process.env.NEXUDUS_PASSWORD
      })
    });

    if (!tokenResponse.ok) {
      throw new Error(`Nexudus authentication failed: ${tokenResponse.status}`);
    }

    const tokenData = await tokenResponse.json();
    if (!tokenData.access_token) {
      throw new Error('No access token received from Nexudus');
    }
    console.log('✔ Nexudus token obtained successfully');
    console.log('');

    // Determine which report to run based on the current day
    const today = new Date();
    const dayOfMonth = today.getUTCDate();
    const dayOfWeek = today.getUTCDay(); // 0 = Sunday, 1 = Monday, etc.
    
    let start, end;
    
    // Check if today is the 1st of the month (run month-end report)
    if (dayOfMonth === 1) {
      // Run report for the remaining days of the previous week that are in the previous month
      // Get the last day of the previous month
      const lastDayPrevMonth = new Date(today);
      lastDayPrevMonth.setUTCDate(0); // Go to last day of previous month
      
      // Find the Monday of the week that contains the last day of previous month
      const lastDayOfWeek = lastDayPrevMonth.getUTCDay();
      const daysBackToMonday = (lastDayOfWeek === 0) ? 6 : (lastDayOfWeek - 1);
      
      const mondayOfLastWeek = new Date(lastDayPrevMonth);
      mondayOfLastWeek.setUTCDate(lastDayPrevMonth.getUTCDate() - daysBackToMonday);
      
      // Find the last Monday that was before the end of the month
      const lastMondayOfMonth = new Date(lastDayPrevMonth);
      lastMondayOfMonth.setUTCDate(lastDayPrevMonth.getUTCDate() - daysBackToMonday - 7);
      
      // Start from the day after last Monday of previous month (or from last Monday if it's in the same month)
      const startDate = new Date(lastMondayOfMonth);
      startDate.setUTCDate(lastMondayOfMonth.getUTCDate() + 7); // Add 7 days to get to the next Monday
      
      // If that Monday is in the current month, start from that Monday instead
      if (startDate.getUTCMonth() === today.getUTCMonth()) {
        // This shouldn't happen on the 1st, but handle it anyway
        startDate.setUTCDate(startDate.getUTCDate() - 7);
      }
      
      // End date is the last day of the previous month
      const endDate = lastDayPrevMonth;
      
      const startYear = startDate.getUTCFullYear();
      const startMonth = String(startDate.getUTCMonth() + 1).padStart(2, '0');
      const startDay = String(startDate.getUTCDate()).padStart(2, '0');
      
      const endYear = endDate.getUTCFullYear();
      const endMonth = String(endDate.getUTCMonth() + 1).padStart(2, '0');
      const endDay = String(endDate.getUTCDate()).padStart(2, '0');
      
      start = `${startYear}-${startMonth}-${startDay}T00:00:00`;
      end = `${endYear}-${endMonth}-${endDay}T23:59:59`;
      
      console.log(`📅 Month-end report: ${start.split('T')[0]} to ${end.split('T')[0]}`);
      
    } else if (dayOfWeek === 1) {
      // Today is Monday - run weekly report for previous week
      
      // Get last Monday (7 days ago)
      const startDate = new Date(today);
      startDate.setUTCDate(today.getUTCDate() - 7);
      
      // Get last Sunday (1 day ago)
      const endDate = new Date(today);
      endDate.setUTCDate(today.getUTCDate() - 1);
      
      const startYear = startDate.getUTCFullYear();
      const startMonth = String(startDate.getUTCMonth() + 1).padStart(2, '0');
      const startDay = String(startDate.getUTCDate()).padStart(2, '0');
      
      const endYear = endDate.getUTCFullYear();
      const endMonth = String(endDate.getUTCMonth() + 1).padStart(2, '0');
      const endDay = String(endDate.getUTCDate()).padStart(2, '0');
      
      start = `${startYear}-${startMonth}-${startDay}T00:00:00`;
      end = `${endYear}-${endMonth}-${endDay}T23:59:59`;
      
      console.log(`📅 Weekly report: ${start.split('T')[0]} to ${end.split('T')[0]}`);
      
    } else if (process.env.FORCE_RUN === 'true') {
      // Manual test run - use previous Monday's week
      const startDate = new Date(today);
      startDate.setUTCDate(today.getUTCDate() - dayOfWeek + (dayOfWeek === 0 ? -6 : 1)); // Get Monday of current week
      startDate.setUTCDate(startDate.getUTCDate() - 7); // Go back one week
      
      const endDate = new Date(startDate);
      endDate.setUTCDate(startDate.getUTCDate() + 6); // Get Sunday of that week
      
      const startYear = startDate.getUTCFullYear();
      const startMonth = String(startDate.getUTCMonth() + 1).padStart(2, '0');
      const startDay = String(startDate.getUTCDate()).padStart(2, '0');
      
      const endYear = endDate.getUTCFullYear();
      const endMonth = String(endDate.getUTCMonth() + 1).padStart(2, '0');
      const endDay = String(endDate.getUTCDate()).padStart(2, '0');
      
      start = `${startYear}-${startMonth}-${startDay}T00:00:00`;
      end = `${endYear}-${endMonth}-${endDay}T23:59:59`;
      
      console.log(`📅 Test run - weekly report: ${start.split('T')[0]} to ${end.split('T')[0]}`);
      
    } else {
      // Should not happen - workflow is configured to only run on Monday or 1st
      throw new Error(`Unexpected day: ${dayOfWeek} (${dayOfMonth}). Should only run on Mondays or the 1st of the month. (Use manual trigger to test)`);
    }

    // Construct URL with query params
    const url = new URL('https://reports.nexudus.com/ReportCenter/Invoices');
    url.searchParams.append('businessId', process.env.BUSINESS_ID);
    url.searchParams.append('reportName', 'Invoices/InvoicesAccount');
    url.searchParams.append('start', start);
    url.searchParams.append('end', end);
    url.searchParams.append('format', 'Csv');
    url.searchParams.append('portrait', 'false');

    const reportResponse = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${tokenData.access_token}`,
        'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
      }
    });

    if (!reportResponse.ok) {
      throw new Error(`Failed to download report: ${reportResponse.status}`);
    }

    const reportArrayBuffer = await reportResponse.arrayBuffer();
    const reportBuffer = Buffer.from(reportArrayBuffer);
    console.log(`✔ Report downloaded successfully (${reportBuffer.length} bytes)`);
    console.log('');

    // Initialize S3
    const s3 = new AWS.S3({
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      region: process.env.AWS_REGION
    });

    // Generate timestamped filename
    const now = new Date();
    const timestamp = now.toISOString()
      .split('T')[0] + '_' + 
      String(now.getUTCHours()).padStart(2, '0') +
      String(now.getUTCMinutes()).padStart(2, '0') +
      String(now.getUTCSeconds()).padStart(2, '0');

    const filename = `ETL_MarioDemo_Nexudus_${timestamp}.csv`;

    console.log('☁️  Uploading to S3...');
    const s3Params = {
      Bucket: process.env.S3_BUCKET,
      Key: `output/${filename}`,
      Body: reportBuffer,
      ContentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    };

    await s3.putObject(s3Params).promise();
    console.log(`✔ File uploaded to S3: s3://${process.env.S3_BUCKET}/output/${filename}`);
    console.log('');

    console.log('==================================================');
    console.log('✅ ETL Job Completed Successfully');
    console.log('==================================================');

  } catch (error) {
    console.error('');
    console.error('==================================================');
    console.error('❌ ETL Job Failed');
    console.error('==================================================');
    console.error(`⏰ Error time: ${new Date().toISOString()}`);
    console.error(`📌 Error: ${error.message}`);
    console.error('==================================================');
    process.exit(1);
  }
}

// Run the ETL job
runETL();