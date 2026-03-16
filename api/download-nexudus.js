const AWS = require('aws-sdk');

export default async (req, res) => {
  try {
    console.log('🚀 Starting Nexudus ETL Job');
    console.log(`⏰ Run time: ${new Date().toISOString()}`);
    
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

    // Calculate yesterday's date range
    const yesterday = new Date();
    yesterday.setUTCDate(yesterday.getUTCDate() - 1);
    
    const year = yesterday.getUTCFullYear();
    const month = String(yesterday.getUTCMonth() + 1).padStart(2, '0');
    const day = String(yesterday.getUTCDate()).padStart(2, '0');
    
    const start = `${year}-${month}-${day}T00:00:00`;
    const end = `${year}-${month}-${day}T23:59:59`;

    console.log(`📅 Fetching invoices from ${start} to ${end}`);

    // Construct URL with query params
    const url = new URL('https://reports.nexudus.com/ReportCenter/Invoices');
    url.searchParams.append('businessId', process.env.BUSINESS_ID);
    url.searchParams.append('reportName', 'Invoices/InvoicesAccount');
    url.searchParams.append('start', start);
    url.searchParams.append('end', end);
    url.searchParams.append('format', 'Excel');
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

    const filename = `ETL_MarioDemo_Nexudus_${timestamp}.xlsx`;

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

    return res.status(200).json({
      success: true,
      message: 'ETL job completed successfully',
      filename: filename,
      s3_path: `s3://${process.env.S3_BUCKET}/output/${filename}`
    });

  } catch (error) {
    console.error('');
    console.error('==================================================');
    console.error('❌ ETL Job Failed');
    console.error('==================================================');
    console.error(`⏰ Error time: ${new Date().toISOString()}`);
    console.error(`📌 Error: ${error.message}`);
    console.error('==================================================');

    return res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
};