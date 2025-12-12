const fs = require('fs');
const https = require('https');
const zlib = require('zlib');

const REDFIN_URL = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz';

async function downloadFile(url) {
  return new Promise((resolve, reject) => {
    console.log('Downloading from Redfin...');
    
    https.get(url, (response) => {
      if (response.statusCode !== 200) {
        reject(new Error(`HTTP ${response.statusCode}`));
        return;
      }
      
      const chunks = [];
      let downloaded = 0;
      
      response.on('data', (chunk) => {
        chunks.push(chunk);
        downloaded += chunk.length;
        if (downloaded % (10 * 1024 * 1024) < chunk.length) {
          console.log(`Downloaded ${Math.round(downloaded / 1024 / 1024)}MB...`);
        }
      });
      
      response.on('end', () => {
        console.log(`Download complete: ${Math.round(downloaded / 1024 / 1024)}MB`);
        resolve(Buffer.concat(chunks));
      });
      
      response.on('error', reject);
    }).on('error', reject);
  });
}

function decompress(buffer) {
  console.log('Decompressing...');
  return zlib.gunzipSync(buffer);
}

function parseTSV(text) {
  console.log('Parsing TSV...');
  
  const lines = text.split('\n');
  console.log(`Total lines: ${lines.length.toLocaleString()}`);
  
  const headers = lines[0].split('\t');
  
  // Find column indices
  const colIndex = {
    region: headers.indexOf('region'),
    region_type: headers.indexOf('region_type'),
    property_type: headers.indexOf('property_type'),
    period_end: headers.indexOf('period_end'),
    median_dom: headers.indexOf('median_dom'),
    median_ppsf: headers.indexOf('median_ppsf'),
    sold_above_list: headers.indexOf('sold_above_list')
  };
  
  console.log('Column indices:', colIndex);
  
  // Process rows
  const zipData = new Map();
  let processed = 0;
  
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;
    
    const cols = line.split('\t');
    processed++;
    
    // Filter: only zip code region type
    if (cols[colIndex.region_type] !== 'zip code') continue;
    
    // Filter: only All Residential
    if (cols[colIndex.property_type] !== 'All Residential') continue;
    
    const zip = cols[colIndex.region];
    const periodEnd = cols[colIndex.period_end];
    
    // Keep only most recent period_end per zip
    const existing = zipData.get(zip);
    if (existing && existing.period_end >= periodEnd) continue;
    
    const median_dom = parseFloat(cols[colIndex.median_dom]) || null;
    const median_ppsf = parseFloat(cols[colIndex.median_ppsf]) || null;
    const sold_above_list = parseFloat(cols[colIndex.sold_above_list]) || null;
    
    // Only keep if we have at least some data
    if (median_dom !== null || median_ppsf !== null || sold_above_list !== null) {
      zipData.set(zip, {
        period_end: periodEnd,
        median_dom,
        median_ppsf,
        sold_above_list
      });
    }
    
    if (processed % 1000000 === 0) {
      console.log(`Processed ${processed.toLocaleString()} rows...`);
    }
  }
  
  console.log(`Processed ${processed.toLocaleString()} rows total`);
  console.log(`Unique zip codes: ${zipData.size.toLocaleString()}`);
  
  return zipData;
}

function convertToOutput(zipData) {
  // Convert to a compact format: { "12345": [median_dom, median_ppsf, sold_above_list], ... }
  // This reduces file size significantly vs full JSON objects
  const output = {
    generated: new Date().toISOString(),
    fields: ['median_dom', 'median_ppsf', 'sold_above_list'],
    data: {}
  };
  
  for (const [zip, data] of zipData) {
    output.data[zip] = [
      data.median_dom,
      data.median_ppsf,
      data.sold_above_list
    ];
  }
  
  return output;
}

async function main() {
  try {
    // Download
    const compressed = await downloadFile(REDFIN_URL);
    
    // Decompress
    const decompressed = decompress(compressed);
    console.log(`Decompressed size: ${Math.round(decompressed.length / 1024 / 1024)}MB`);
    
    // Parse
    const text = decompressed.toString('utf-8');
    const zipData = parseTSV(text);
    
    // Convert to output format
    const output = convertToOutput(zipData);
    
    // Create output directory
    if (!fs.existsSync('output')) {
      fs.mkdirSync('output');
    }
    
    // Write JSON file
    const jsonContent = JSON.stringify(output);
    fs.writeFileSync('output/market-data.json', jsonContent);
    console.log(`Output file size: ${Math.round(jsonContent.length / 1024)}KB`);
    
    console.log('Done!');
    
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

main();
