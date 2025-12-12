const fs = require('fs');
const https = require('https');
const zlib = require('zlib');
const readline = require('readline');
const { pipeline } = require('stream/promises');

const REDFIN_URL = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz';
const TEMP_FILE = 'temp_data.tsv';

function stripQuotes(str) {
  if (str && str.startsWith('"') && str.endsWith('"')) {
    return str.slice(1, -1);
  }
  return str;
}

async function downloadAndDecompress(url) {
  console.log('Downloading and decompressing...');
  
  return new Promise((resolve, reject) => {
    https.get(url, async (response) => {
      if (response.statusCode !== 200) {
        reject(new Error(`HTTP ${response.statusCode}`));
        return;
      }
      
      const gunzip = zlib.createGunzip();
      const output = fs.createWriteStream(TEMP_FILE);
      
      let downloaded = 0;
      response.on('data', (chunk) => {
        downloaded += chunk.length;
        if (downloaded % (50 * 1024 * 1024) < chunk.length) {
          console.log(`Downloaded ${Math.round(downloaded / 1024 / 1024)}MB compressed...`);
        }
      });
      
      try {
        await pipeline(response, gunzip, output);
        console.log('Download and decompression complete');
        resolve();
      } catch (err) {
        reject(err);
      }
    }).on('error', reject);
  });
}

async function parseTSV() {
  console.log('Parsing TSV...');
  
  const fileStream = fs.createReadStream(TEMP_FILE);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });
  
  let headers = null;
  let colIndex = {};
  const zipData = new Map();
  let processed = 0;
  
  for await (const line of rl) {
    if (!headers) {
      // Parse headers - strip quotes and convert to lowercase for matching
      const rawHeaders = line.split('\t');
      headers = rawHeaders.map(h => stripQuotes(h).toLowerCase());
      
      console.log('Cleaned headers sample:', headers.slice(0, 10));
      
      colIndex = {
        region: headers.indexOf('region'),
        region_type: headers.indexOf('region_type'),
        property_type: headers.indexOf('property_type'),
        period_end: headers.indexOf('period_end'),
        median_dom: headers.indexOf('median_dom'),
        median_ppsf: headers.indexOf('median_ppsf'),
        sold_above_list: headers.indexOf('sold_above_list')
      };
      console.log('Column indices:', colIndex);
      continue;
    }
    
    if (!line.trim()) continue;
    
    const cols = line.split('\t');
    processed++;
    
    // Strip quotes from values we're checking
    const regionType = stripQuotes(cols[colIndex.region_type]);
    const propertyType = stripQuotes(cols[colIndex.property_type]);
    
    if (regionType !== 'zip code') continue;
    if (propertyType !== 'All Residential') continue;
    
    const region = stripQuotes(cols[colIndex.region]);
    const periodEnd = stripQuotes(cols[colIndex.period_end]);
    
    // Extract just the zip code number from "Zip Code: 64119" format
    const zipMatch = region.match(/(\d{5})/);
    if (!zipMatch) continue;
    const zip = zipMatch[1];
    
    const existing = zipData.get(zip);
    if (existing && existing.period_end >= periodEnd) continue;
    
    const median_dom = parseFloat(cols[colIndex.median_dom]) || null;
    const median_ppsf = parseFloat(cols[colIndex.median_ppsf]) || null;
    const sold_above_list = parseFloat(cols[colIndex.sold_above_list]) || null;
    
    if (median_dom !== null || median_ppsf !== null || sold_above_list !== null) {
      zipData.set(zip, {
        period_end: periodEnd,
        median_dom,
        median_ppsf,
        sold_above_list
      });
    }
    
    if (processed % 1000000 === 0) {
      console.log(`Processed ${processed.toLocaleString()} rows, ${zipData.size} zips kept...`);
    }
  }
  
  console.log(`Processed ${processed.toLocaleString()} rows total`);
  console.log(`Unique zip codes: ${zipData.size.toLocaleString()}`);
  
  return zipData;
}

function convertToOutput(zipData) {
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
    await downloadAndDecompress(REDFIN_URL);
    
    const zipData = await parseTSV();
    const output = convertToOutput(zipData);
    
    fs.unlinkSync(TEMP_FILE);
    console.log('Temp file cleaned up');
    
    if (!fs.existsSync('output')) {
      fs.mkdirSync('output');
    }
    
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
