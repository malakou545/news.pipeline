const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

const CAPTURES_DIR = path.join(__dirname, '..', 'captures');
if (!fs.existsSync(CAPTURES_DIR)) {
  fs.mkdirSync(CAPTURES_DIR);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function run() {
  console.log("Starting screenshot automation...");
  const browser = await puppeteer.launch({
    headless: "new",
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1920, height: 1080 });

  // ─────────────────────────────────────────────────────────────
  // 1. Spark Master UI
  // ─────────────────────────────────────────────────────────────
  try {
    console.log("Navigating to Spark Master UI...");
    await page.goto('http://localhost:8081', { waitUntil: 'networkidle2', timeout: 30000 });
    await sleep(2000);
    const filePath = path.join(CAPTURES_DIR, 'spark_master_ui.png');
    await page.screenshot({ path: filePath });
    console.log(`Saved Spark Master UI screenshot to: ${filePath}`);
  } catch (err) {
    console.error("Failed to capture Spark Master UI:", err.message);
  }

  // ─────────────────────────────────────────────────────────────
  // 2. MinIO Console (Data Lake)
  // ─────────────────────────────────────────────────────────────
  try {
    console.log("Navigating to MinIO Login...");
    await page.goto('http://localhost:9001', { waitUntil: 'networkidle2', timeout: 30000 });
    await sleep(2000);

    console.log("Logging into MinIO...");
    await page.type('input#accessKey', 'admin');
    await page.type('input#secretKey', 'password');
    await page.click('button[type="submit"]');
    
    await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 }).catch(() => {});
    await sleep(5000); // Wait for the MinIO bucket dashboard to load fully

    const filePath = path.join(CAPTURES_DIR, 'minio_buckets.png');
    await page.screenshot({ path: filePath });
    console.log(`Saved MinIO Console screenshot to: ${filePath}`);
  } catch (err) {
    console.error("Failed to capture MinIO Console:", err.message);
  }

  // ─────────────────────────────────────────────────────────────
  // 3. Airflow Web UI (Orchestrator)
  // ─────────────────────────────────────────────────────────────
  try {
    console.log("Navigating to Airflow Login...");
    await page.goto('http://localhost:8082/login/', { waitUntil: 'networkidle2', timeout: 30000 });
    await sleep(2000);

    console.log("Logging into Airflow...");
    await page.type('#username', 'admin');
    await page.type('#password', 'admin');
    await page.click('input[type="submit"]');

    await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 }).catch(() => {});
    await sleep(5000); // Wait for DAGs table to render fully

    const filePath = path.join(CAPTURES_DIR, 'airflow_dags.png');
    await page.screenshot({ path: filePath });
    console.log(`Saved Airflow UI screenshot to: ${filePath}`);
  } catch (err) {
    console.error("Failed to capture Airflow UI:", err.message);
  }

  // ─────────────────────────────────────────────────────────────
  // 4. Metabase UI (Visualization)
  // ─────────────────────────────────────────────────────────────
  try {
    console.log("Navigating to Metabase Setup screen...");
    await page.goto('http://localhost:3001', { waitUntil: 'networkidle2', timeout: 45000 });
    await sleep(5000);

    const filePath = path.join(CAPTURES_DIR, 'metabase_setup.png');
    await page.screenshot({ path: filePath });
    console.log(`Saved Metabase setup screenshot to: ${filePath}`);
  } catch (err) {
    console.error("Failed to capture Metabase setup:", err.message);
  }

  await browser.close();
  console.log("Screenshot automation completed!");
}

run().catch(console.error);
