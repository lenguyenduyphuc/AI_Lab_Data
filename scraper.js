import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import cron from 'node-cron';
import fs from 'fs/promises';
import path from 'path';

export const KEYWORDS = [
  'human trafficking',
  'cyber abuse',
  'online exploitation',
  'child trafficking',
  'sexual exploitation',
  'forced labor',
  'digital trafficking',
  'online predators',
  'cyberbullying',
  'domestic abuse online',
  'violence'
];

export const ARTICLE_URLS = [
  'https://www.fbi.gov/investigate/violent-crime/human-trafficking',
  'https://www.cdc.gov/youth-violence/about/about-school-violence.html',
  'https://pmc.ncbi.nlm.nih.gov/articles/PMC9929464/'
];

puppeteer.use(StealthPlugin());
const BROWSER_CONFIG = {
  headless: 'new',
  args: [
    '--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
    '--disable-accelerated-2d-canvas', '--disable-gpu', '--window-size=1920,1080'
  ]
};

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15'
];

const pick = arr => arr[Math.floor(Math.random() * arr.length)];
const delay = ms => new Promise(r => setTimeout(r, ms));

async function ensureDir(dir) {
  try { await fs.access(dir); } catch { await fs.mkdir(dir, { recursive: true }); }
}

function splitSentences(text) {
  return text
    .replace(/[\r\n]+/g, ' ')
    .replace(/\s+/g, ' ')
    .split(/(?<=[.!?;:])\s+/)
    .map(s => s.trim())
    .filter(Boolean);
}

function extractKeywordSentences(text, keywords, aggregator) {
  const sentences = splitSentences(text);
  sentences.forEach(sentence => {
    const lower = sentence.toLowerCase();
    keywords.forEach(k => {
      if (lower.includes(k)) {
        const obj = aggregator[k];
        obj.count += 1;
        if (!obj.sentences.has(sentence)) obj.sentences.add(sentence);
      }
    });
  });
}

async function scrapeArticle(page, url) {
  await page.setViewport({ width: 1920, height: 1080 });
  await page.setUserAgent(pick(USER_AGENTS));
  await page.setRequestInterception(true);
  page.on('request', req => {
    const t = req.resourceType();
    if (['image', 'font', 'media'].includes(t)) req.abort(); else req.continue();
  });

  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 90000 });
  await delay(1000 + Math.random()*1000);

  const title = await page.$$eval(['.heading-title','h1','title'].join(','), els =>
    els.length ? els[0].innerText.trim() : document.title);

  const body = await page.$$eval('p', ps => ps.map(p=>p.innerText).join(' '));
  return `${title}. ${body}`;
}

async function runArticleScrapes() {
  await ensureDir('./data');
  const aggregator = Object.fromEntries(KEYWORDS.map(k => [k, { count: 0, sentences: new Set() }]));

  const browser = await puppeteer.launch(BROWSER_CONFIG);
  try {
    for (const url of ARTICLE_URLS) {
      const page = await browser.newPage();
      console.log(`\n📄  Processing: ${url}`);

      const fullText = await scrapeArticle(page, url);
      extractKeywordSentences(fullText, KEYWORDS, aggregator);

      await page.close();
      await delay(2500 + Math.random()*2000);
    }
  } finally {
    await browser.close();
  }

  const output = {};
  for (const k of KEYWORDS) {
    output[k] = {
      totalHits: aggregator[k].count,
      sentences: Array.from(aggregator[k].sentences).map(s => s.length>300?`${s.slice(0,297)}…`:s)
    };
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g,'-');
  const outPath = path.join('./data', `data-${timestamp}.json`);
  await fs.writeFile(outPath, JSON.stringify(output, null, 2));
  console.log(`\nAggregated data saved to: ${outPath}`);
  return { outPath };
}

export const handler = async () => {
  const { outPath } = await runArticleScrapes();
  return { statusCode: 200, body: JSON.stringify({ file: outPath }) };
};


cron.schedule('0 56 11 * * *', () => {
    console.log('Automated job started at 11:56 AM CST');
    runArticleScrapes().catch(err => console.error('Job error', err));
  },
  {
    timezone: 'America/Chicago' 
  }
);

runArticleScrapes().catch(err => {
  console.error('Immediate run failed', err);
  process.exit(1);
});
