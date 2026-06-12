// Capture product UI screenshots for the BD deck.
// Logs in via injected JWT (minted by backend) then snaps each page.

const { chromium } = require('playwright');
const fs = require('fs');

const TOKEN = process.env.AUTH_TOKEN;
if (!TOKEN) {
  console.error('AUTH_TOKEN env var required');
  process.exit(1);
}

const BASE = 'http://localhost:3010';

const PAGES = [
  { name: 'dashboard', path: '/dashboard', wait: 2000 },
  { name: 'schemes',   path: '/schemes',   wait: 3000 },
  { name: 'applications', path: '/applications', wait: 3000 },
  { name: 'arrears',   path: '/arrears',   wait: 3000 },
  { name: 'companies', path: '/companies', wait: 7000 },
  { name: 'pipeline',  path: '/pipeline',  wait: 2000 },
];

(async () => {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    viewport: { width: 1440, height: 1080 },
    deviceScaleFactor: 2, // crisp screenshots
  });
  const page = await ctx.newPage();

  // Bootstrap auth: load any page first to set origin, then inject token
  await page.goto(BASE + '/login', { waitUntil: 'domcontentloaded' });
  await page.evaluate((tok) => {
    localStorage.setItem('auth_token', tok);
  }, TOKEN);

  for (const p of PAGES) {
    console.log(`-> ${p.path}`);
    try {
      await page.goto(BASE + p.path, { waitUntil: 'networkidle', timeout: 20000 });
    } catch (e) {
      console.log(`   networkidle timed out, falling back to load: ${e.message}`);
      await page.goto(BASE + p.path, { waitUntil: 'load', timeout: 20000 });
    }
    await page.waitForTimeout(p.wait);
    const out = `shots/${p.name}.png`;
    await page.screenshot({ path: out, fullPage: false });
    console.log(`   saved ${out}`);
  }

  await browser.close();
  console.log('Done.');
})().catch(err => {
  console.error(err);
  process.exit(1);
});
