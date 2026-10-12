const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch({ args: ['--disable-web-security'] });
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  await page.goto('http://localhost:3015/login');
  const tok = process.env.TOK;
  await page.evaluate((t) => localStorage.setItem('auth_token', t), tok);
  await page.goto('http://localhost:3015/schemes', { waitUntil: 'networkidle' }).catch(()=>{});
  await page.waitForTimeout(5000);
  const info = await page.evaluate(() => {
    return [...document.querySelectorAll('h1')].map(h => ({
      text: h.textContent.trim().slice(0, 30), cls: h.className.slice(0, 120),
      bg: getComputedStyle(h).backgroundImage.slice(0, 140) }));
  });
  console.log(JSON.stringify(info, null, 1));
  await browser.close();
})();
