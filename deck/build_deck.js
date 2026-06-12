// UK BTR/PBSA BD Intelligence Platform — Business Value Deck (Condensed v2)
// Audience: Founders / BD leadership / Investment committee
// Style: light text, product screenshots front-and-centre.

const pptxgen = require("pptxgenjs");
const path = require("path");

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE"; // 13.3 x 7.5
pres.author = "BD Intelligence Platform";
pres.title = "UK BTR/PBSA BD Intelligence Platform";

// ---------- palette ----------
const BG_DARK = "0B1424";
const PANEL = "152135";
const PANEL_LIGHT = "1E2D45";
const ACCENT = "F2A65A";          // amber — primary brand
const ACCENT_HOT = "E5484D";      // red — distress / critical
const ACCENT_OK = "30A46C";       // green — wins
const ACCENT_BLUE = "5EB1FF";     // blue — info
const ACCENT_VIOLET = "B197D6";   // violet
const TEXT = "F2F4F8";
const TEXT_MUTED = "8AA0BE";
const LINE = "263247";

const FONT_HEAD = "Calibri";
const FONT_BODY = "Calibri";

const SHOTS = path.join(__dirname, "shots");

// ---------- helpers ----------
function bgFill(slide) {
  slide.background = { color: BG_DARK };
}

function slideHeader(slide, title, subtitle) {
  slide.addText(title, {
    x: 0.5, y: 0.35, w: 12.3, h: 0.6,
    fontSize: 30, bold: true, color: TEXT, fontFace: FONT_HEAD, margin: 0,
  });
  if (subtitle) {
    slide.addText(subtitle, {
      x: 0.5, y: 0.97, w: 12.3, h: 0.35,
      fontSize: 14, color: TEXT_MUTED, fontFace: FONT_BODY, margin: 0,
    });
  }
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 0.2, y: 0.4, w: 0.12, h: 0.55,
    fill: { color: ACCENT }, line: { color: ACCENT },
  });
}

function pageFooter(slide, pageNum, total) {
  slide.addShape(pres.shapes.LINE, {
    x: 0.5, y: 7.1, w: 12.3, h: 0,
    line: { color: LINE, width: 0.75 },
  });
  slide.addText("UK BTR/PBSA BD Intelligence Platform", {
    x: 0.5, y: 7.15, w: 6, h: 0.3,
    fontSize: 10, color: TEXT_MUTED, fontFace: FONT_BODY, margin: 0,
  });
  slide.addText(`${pageNum} / ${total}`, {
    x: 11.5, y: 7.15, w: 1.3, h: 0.3,
    fontSize: 10, color: TEXT_MUTED, fontFace: FONT_BODY, align: "right", margin: 0,
  });
}

function bigStat(slide, x, y, w, label, value, sub, accent) {
  slide.addText(label.toUpperCase(), {
    x, y, w, h: 0.3,
    fontSize: 11, color: TEXT_MUTED, fontFace: FONT_BODY, bold: true, charSpacing: 4, margin: 0,
  });
  slide.addText(value, {
    x, y: y + 0.32, w, h: 1.0,
    fontSize: 54, color: accent, fontFace: FONT_HEAD, bold: true, margin: 0,
  });
  if (sub) {
    slide.addText(sub, {
      x, y: y + 1.35, w, h: 0.3,
      fontSize: 11, color: TEXT, fontFace: FONT_BODY, margin: 0,
    });
  }
}

// Small caption above a screenshot so the audience knows what they're looking at
function shotFrame(slide, x, y, w, h, file, caption) {
  // shadow / frame
  slide.addShape(pres.shapes.RECTANGLE, {
    x: x - 0.03, y: y - 0.03, w: w + 0.06, h: h + 0.06,
    fill: { color: LINE }, line: { color: LINE },
  });
  slide.addImage({
    path: path.join(SHOTS, file),
    x, y, w, h,
  });
  if (caption) {
    slide.addText(caption, {
      x, y: y + h + 0.08, w, h: 0.3,
      fontSize: 10, color: TEXT_MUTED, fontFace: FONT_BODY, italic: true, align: "center", margin: 0,
    });
  }
}

// Tight value bullet (used inside use-case panels)
function tightBullet(slide, x, y, w, accent, text) {
  slide.addShape(pres.shapes.OVAL, {
    x, y: y + 0.13, w: 0.1, h: 0.1,
    fill: { color: accent }, line: { color: accent },
  });
  slide.addText(text, {
    x: x + 0.2, y, w: w - 0.2, h: 0.38,
    fontSize: 12, color: TEXT, fontFace: FONT_BODY, margin: 0,
  });
}

const TOTAL = 11;

// =====================================================================
// SLIDE 1 — Title + 3 anchor stats
// =====================================================================
{
  const s = pres.addSlide();
  s.background = { color: BG_DARK };

  s.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 0, w: 13.3, h: 0.25,
    fill: { color: ACCENT }, line: { color: ACCENT },
  });

  s.addText("UK BTR / PBSA", {
    x: 0.7, y: 1.2, w: 11, h: 0.5,
    fontSize: 20, color: ACCENT, fontFace: FONT_HEAD, bold: true, charSpacing: 8, margin: 0,
  });
  s.addText("BD Intelligence Platform", {
    x: 0.7, y: 1.7, w: 12, h: 1.1,
    fontSize: 54, color: TEXT, fontFace: FONT_HEAD, bold: true, margin: 0,
  });
  s.addText("One live source of BD truth for UK BTR & PBSA — every scheme, every planning application, every operator.", {
    x: 0.7, y: 2.95, w: 12, h: 0.7,
    fontSize: 16, color: TEXT_MUTED, fontFace: FONT_BODY, italic: true, margin: 0,
  });

  // 3 big stats
  bigStat(s, 0.7,  4.4, 4.0, "Schemes",            "27,767",  "operating BTR / PBSA", ACCENT);
  bigStat(s, 5.0,  4.4, 4.0, "Planning apps",      "1.35M",   "305 councils, daily refresh", ACCENT_OK);
  bigStat(s, 9.3,  4.4, 4.0, "BD-actionable now",  "332",     "operators showing distress", ACCENT_HOT);

  s.addShape(pres.shapes.LINE, {
    x: 0.7, y: 7.0, w: 11.9, h: 0,
    line: { color: LINE, width: 0.75 },
  });
  s.addText("Internal — BD Leadership Review", {
    x: 0.7, y: 7.1, w: 6, h: 0.3,
    fontSize: 10, color: TEXT_MUTED, fontFace: FONT_BODY, charSpacing: 4, margin: 0,
  });
  s.addText(`1 / ${TOTAL}`, {
    x: 11.5, y: 7.1, w: 1.3, h: 0.3,
    fontSize: 10, color: TEXT_MUTED, fontFace: FONT_BODY, align: "right", margin: 0,
  });
}

// =====================================================================
// SLIDE 2 — The problem (without / with)
// =====================================================================
{
  const s = pres.addSlide();
  bgFill(s);
  slideHeader(s, "The problem we solve", "From fragmented BD intel to one live source of truth");

  // WITHOUT panel
  const lx = 0.5, lw = 6.0;
  s.addShape(pres.shapes.RECTANGLE, {
    x: lx, y: 1.55, w: lw, h: 5.3,
    fill: { color: PANEL }, line: { color: LINE, width: 0.5 },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: lx, y: 1.55, w: 0.08, h: 5.3,
    fill: { color: ACCENT_HOT }, line: { color: ACCENT_HOT },
  });
  s.addText("WITHOUT THE TOOL", {
    x: lx + 0.3, y: 1.75, w: lw - 0.5, h: 0.4,
    fontSize: 12, color: ACCENT_HOT, fontFace: FONT_BODY, bold: true, charSpacing: 4, margin: 0,
  });
  const pains = [
    "Intel scattered across Knight Frank, JLL, BPF, press releases",
    "BTR pipeline surfaces weeks after planning permission",
    "No central view of which operator runs which scheme",
    "Distress signals only land after they're already public",
  ];
  let py = 2.45;
  for (const p of pains) {
    s.addText("✗", {
      x: lx + 0.3, y: py, w: 0.3, h: 0.5,
      fontSize: 18, color: ACCENT_HOT, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(p, {
      x: lx + 0.7, y: py + 0.04, w: lw - 0.9, h: 0.95,
      fontSize: 14, color: TEXT, fontFace: FONT_BODY, margin: 0,
    });
    py += 1.05;
  }

  // WITH panel
  const rx = 6.8, rw = 6.0;
  s.addShape(pres.shapes.RECTANGLE, {
    x: rx, y: 1.55, w: rw, h: 5.3,
    fill: { color: PANEL }, line: { color: LINE, width: 0.5 },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: rx, y: 1.55, w: 0.08, h: 5.3,
    fill: { color: ACCENT_OK }, line: { color: ACCENT_OK },
  });
  s.addText("WITH THE TOOL", {
    x: rx + 0.3, y: 1.75, w: rw - 0.5, h: 0.4,
    fontSize: 12, color: ACCENT_OK, fontFace: FONT_BODY, bold: true, charSpacing: 4, margin: 0,
  });
  const wins = [
    "27,767 operating schemes — operator, owner, units, contract, postcode",
    "1.35M planning apps refreshed daily, BTR-eligible flagged automatically",
    "Live distress score on every operator — switch opportunities surface first",
    "Filter, rank, deep-link — built for sales calls and pitch decks",
  ];
  let wy = 2.45;
  for (const w of wins) {
    s.addText("✓", {
      x: rx + 0.3, y: wy, w: 0.3, h: 0.5,
      fontSize: 18, color: ACCENT_OK, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(w, {
      x: rx + 0.7, y: wy + 0.04, w: rw - 0.9, h: 0.95,
      fontSize: 14, color: TEXT, fontFace: FONT_BODY, margin: 0,
    });
    wy += 1.05;
  }

  pageFooter(s, 2, TOTAL);
}

// =====================================================================
// SLIDE 3 — The product (Dashboard hero screenshot)
// =====================================================================
{
  const s = pres.addSlide();
  bgFill(s);
  slideHeader(s, "What the team sees on day one", "Live dashboard: pipeline, distress, schemes in one view");

  // Big screenshot — dashboard (1440x1080 = 4:3 aspect)
  shotFrame(s, 0.5, 1.55, 7.20, 5.40, "dashboard.png", null);

  // Right column — 5 short capability points
  const rx = 8.1, rw = 4.8;
  const points = [
    { c: ACCENT,        h: "Headline KPIs",          t: "1.35M apps · 332 distress · contracts expiring" },
    { c: ACCENT_OK,     h: "Pipeline velocity",      t: "6-stage funnel from Identified to Won" },
    { c: ACCENT_BLUE,   h: "Live intelligence feed", t: "New high-value apps surfaced minute-by-minute" },
    { c: ACCENT_HOT,    h: "Distress alerts",        t: "Operators flagged as their CH filings change" },
    { c: ACCENT_VIOLET, h: "Quick actions",          t: "Add opportunity, jump to scheme, export list" },
  ];
  let ry = 1.55;
  for (const p of points) {
    s.addShape(pres.shapes.RECTANGLE, {
      x: rx, y: ry, w: 0.05, h: 0.92,
      fill: { color: p.c }, line: { color: p.c },
    });
    s.addText(p.h, {
      x: rx + 0.15, y: ry, w: rw - 0.2, h: 0.34,
      fontSize: 13, color: TEXT, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(p.t, {
      x: rx + 0.15, y: ry + 0.34, w: rw - 0.2, h: 0.6,
      fontSize: 11, color: TEXT_MUTED, fontFace: FONT_BODY, margin: 0,
    });
    ry += 1.06;
  }

  pageFooter(s, 3, TOTAL);
}

// =====================================================================
// SLIDE 4 — Schemes data for BD (with screenshot)
// =====================================================================
{
  const s = pres.addSlide();
  bgFill(s);
  slideHeader(s, "Schemes data — how BD uses it",
    "27,767 BTR / PBSA schemes · operator · owner · units · rent · contract · distress");

  // Screenshot left — 4:3 aspect
  shotFrame(s, 0.5, 1.55, 6.20, 4.65, "schemes.png", null);

  // Use cases right
  const rx = 7.1, rw = 5.7;
  s.addText("WHAT BD DOES WITH IT", {
    x: rx, y: 1.55, w: rw, h: 0.35,
    fontSize: 11, color: ACCENT, fontFace: FONT_BODY, bold: true, charSpacing: 4, margin: 0,
  });
  const uses = [
    { c: ACCENT_BLUE, t: "Rent benchmarking & pricing defence" },
    { c: ACCENT_VIOLET, t: "Operator targeting — find every scheme in their book" },
    { c: ACCENT_HOT,  t: "Acquisition shortlist — 332 schemes in distress today" },
    { c: ACCENT_OK,   t: "Market sizing & white-space discovery by city" },
    { c: ACCENT,      t: "Track operator changes & contract expiries" },
  ];
  let uy = 2.05;
  for (const u of uses) {
    s.addShape(pres.shapes.OVAL, {
      x: rx, y: uy + 0.12, w: 0.18, h: 0.18,
      fill: { color: u.c }, line: { color: u.c },
    });
    s.addText(u.t, {
      x: rx + 0.3, y: uy, w: rw - 0.3, h: 0.45,
      fontSize: 13, color: TEXT, fontFace: FONT_BODY, margin: 0,
    });
    uy += 0.65;
  }

  // Bottom strip — mini stats
  const stripY = 6.40;
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: stripY, w: 12.3, h: 0.55,
    fill: { color: PANEL_LIGHT }, line: { color: LINE, width: 0.5 },
  });
  const miniStats = [
    { v: "27,767", l: "Schemes" },
    { v: "1,182",  l: "Rent records" },
    { v: "44",     l: "Operators linked" },
    { v: "332",    l: "Operators in distress" },
    { v: "59",     l: "Acquisition targets" },
  ];
  for (let i = 0; i < miniStats.length; i++) {
    const ix = 0.6 + i * 2.46;
    s.addText(miniStats[i].v, {
      x: ix, y: stripY + 0.06, w: 1.2, h: 0.32,
      fontSize: 18, color: ACCENT, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(miniStats[i].l, {
      x: ix + 1.2, y: stripY + 0.13, w: 1.1, h: 0.3,
      fontSize: 10, color: TEXT_MUTED, fontFace: FONT_BODY, margin: 0,
    });
  }

  pageFooter(s, 4, TOTAL);
}

// =====================================================================
// SLIDE 5 — Applications data for BD (with screenshot)
// =====================================================================
{
  const s = pres.addSlide();
  bgFill(s);
  slideHeader(s, "Applications data — how BD uses it",
    "1.35M planning applications · BTR opportunities surfaced before competitors find them");

  // Use cases LEFT
  const lx = 0.5, lw = 4.5;
  s.addText("WHAT BD DOES WITH IT", {
    x: lx, y: 1.55, w: lw, h: 0.35,
    fontSize: 11, color: ACCENT, fontFace: FONT_BODY, bold: true, charSpacing: 4, margin: 0,
  });
  const uses = [
    { c: ACCENT_OK,     t: "Early-stage BTR pipeline — before competitors notice" },
    { c: ACCENT_BLUE,   t: "Pitch the right developer at the right moment" },
    { c: ACCENT_VIOLET, t: "Track developer pipelines — Berkeley, Quintain, Moda…" },
    { c: ACCENT,        t: "Council relationships — see who's planning-friendly" },
    { c: ACCENT_HOT,    t: "BD-actionable filter — Pending / Pre-App / Submitted" },
  ];
  let uy = 2.05;
  for (const u of uses) {
    s.addShape(pres.shapes.OVAL, {
      x: lx, y: uy + 0.12, w: 0.18, h: 0.18,
      fill: { color: u.c }, line: { color: u.c },
    });
    s.addText(u.t, {
      x: lx + 0.3, y: uy, w: lw - 0.3, h: 0.45,
      fontSize: 13, color: TEXT, fontFace: FONT_BODY, margin: 0,
    });
    uy += 0.65;
  }

  // Screenshot RIGHT — 4:3 aspect
  shotFrame(s, 6.60, 1.55, 6.20, 4.65, "applications.png", null);

  // Bottom strip
  const stripY = 6.40;
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: stripY, w: 12.3, h: 0.55,
    fill: { color: PANEL_LIGHT }, line: { color: LINE, width: 0.5 },
  });
  const miniStats = [
    { v: "1.35M", l: "Applications" },
    { v: "305",   l: "Councils tracked" },
    { v: "1,373", l: "BTR-eligible" },
    { v: "479",   l: "Pending BD-actionable" },
    { v: "~1,000", l: "New every day" },
  ];
  for (let i = 0; i < miniStats.length; i++) {
    const ix = 0.6 + i * 2.46;
    s.addText(miniStats[i].v, {
      x: ix, y: stripY + 0.06, w: 1.2, h: 0.32,
      fontSize: 18, color: ACCENT, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(miniStats[i].l, {
      x: ix + 1.2, y: stripY + 0.13, w: 1.1, h: 0.3,
      fontSize: 10, color: TEXT_MUTED, fontFace: FONT_BODY, margin: 0,
    });
  }

  pageFooter(s, 5, TOTAL);
}

// =====================================================================
// SLIDE 6 — Distress / operator intelligence hub
// =====================================================================
{
  const s = pres.addSlide();
  bgFill(s);
  slideHeader(s, "Distress intelligence",
    "Who's wobbling, which schemes are exposed, where the BD play is");

  // Screenshot — arrears hub (4:3 aspect)
  shotFrame(s, 0.5, 1.55, 7.20, 5.40, "arrears.png", null);

  // Right column callouts
  const rx = 8.1, rw = 4.8;
  s.addText("OUTPUTS", {
    x: rx, y: 1.55, w: rw, h: 0.3,
    fontSize: 11, color: ACCENT, fontFace: FONT_BODY, bold: true, charSpacing: 4, margin: 0,
  });

  // Big stat callouts
  const callouts = [
    { v: "80",     l: "Critical (≥80)",         c: ACCENT_HOT },
    { v: "252",    l: "Distressed (60-79)",     c: ACCENT },
    { v: "17,649", l: "Scheme cohort scored",   c: ACCENT_OK },
    { v: "59",     l: "Acquisition targets",    c: ACCENT_BLUE },
  ];
  let cy = 1.95;
  for (const c of callouts) {
    s.addShape(pres.shapes.RECTANGLE, {
      x: rx, y: cy, w: rw, h: 1.10,
      fill: { color: PANEL }, line: { color: LINE, width: 0.5 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: rx, y: cy, w: 0.08, h: 1.10,
      fill: { color: c.c }, line: { color: c.c },
    });
    s.addText(c.v, {
      x: rx + 0.2, y: cy + 0.08, w: 1.6, h: 0.95,
      fontSize: 30, color: c.c, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(c.l, {
      x: rx + 1.8, y: cy + 0.34, w: rw - 1.9, h: 0.5,
      fontSize: 11, color: TEXT, fontFace: FONT_BODY, margin: 0,
    });
    cy += 1.22;
  }

  pageFooter(s, 6, TOTAL);
}

// =====================================================================
// SLIDE 7 — Coverage + honest gaps
// =====================================================================
{
  const s = pres.addSlide();
  bgFill(s);
  slideHeader(s, "Coverage & honest gaps",
    "What we have today, what's still thin, and the business impact");

  // Coverage row — 4 KPIs
  const ky = 1.55, kh = 1.10, kw = 2.95, gap = 0.18;
  const kpis = [
    { l: "UK COUNCILS",      v: "339",    s: "of 417 LPAs (81%)",     c: ACCENT },
    { l: "COUNCIL LINKAGE",  v: "99.97%", s: "of scheme records",     c: ACCENT_OK },
    { l: "SCHEMES SCORED",   v: "27,503", s: "every BD-cohort row",   c: ACCENT_BLUE },
    { l: "ARREARS SCORED",   v: "63.6%",  s: "of BD cohort",          c: ACCENT_HOT },
  ];
  for (let i = 0; i < 4; i++) {
    const x = 0.5 + i * (kw + gap);
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: ky, w: kw, h: kh,
      fill: { color: PANEL }, line: { color: LINE, width: 0.5 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: ky, w: kw, h: 0.06,
      fill: { color: kpis[i].c }, line: { color: kpis[i].c },
    });
    s.addText(kpis[i].l, {
      x: x + 0.2, y: ky + 0.15, w: kw - 0.4, h: 0.28,
      fontSize: 10, color: TEXT_MUTED, fontFace: FONT_BODY, bold: true, charSpacing: 3, margin: 0,
    });
    s.addText(kpis[i].v, {
      x: x + 0.2, y: ky + 0.42, w: kw - 0.4, h: 0.55,
      fontSize: 26, color: kpis[i].c, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(kpis[i].s, {
      x: x + 0.2, y: ky + 0.85, w: kw - 0.4, h: 0.22,
      fontSize: 10, color: TEXT, fontFace: FONT_BODY, margin: 0,
    });
  }

  // Gaps row (3 honest gaps)
  const gy = 3.05, gh = 3.65;
  s.addText("HONEST GAPS", {
    x: 0.5, y: 2.85, w: 12, h: 0.3,
    fontSize: 11, color: ACCENT, fontFace: FONT_BODY, bold: true, charSpacing: 4, margin: 0,
  });
  const gapsLayout = [
    {
      n: "01",
      title: "Planning data is metadata-thin",
      impact: "Mid-tier developer pipeline under-represented",
      desc: "~99% of council planning records lack unit counts and developer names. Top operators captured reliably; mid-tier weaker.",
      c: ACCENT_HOT,
    },
    {
      n: "02",
      title: "Operator linkage incomplete",
      impact: "Distress leaderboard understated — 5× upside on completion",
      desc: "1,468 schemes have no operator. 8,500 more have an operator named but no Companies House link, so distress can't flow through.",
      c: ACCENT,
    },
    {
      n: "03",
      title: "Live occupancy + CSAT not wired",
      impact: "Activating these doubles operator-switch ranking precision",
      desc: "BD score has 4 dimensions — 2 are live (contract urgency, distress); 2 (occupancy, Google Reviews) are baseline-defaulted.",
      c: ACCENT_BLUE,
    },
  ];
  const cw = (12.3 - 2 * 0.2) / 3;
  for (let i = 0; i < 3; i++) {
    const x = 0.5 + i * (cw + 0.2);
    const g = gapsLayout[i];
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: gy, w: cw, h: gh,
      fill: { color: PANEL }, line: { color: LINE, width: 0.5 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x, y: gy, w: 0.08, h: gh,
      fill: { color: g.c }, line: { color: g.c },
    });
    s.addText(g.n, {
      x: x + 0.25, y: gy + 0.2, w: 0.6, h: 0.4,
      fontSize: 18, color: g.c, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(g.title, {
      x: x + 0.25, y: gy + 0.65, w: cw - 0.4, h: 0.65,
      fontSize: 14, color: TEXT, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(g.desc, {
      x: x + 0.25, y: gy + 1.35, w: cw - 0.4, h: 1.4,
      fontSize: 11, color: TEXT_MUTED, fontFace: FONT_BODY, margin: 0,
    });
    s.addShape(pres.shapes.LINE, {
      x: x + 0.25, y: gy + 2.85, w: cw - 0.5, h: 0,
      line: { color: LINE, width: 0.5 },
    });
    s.addText("BUSINESS IMPACT", {
      x: x + 0.25, y: gy + 2.95, w: cw - 0.4, h: 0.25,
      fontSize: 9, color: ACCENT, fontFace: FONT_BODY, bold: true, charSpacing: 3, margin: 0,
    });
    s.addText(g.impact, {
      x: x + 0.25, y: gy + 3.20, w: cw - 0.4, h: 0.5,
      fontSize: 11, color: TEXT, fontFace: FONT_BODY, italic: true, margin: 0,
    });
  }

  pageFooter(s, 7, TOTAL);
}

// =====================================================================
// SLIDE 8 — Ownership intelligence (scheme → SPV → fund)
// =====================================================================
{
  const s = pres.addSlide();
  bgFill(s);
  slideHeader(s, "Ownership intelligence — scheme → SPV → fund",
    "Identify the PE owners, white-label SPVs and asset-management platforms behind every building");

  // -------- LEFT: 4-step ownership chain --------
  const lx = 0.5, lw = 5.7;
  s.addText("HOW WE UNMASK THE WHITE LABEL", {
    x: lx, y: 1.50, w: lw, h: 0.3,
    fontSize: 11, color: ACCENT, fontFace: FONT_BODY, bold: true, charSpacing: 4, margin: 0,
  });

  const chain = [
    { t: "Operating scheme",            d: "The brand on the building — already in platform: 27,767 schemes", src: "TODAY", c: ACCENT },
    { t: "Owning SPV",                  d: "Single-asset company named on the land title",                     src: "HM LAND REGISTRY CCOD / OCOD — £0", c: ACCENT_BLUE },
    { t: "Asset-management platform",   d: "Clusters of SPVs sharing a registered address + officers",         src: "COMPANIES HOUSE — £0", c: ACCENT_VIOLET },
    { t: "Ultimate owner / PE fund",    d: "Persons-with-significant-control chain + overseas entity register", src: "COMPANIES HOUSE PSC + ROE — £0", c: ACCENT_OK },
  ];
  let cyy = 1.90;
  for (let i = 0; i < chain.length; i++) {
    const st = chain[i];
    s.addShape(pres.shapes.RECTANGLE, {
      x: lx, y: cyy, w: lw, h: 0.96,
      fill: { color: PANEL }, line: { color: LINE, width: 0.5 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: lx, y: cyy, w: 0.08, h: 0.96,
      fill: { color: st.c }, line: { color: st.c },
    });
    s.addText(st.t, {
      x: lx + 0.25, y: cyy + 0.08, w: lw - 0.4, h: 0.32,
      fontSize: 14, color: TEXT, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(st.d, {
      x: lx + 0.25, y: cyy + 0.40, w: lw - 0.4, h: 0.30,
      fontSize: 10.5, color: TEXT_MUTED, fontFace: FONT_BODY, margin: 0,
    });
    s.addText(st.src, {
      x: lx + 0.25, y: cyy + 0.68, w: lw - 0.4, h: 0.24,
      fontSize: 9, color: st.c, fontFace: FONT_BODY, bold: true, charSpacing: 2, margin: 0,
    });
    if (i < chain.length - 1) {
      s.addText("▼", {
        x: lx + lw / 2 - 0.2, y: cyy + 0.96, w: 0.4, h: 0.22,
        fontSize: 10, color: TEXT_MUTED, fontFace: FONT_BODY, align: "center", margin: 0,
      });
    }
    cyy += 1.20;
  }

  // -------- RIGHT: target-city coverage table --------
  const rx = 6.6, rw = 6.2;
  s.addText("THE 23 TARGET CITIES — COVERAGE TODAY", {
    x: rx, y: 1.50, w: rw, h: 0.3,
    fontSize: 11, color: ACCENT, fontFace: FONT_BODY, bold: true, charSpacing: 4, margin: 0,
  });

  // header row
  const thY = 1.90;
  s.addShape(pres.shapes.RECTANGLE, {
    x: rx, y: thY, w: rw, h: 0.32,
    fill: { color: PANEL_LIGHT }, line: { color: LINE, width: 0.5 },
  });
  s.addText("TIER",     { x: rx + 0.15, y: thY + 0.04, w: 3.0, h: 0.26, fontSize: 9, color: TEXT_MUTED, fontFace: FONT_BODY, bold: true, charSpacing: 2, margin: 0 });
  s.addText("SCHEMES",  { x: rx + 3.2,  y: thY + 0.04, w: 1.0, h: 0.26, fontSize: 9, color: TEXT_MUTED, fontFace: FONT_BODY, bold: true, charSpacing: 2, align: "right", margin: 0 });
  s.addText("OPERATOR", { x: rx + 4.25, y: thY + 0.04, w: 0.9, h: 0.26, fontSize: 9, color: TEXT_MUTED, fontFace: FONT_BODY, bold: true, charSpacing: 2, align: "right", margin: 0 });
  s.addText("OWNER",    { x: rx + 5.2,  y: thY + 0.04, w: 0.85, h: 0.26, fontSize: 9, color: TEXT_MUTED, fontFace: FONT_BODY, bold: true, charSpacing: 2, align: "right", margin: 0 });

  const tiers = [
    { t: "Highest Opportunity",        cities: "London · Bristol · Edinburgh · Glasgow",                n: "9,309", op: "5%",  own: "96%", focus: false },
    { t: "Strong Institutional",       cities: "Manchester · Birmingham · Liverpool",                   n: "1,567", op: "15%", own: "96%", focus: false },
    { t: "Emerging — FOCUS",           cities: "Leeds · Cardiff · Exeter · S'oton · M'bro · Colchester", n: "1,013", op: "9%",  own: "95%", focus: true },
    { t: "Highest Potential — FOCUS",  cities: "10 secondary university cities",                        n: "576",   op: "5%",  own: "93%", focus: true },
  ];
  let trY = thY + 0.32;
  for (const tr of tiers) {
    s.addShape(pres.shapes.RECTANGLE, {
      x: rx, y: trY, w: rw, h: 0.74,
      fill: { color: PANEL }, line: { color: LINE, width: 0.5 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: rx, y: trY, w: 0.06, h: 0.74,
      fill: { color: tr.focus ? ACCENT : LINE }, line: { color: tr.focus ? ACCENT : LINE },
    });
    s.addText(tr.t, {
      x: rx + 0.15, y: trY + 0.08, w: 3.0, h: 0.3,
      fontSize: 12, color: tr.focus ? ACCENT : TEXT, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(tr.cities, {
      x: rx + 0.15, y: trY + 0.40, w: 3.0, h: 0.28,
      fontSize: 9, color: TEXT_MUTED, fontFace: FONT_BODY, margin: 0,
    });
    s.addText(tr.n,   { x: rx + 3.2,  y: trY + 0.20, w: 1.0,  h: 0.35, fontSize: 13, color: TEXT, fontFace: FONT_HEAD, bold: true, align: "right", margin: 0 });
    s.addText(tr.op,  { x: rx + 4.25, y: trY + 0.20, w: 0.9,  h: 0.35, fontSize: 13, color: ACCENT_HOT, fontFace: FONT_HEAD, bold: true, align: "right", margin: 0 });
    s.addText(tr.own, { x: rx + 5.2,  y: trY + 0.20, w: 0.85, h: 0.35, fontSize: 13, color: ACCENT_OK, fontFace: FONT_HEAD, bold: true, align: "right", margin: 0 });
    trY += 0.74;
  }

  // discovery gap callout
  const dgY = trY + 0.15;
  s.addShape(pres.shapes.RECTANGLE, {
    x: rx, y: dgY, w: rw, h: 0.95,
    fill: { color: PANEL }, line: { color: ACCENT_HOT, width: 1 },
  });
  s.addText("DISCOVERY GAP", {
    x: rx + 0.2, y: dgY + 0.10, w: rw - 0.4, h: 0.25,
    fontSize: 10, color: ACCENT_HOT, fontFace: FONT_BODY, bold: true, charSpacing: 3, margin: 0,
  });
  s.addText("Only 12 PBSA records exist across the 10 secondary university cities — real stock is 10–30 schemes per city. Find the stock first, then fill every field.", {
    x: rx + 0.2, y: dgY + 0.35, w: rw - 0.4, h: 0.55,
    fontSize: 10.5, color: TEXT, fontFace: FONT_BODY, margin: 0,
  });

  // honest ceiling note
  s.addText("Honest ceiling: offshore fund structures cap ownership resolution at ~80–90% — flagged in the UI, never silently missing.", {
    x: rx, y: dgY + 1.05, w: rw, h: 0.45,
    fontSize: 10, color: TEXT_MUTED, fontFace: FONT_BODY, italic: true, margin: 0,
  });

  pageFooter(s, 8, TOTAL);
}

// =====================================================================
// SLIDE 9 — Build plan, step by step (dev actions)
// =====================================================================
{
  const s = pres.addSlide();
  bgFill(s);
  slideHeader(s, "Build plan — step by step",
    "14 development steps across four phases · ~3 weeks · focus market first");

  const phases = [
    {
      t: "Discover the stock", tag: "WEEK 1 · £0 (DEV)", c: ACCENT,
      steps: [
        [1, "Build StuRents scraper — city → scheme, operator, rents"],
        [2, "Build AccommodationForStudents scraper (cross-check)"],
        [3, "Extend operator-site scrapers to ~15 PBSA brands"],
        [4, "Scrape university halls pages in 10 secondary cities"],
      ],
    },
    {
      t: "Fill operators & merge", tag: "WEEKS 1–2 · £500 AI CREDITS", c: ACCENT_OK,
      steps: [
        [5, "Dedupe + merge new schemes (existing pipeline)"],
        [6, "AI enrichment over every focus-city operator gap"],
        [7, "Manual curation residue + lock verified fields"],
      ],
    },
    {
      t: "Ownership pipeline", tag: "WEEKS 2–3 · £0 — FREE REGISTERS", c: ACCENT_BLUE,
      steps: [
        [8, "Ingest Land Registry CCOD / OCOD monthly files"],
        [9, "Match titles → schemes by postcode + address"],
        [10, "Walk Companies House PSC chains: SPV → ultimate owner"],
        [11, "Cluster SPVs by address + officers → platforms"],
      ],
    },
    {
      t: "Surface for BD", tag: "WEEK 3+ · INCLUDED", c: ACCENT_VIOLET,
      steps: [
        [12, "Ownership tab: scheme → SPV → platform → fund"],
        [13, "PE / asset-manager target-list view + export"],
        [14, "Contract-end AI estimation with confidence flags"],
      ],
    },
  ];

  const cw = 6.0, ch = 2.18, gx = 0.3, gy = 0.14, x0 = 0.5, y0 = 1.55;
  for (let i = 0; i < phases.length; i++) {
    const p = phases[i];
    const x = x0 + (i % 2) * (cw + gx);
    const y = y0 + Math.floor(i / 2) * (ch + gy);
    s.addShape(pres.shapes.RECTANGLE, {
      x, y, w: cw, h: ch,
      fill: { color: PANEL }, line: { color: LINE, width: 0.5 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x, y, w: 0.08, h: ch,
      fill: { color: p.c }, line: { color: p.c },
    });
    s.addText(p.t, {
      x: x + 0.25, y: y + 0.12, w: cw - 0.4, h: 0.34,
      fontSize: 15, color: TEXT, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(p.tag, {
      x: x + 0.25, y: y + 0.46, w: cw - 0.4, h: 0.24,
      fontSize: 9, color: p.c, fontFace: FONT_BODY, bold: true, charSpacing: 2, margin: 0,
    });
    let by = y + 0.78;
    for (const [num, txt] of p.steps) {
      s.addText(String(num), {
        x: x + 0.25, y: by, w: 0.4, h: 0.3,
        fontSize: 10.5, color: p.c, fontFace: FONT_HEAD, bold: true, margin: 0,
      });
      s.addText(txt, {
        x: x + 0.62, y: by, w: cw - 0.85, h: 0.3,
        fontSize: 10.5, color: TEXT, fontFace: FONT_BODY, margin: 0,
      });
      by += 0.33;
    }
  }

  // total investment band
  const tbY = y0 + 2 * ch + gy + 0.15;
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: tbY, w: 12.3, h: 0.58,
    fill: { color: PANEL }, line: { color: ACCENT, width: 1.25 },
  });
  s.addText("TOTAL INVESTMENT", {
    x: 0.7, y: tbY + 0.15, w: 2.4, h: 0.3,
    fontSize: 11, color: ACCENT, fontFace: FONT_BODY, bold: true, charSpacing: 3, margin: 0,
  });
  s.addText([
    { text: "£500 – £1,000 one-off", options: { bold: true, color: TEXT } },
    { text: " (AI credits)  ·  ", options: { color: TEXT_MUTED } },
    { text: "~3 weeks dev", options: { bold: true, color: TEXT } },
    { text: "  ·  ", options: { color: TEXT_MUTED } },
    { text: "£0 data cost", options: { bold: true, color: ACCENT_OK } },
    { text: " — every ownership source is a free public register", options: { color: TEXT_MUTED } },
  ], {
    x: 3.1, y: tbY + 0.13, w: 9.5, h: 0.35,
    fontSize: 12, fontFace: FONT_BODY, margin: 0,
  });

  pageFooter(s, 9, TOTAL);
}

// =====================================================================
// SLIDE 10 — What gets scraped (source list)
// =====================================================================
{
  const s = pres.addSlide();
  bgFill(s);
  slideHeader(s, "What gets scraped",
    "Five scrape targets to build or extend, plus three free public registers to integrate");

  const lx = 0.5, lw = 12.3, rowH = 0.48, pitch = 0.54;

  function sourceRow(y, name, yields, tag, tagColor) {
    s.addShape(pres.shapes.RECTANGLE, {
      x: lx, y, w: lw, h: rowH,
      fill: { color: PANEL }, line: { color: LINE, width: 0.5 },
    });
    s.addText(name, {
      x: lx + 0.2, y: y + 0.11, w: 3.3, h: 0.3,
      fontSize: 12, color: TEXT, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(yields, {
      x: lx + 3.6, y: y + 0.12, w: 6.6, h: 0.3,
      fontSize: 10.5, color: TEXT_MUTED, fontFace: FONT_BODY, margin: 0,
    });
    s.addText(tag, {
      x: lx + 10.3, y: y + 0.13, w: 1.85, h: 0.28,
      fontSize: 9, color: tagColor, fontFace: FONT_BODY, bold: true, charSpacing: 2, align: "right", margin: 0,
    });
  }

  s.addText("NEW SCRAPERS TO BUILD OR EXTEND", {
    x: lx, y: 1.50, w: lw, h: 0.3,
    fontSize: 11, color: ACCENT, fontFace: FONT_BODY, bold: true, charSpacing: 4, margin: 0,
  });
  const scrapers = [
    ["StuRents",                        "Every PBSA scheme listed per city — name, address, operator, rooms, rents", "BUILD · WK 1", ACCENT],
    ["AccommodationForStudents",        "Second PBSA directory — cross-validates schemes and fills gaps",            "BUILD · WK 1", ACCENT],
    ["Operator websites (~15 brands)",  "Unite, iQ, Fresh, Vita, Yugo, Homes for Students, CRM, Prestige, Collegiate…", "EXTEND · WK 1", ACCENT_BLUE],
    ["University halls pages",          "Partner halls + nomination stock across the 10 secondary cities",           "BUILD · WK 1", ACCENT],
    ["Rightmove / Zoopla",              "BTR + co-living listings and rent evidence in the focus cities",            "EXTEND · WK 2", ACCENT_BLUE],
  ];
  let sy = 1.86;
  for (const r of scrapers) { sourceRow(sy, r[0], r[1], r[2], r[3]); sy += pitch; }

  s.addText("FREE PUBLIC REGISTERS — API / BULK DOWNLOAD, NO SCRAPING NEEDED", {
    x: lx, y: sy + 0.08, w: lw, h: 0.3,
    fontSize: 11, color: ACCENT_OK, fontFace: FONT_BODY, bold: true, charSpacing: 4, margin: 0,
  });
  const registers = [
    ["HMLR CCOD + OCOD",               "Registered owner (SPV) of every UK title — company name + CH number, monthly", "INTEGRATE · WK 2", ACCENT_OK],
    ["Companies House API",            "PSC chains, officers, charges — already integrated for distress scoring",      "LIVE TODAY", ACCENT_OK],
    ["Register of Overseas Entities",  "Beneficial owners behind offshore vehicles holding UK property",               "INTEGRATE · WK 3", ACCENT_OK],
  ];
  let ry = sy + 0.44;
  for (const r of registers) { sourceRow(ry, r[0], r[1], r[2], r[3]); ry += pitch; }

  s.addText("All scraping is rate-limited and respects robots.txt · registers are licensed open data · scheme records dedupe through the existing merge pipeline.", {
    x: lx, y: ry + 0.04, w: lw, h: 0.25,
    fontSize: 9.5, color: TEXT_MUTED, fontFace: FONT_BODY, italic: true, margin: 0,
  });

  pageFooter(s, 10, TOTAL);
}

// =====================================================================
// SLIDE 11 — Platform investment + next steps
// =====================================================================
{
  const s = pres.addSlide();
  bgFill(s);
  slideHeader(s, "Platform investment & next steps",
    "Five targeted investments, sequenced into three phases");

  // Left: investments table (5 rows)
  const lx = 0.5, lw = 7.5, rowH = 0.85, headerH = 0.4;
  s.addText("WHAT TO FUND", {
    x: lx, y: 1.55, w: lw, h: 0.3,
    fontSize: 11, color: ACCENT, fontFace: FONT_BODY, bold: true, charSpacing: 4, margin: 0,
  });
  // Column headers
  const colY = 1.92;
  s.addShape(pres.shapes.RECTANGLE, {
    x: lx, y: colY, w: lw, h: headerH,
    fill: { color: PANEL_LIGHT }, line: { color: LINE, width: 0.5 },
  });
  s.addText("INVESTMENT",  { x: lx + 0.15, y: colY + 0.05, w: 4.1, h: 0.3, fontSize: 10, color: TEXT_MUTED, fontFace: FONT_BODY, bold: true, charSpacing: 3, margin: 0 });
  s.addText("COST",        { x: lx + 4.3,  y: colY + 0.05, w: 1.6, h: 0.3, fontSize: 10, color: TEXT_MUTED, fontFace: FONT_BODY, bold: true, charSpacing: 3, margin: 0 });
  s.addText("UNLOCKS",     { x: lx + 5.9,  y: colY + 0.05, w: 1.6, h: 0.3, fontSize: 10, color: TEXT_MUTED, fontFace: FONT_BODY, bold: true, charSpacing: 3, margin: 0 });

  const items = [
    { n: "AI enrichment top-up",          c: "£500 → £5k",     u: "Operator link on ~10–15k schemes",  col: ACCENT },
    { n: "Re-enable scheduled scrapers",  c: "£0",             u: "~1,000 new planning apps / day",     col: ACCENT_OK },
    { n: "Deeper council planning crawls",c: "£0 + 1 wk dev",  u: "BTR pipeline 290 → 1,500–3,000",     col: ACCENT_BLUE },
    { n: "Complete PBSA rent scraping",   c: "£50 / month",    u: "Full UK Tier-1 rent benchmarks",     col: ACCENT_VIOLET },
    { n: "Vacancy + CSAT signals",        c: "£200 / month",   u: "Activates 45% of BD score weight",   col: ACCENT_HOT },
  ];
  let iy = colY + headerH;
  for (const it of items) {
    s.addShape(pres.shapes.RECTANGLE, {
      x: lx, y: iy, w: lw, h: rowH,
      fill: { color: PANEL }, line: { color: LINE, width: 0.5 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: lx, y: iy, w: 0.06, h: rowH,
      fill: { color: it.col }, line: { color: it.col },
    });
    s.addText(it.n, {
      x: lx + 0.15, y: iy + 0.18, w: 4.1, h: 0.55,
      fontSize: 12, color: TEXT, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(it.c, {
      x: lx + 4.3, y: iy + 0.18, w: 1.6, h: 0.55,
      fontSize: 12, color: ACCENT, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(it.u, {
      x: lx + 5.9, y: iy + 0.18, w: 1.55, h: 0.55,
      fontSize: 11, color: TEXT_MUTED, fontFace: FONT_BODY, margin: 0,
    });
    iy += rowH;
  }

  // Right: 3 phases
  const rx = 8.3, rw = 4.5;
  s.addText("ROLL-OUT", {
    x: rx, y: 1.55, w: rw, h: 0.3,
    fontSize: 11, color: ACCENT, fontFace: FONT_BODY, bold: true, charSpacing: 4, margin: 0,
  });

  const phases = [
    { p: "Phase 1", w: "Week 1",       cost: "£500–1k",      title: "Quick wins",        body: "AI top-up, scrapers re-enabled, top-1k targets enriched", c: ACCENT_OK },
    { p: "Phase 2", w: "Weeks 2 – 4",  cost: "+2 wks dev",   title: "Coverage extension",body: "Deeper council crawls, full PBSA rent benchmark",        c: ACCENT_BLUE },
    { p: "Phase 3", w: "Month 2 – 3",  cost: "£250 / mo",    title: "Premium signals",   body: "CSAT + vacancy live → full 100% BD score",               c: ACCENT },
  ];
  let phy = 1.95;
  for (const p of phases) {
    s.addShape(pres.shapes.RECTANGLE, {
      x: rx, y: phy, w: rw, h: 1.55,
      fill: { color: PANEL }, line: { color: LINE, width: 0.5 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: rx, y: phy, w: 0.06, h: 1.55,
      fill: { color: p.c }, line: { color: p.c },
    });
    s.addText(p.p, {
      x: rx + 0.18, y: phy + 0.1, w: 1.4, h: 0.32,
      fontSize: 11, color: p.c, fontFace: FONT_BODY, bold: true, charSpacing: 3, margin: 0,
    });
    s.addText(p.w, {
      x: rx + 1.5, y: phy + 0.1, w: 1.6, h: 0.32,
      fontSize: 10, color: TEXT_MUTED, fontFace: FONT_BODY, margin: 0,
    });
    s.addText(p.cost, {
      x: rx + 3.0, y: phy + 0.1, w: 1.4, h: 0.32,
      fontSize: 10, color: ACCENT, fontFace: FONT_HEAD, bold: true, align: "right", margin: 0,
    });
    s.addText(p.title, {
      x: rx + 0.18, y: phy + 0.50, w: rw - 0.3, h: 0.36,
      fontSize: 14, color: TEXT, fontFace: FONT_HEAD, bold: true, margin: 0,
    });
    s.addText(p.body, {
      x: rx + 0.18, y: phy + 0.92, w: rw - 0.3, h: 0.6,
      fontSize: 11, color: TEXT_MUTED, fontFace: FONT_BODY, margin: 0,
    });
    phy += 1.65;
  }

  pageFooter(s, 11, TOTAL);
}

// ---------- write file ----------
pres.writeFile({ fileName: "bd_intelligence_platform.pptx" }).then(f => {
  console.log("Saved:", f);
});
