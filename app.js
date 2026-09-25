(function(){
'use strict';

/* ------------------------------------------------------------------ */
/* State                                                               */
/* ------------------------------------------------------------------ */
const COLS = ['settings','positions','watchlist','transactions','fundamentals','history','closed','alertState'];
const S = {}; COLS.forEach(c => S[c] = {});
const CFG = window.TL_CONFIG || {};
const cloudReady = !!(CFG.supabaseUrl && CFG.supabaseAnonKey && window.supabase);
let mode = 'boot';                  // 'boot' | 'auth' | 'cloud' | 'local'
let sb = null, user = null, rtChannel = null;
let synced = false, lastSync = null;
let installEvt = null;
let G = null;                       // last computed model
const ui = Object.assign({display:null, alloc:5000, wsort:'closest', collapsed:{}, histFilter:'', inbox:'active'}, loadUI());
const ai = {};
const auth = {step:'email', email:'', busy:false, msg:''};

function loadUI(){ try { return JSON.parse(localStorage.getItem('tl-ui')||'{}'); } catch(e){ return {}; } }
function saveUI(){ try { localStorage.setItem('tl-ui', JSON.stringify({display:ui.display, wsort:ui.wsort, collapsed:ui.collapsed})); } catch(e){} }
const GUEST_KEY = 'thesis-ledger-v1';
const cacheKey = () => mode === 'cloud' ? 'tl-cache-' + user.id : GUEST_KEY;
const outboxKey = () => 'tl-outbox-' + (user ? user.id : 'guest');
function readJSON(k, dflt){ try { const v = JSON.parse(localStorage.getItem(k) || 'null'); return v == null ? dflt : v; } catch(e){ return dflt; } }
function loadCache(){ const d = readJSON(cacheKey(), {}); COLS.forEach(c => S[c] = d[c] || {}); }
function saveCache(){ try { localStorage.setItem(cacheKey(), JSON.stringify(S)); } catch(e){ if (mode === 'local') toast('This device would not save the change. Free up browser storage.'); } }
function clearState(){ COLS.forEach(c => S[c] = {}); }

const clean = x => JSON.parse(JSON.stringify(x, (k,v) => (typeof v === 'number' && !isFinite(v)) ? null : v));
const newId = () => Date.now().toString(36) + Math.random().toString(36).slice(2,8);
const safeId = s => String(s||'').toUpperCase().trim().replace(/[^A-Z0-9._~:@+-]/g,'_').replace(/^\.+$/,'_');

/* Writes land in memory and the device cache first, then go to the cloud
   through an outbox, so the app keeps working offline. */
let outbox = [], flushing = false;
function loadOutbox(){ outbox = readJSON(outboxKey(), []); }
function saveOutbox(){ try { localStorage.setItem(outboxKey(), JSON.stringify(outbox)); } catch(e){} }
function enqueue(op){ outbox = outbox.filter(o => !(o.col === op.col && o.id === op.id)); outbox.push(op); saveOutbox(); flush(); }
async function flush(){
  if (mode !== 'cloud' || flushing || !outbox.length) return;
  flushing = true;
  try {
    while (outbox.length && mode === 'cloud') {
      const op = outbox[0];
      const r = op.del
        ? await sb.from('docs').delete().match({user_id:user.id, col:op.col, id:op.id})
        : await sb.from('docs').upsert({user_id:user.id, col:op.col, id:op.id, data:op.data, updated_at:new Date().toISOString()});
      if (r.error) { if (!/fetch|network/i.test(r.error.message || '')) { console.error(r.error); toast('Sync error: ' + r.error.message); } break; }
      outbox.shift(); saveOutbox(); lastSync = new Date();
    }
  } catch (e) { /* offline: retried on reconnect */ }
  finally { flushing = false; schedule(); }
}
const store = {
  async set(col, id, data){
    const isNewSymbol = (col === 'positions' || col === 'watchlist') && !S[col][id];
    data = clean(data); S[col][id] = data; saveCache(); schedule();
    if (isNewSymbol && mode === 'cloud') setTimeout(() => pollQuotes(true), 1500);
    if (mode === 'cloud') enqueue({col, id, data});
  },
  async del(col, id){
    delete S[col][id]; saveCache(); schedule();
    if (mode === 'cloud') enqueue({col, id, del:true});
  },
  async add(col, data){ const id = newId(); await this.set(col, id, data); return id; }
};

async function pull(){
  const rows = [], page = 1000;
  for (let from = 0; ; from += page) {
    const {data, error} = await sb.from('docs').select('col,id,data').range(from, from + page - 1);
    if (error) throw error;
    rows.push(...data); if (data.length < page) break;
  }
  const fresh = {}; COLS.forEach(c => fresh[c] = {});
  rows.forEach(r => { if (fresh[r.col]) fresh[r.col][r.id] = r.data; });
  outbox.forEach(op => { if (!fresh[op.col]) return; if (op.del) delete fresh[op.col][op.id]; else fresh[op.col][op.id] = op.data; });
  COLS.forEach(c => S[c] = fresh[c]);
  synced = true; lastSync = new Date(); saveCache(); schedule();
  ui.offerImport = !rows.length && hasGuestData();
}
function hasGuestData(){ const d = readJSON(GUEST_KEY, {}); return COLS.some(c => d[c] && Object.keys(d[c]).length); }
async function importGuest(){
  const d = readJSON(GUEST_KEY, {}); let n = 0;
  for (const c of COLS) for (const [id, doc] of Object.entries(d[c] || {})) { await store.set(c, id, doc); n++; }
  ui.offerImport = false; toast(n + ' records moved into your account');
}
function subscribe(){
  if (rtChannel) sb.removeChannel(rtChannel);
  const apply = p => {
    if (p.eventType === 'DELETE') { const o = p.old || {}; if (o.user_id && o.user_id !== user.id) return; if (S[o.col]) delete S[o.col][o.id]; }
    else { const n = p.new || {}; if (n.user_id !== user.id || !S[n.col]) return; if (outbox.some(op => op.col === n.col && op.id === n.id)) return; S[n.col][n.id] = n.data; }
    saveCache(); schedule();
  };
  rtChannel = sb.channel('docs-' + user.id)
    .on('postgres_changes', {event:'INSERT', schema:'public', table:'docs', filter:'user_id=eq.' + user.id}, apply)
    .on('postgres_changes', {event:'UPDATE', schema:'public', table:'docs', filter:'user_id=eq.' + user.id}, apply)
    .on('postgres_changes', {event:'DELETE', schema:'public', table:'docs'}, apply)
    .subscribe();
}
function startCloud(u){
  if (user && user.id === u.id && mode === 'cloud') return;
  user = {id:u.id, email:u.email}; mode = 'cloud'; synced = false;
  loadOutbox(); loadCache(); render();
  pull().catch(() => toast('Offline. Showing the copy saved on this device.')).then(flush).then(() => loadMarket()).then(() => pollQuotes(true)).catch(() => {});
  subscribe();
}
function startLocal(){ user = null; mode = 'local'; loadCache(); synced = true; render(); }
async function signOut(){
  if (outbox.length && !confirmFlag('signout')) { toast(outbox.length + ' changes have not synced yet. Go online first, or press Sign out again to discard them.'); return; }
  if (mode === 'cloud') { try { localStorage.removeItem(cacheKey()); localStorage.removeItem(outboxKey()); } catch(e){} if (rtChannel) sb.removeChannel(rtChannel); await sb.auth.signOut(); }
  try { localStorage.removeItem('tl-guest'); } catch(e){}
  user = null; clearState(); mode = cloudReady ? 'auth' : 'local'; closeModal(); location.hash = 'home'; render();
}
const flags = {};
function confirmFlag(k){ if (flags[k] && Date.now() - flags[k] < 6000) { delete flags[k]; return true; } flags[k] = Date.now(); return false; }

/* Server functions: the Claude analyst, live quotes and account deletion. */
const aiOn = () => mode === 'cloud' && CFG.ai !== false;
async function callFn(name, body){
  const {data, error} = await sb.functions.invoke(name, {body});
  if (error) {
    let code = 'upstream_error';
    try { const j = await error.context.json(); code = j.code || code; } catch(e){ if (/Failed to send|fetch/i.test(error.message || '')) code = 'offline'; }
    throw {code};
  }
  return data;
}
function parseJSONReply(t){
  try { return JSON.parse(t); } catch(e){}
  const m = t.match(/```(?:json)?\s*([\s\S]*?)```/); if (m) { try { return JSON.parse(m[1]); } catch(e){} }
  const a = t.indexOf('{'), b = t.lastIndexOf('}'); if (a >= 0 && b > a) { try { return JSON.parse(t.slice(a, b + 1)); } catch(e){} }
  throw {code:'invalid_json'};
}

/* ------------------------------------------------------------------ */
/* Formatting                                                          */
/* ------------------------------------------------------------------ */
const $ = (s, r=document) => r.querySelector(s);
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num = x => (x === '' || x == null || x === false || x === true) ? null : (isFinite(+x) ? +x : null);
const isNum = x => typeof x === 'number' && isFinite(x);
const sum = a => a.reduce((s,x) => s + (isNum(x) ? x : 0), 0);
const fmtN = (v, d=2) => isNum(v) ? v.toLocaleString('en-US', {minimumFractionDigits:d, maximumFractionDigits:d}) : '—';
const usd = (v, d) => isNum(v) ? (v < 0 ? '-$' : '$') + fmtN(Math.abs(v), d ?? (Math.abs(v) >= 1000 ? 0 : 2)) : '—';
const px = (v, cur) => cur === 'SAR' ? (isNum(v) ? 'SAR ' + fmtN(v, 2) : '—') : usd(v, 2);
const pct = (v, d=1, sign=true) => isNum(v) ? ((sign && v > 0) ? '+' : '') + v.toFixed(d) + '%' : '—';
const cls = v => isNum(v) ? (v > 0 ? 'pos' : v < 0 ? 'neg' : '') : '';
const MON = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
function todayISO(){ const d = new Date(); return d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0'); }
function pd(iso){ if(!iso) return null; const [y,m,d] = String(iso).slice(0,10).split('-').map(Number); return (y && m) ? Date.UTC(y, m-1, d||1) : null; }
function daysBetween(a, b){ const x = pd(a), y = pd(b); return (x == null || y == null) ? null : Math.round((y - x) / 86400000); }
function fmtDate(iso){ const t = pd(iso); if (t == null) return '—'; const d = new Date(t); return d.getUTCDate() + ' ' + MON[d.getUTCMonth()] + ' ' + d.getUTCFullYear(); }
function fmtMonth(iso){ const t = pd(iso); if (t == null) return '—'; const d = new Date(t); return MON[d.getUTCMonth()] + ' ' + d.getUTCFullYear(); }
function heldFor(days){ if (!isNum(days)) return '—'; if (days < 60) return days + ' days'; const mo = days / 30.44; return mo < 24 ? Math.round(mo) + ' months' : (mo/12).toFixed(1) + ' years'; }
function money(sar, signed){
  if (!isNum(sar)) return '—';
  const usdMode = (ui.display || (G && G.set.display) || 'SAR') === 'USD';
  const fx = G ? G.fx : 3.75;
  const v = usdMode ? sar / fx : sar;
  const s = signed && v > 0 ? '+' : v < 0 ? '-' : '';
  return s + (usdMode ? '$' : 'SAR ') + fmtN(Math.abs(v), 0);
}

/* ------------------------------------------------------------------ */
/* Vocabulary                                                          */
/* ------------------------------------------------------------------ */
const CLASSES = ['CORE COMPOUNDER','UNDERVALUED QUALITY','SECULAR GROWTH','ASYMMETRIC OPPORTUNITY','TURNAROUND','EVENT-DRIVEN','SPECULATIVE','THEMATIC','MOMENTUM','OTHER'];
const THESIS = ['STRENGTHENING','INTACT','WEAKENING','BROKEN','COMPLETED'];
const CONV = {HIGH:1, MEDIUM:.75, LOW:.5};
const CATS = {PRICE:'Price / valuation level', VALUATION:'Fundamental value', COMPLETION:'Thesis completion', FAILURE:'Thesis failure', SIZE:'Position size', OPPORTUNITY:'Better opportunity', TIME:'Time', LIQUIDITY:'Personal liquidity', ADD:'Add / buy more'};
const CAT_TITLE = {PRICE:'REVIEW PRICE REACHED', VALUATION:'VALUATION CONDITION MET', COMPLETION:'THESIS COMPLETION', FAILURE:'THESIS BREAK CONDITION', SIZE:'POSITION TOO LARGE', OPPORTUNITY:'EXPECTED RETURN BELOW MINIMUM', TIME:'SCHEDULED THESIS REVIEW DUE', LIQUIDITY:'LIQUIDITY NEED'};
const MISTAKES = ['CHASED PRICE','AVERAGED DOWN WITHOUT THESIS','POSITION TOO LARGE','POSITION TOO SMALL','SOLD WINNER TOO EARLY','HELD BROKEN THESIS','IGNORED VALUATION','IGNORED DILUTION','OVERREACTED TO NEWS','FOMO','ANCHORING TO PURCHASE PRICE','OVER-DIVERSIFICATION','DUPLICATE EXPOSURE'];
const ACTIONS = ['HOLD','ADD','STOP ADDING','TRIM 25%','TRIM 50%','EXIT','IMMEDIATE THESIS REVIEW'];
const STATUS_TONE = {'ADD':'g','ACCUMULATE':'g','HOLD':'n','WATCH':'y','REVIEW':'o','THESIS BREAK':'r','TARGET REACHED':'b'};
const THESIS_TONE = {STRENGTHENING:'g', INTACT:'n', WEAKENING:'o', BROKEN:'r', COMPLETED:'b'};
const ACTION_TONE = {'HOLD':'n','ADD':'g','STOP ADDING':'y','TRIM 25%':'o','TRIM 50%':'o','EXIT':'r','IMMEDIATE THESIS REVIEW':'r','DEFINE THESIS':'y'};
const SEV_TONE = {'THESIS BREAK':'r','ACTION REQUIRED':'r','IMPORTANT':'o','WATCH':'y','INFO':'n'};
const SEV_RANK = {'THESIS BREAK':0,'ACTION REQUIRED':1,'IMPORTANT':2,'WATCH':3,'INFO':4};
const WSTATUS_TONE = {'WAIT':'n','APPROACHING BUY':'y','BUY PRICE REACHED':'g','STRONG BUY REACHED':'g','MISSED ENTRY':'b','NO BUY PRICE':'n'};
const VER = {
  A:{tone:'g', lines:['PRICE TARGET REACHED','THESIS INTACT'], verdict:'BUY CONDITION SATISFIED', plines:['PRICE CONDITION MET','FUNDAMENTALS INTACT'], pverdict:'ACCUMULATION CONDITION SATISFIED'},
  B:{tone:'g', lines:['PRICE TARGET REACHED','THESIS STRONGER'], verdict:'HIGH-CONVICTION BUY REVIEW', plines:['PRICE CONDITION MET','THESIS STRONGER'], pverdict:'HIGH-CONVICTION ADD REVIEW'},
  C:{tone:'o', lines:['PRICE TARGET REACHED','BUT THESIS WEAKENED'], verdict:'INVESTIGATE BEFORE BUYING', plines:['PRICE CONDITION MET','BUT THESIS HAS DETERIORATED'], pverdict:'DO NOT AUTOMATICALLY ADD'},
  D:{tone:'r', lines:['PRICE TARGET REACHED','THESIS BROKEN'], verdict:'DO NOT BUY', plines:['PRICE CONDITION MET','THESIS BROKEN'], pverdict:'DO NOT ADD'}
};
const METRICS = {
  price:['Price','$'], weight:['Portfolio weight','%'], cagr3y:['Expected 3Y CAGR','%'], priceToFair:['Price vs model fair value','chg'],
  revGrowth:['Revenue growth (YoY)','%'], grossMargin:['Gross margin','%'], fwdFcfMult:['Forward FCF multiple','x'], fwdPE:['Forward P/E','x'],
  revEstChg:['Revenue estimates vs purchase','chg'], epsEstChg:['EPS estimates vs purchase','chg'], analystTargetChg:['Analyst consensus vs purchase','chg'],
  backlogChg:['Backlog vs purchase','chg'], dilution:['Share count vs purchase','chg'], monthsHeld:['Months held','mo'], daysToReview:['Days to scheduled review','d'],
  guidanceCut:['Guidance cut (1 = yes)','flag'], customerLoss:['Major customer lost (1 = yes)','flag']
};
const EXTRA_LABELS = {shares:'shares outstanding', high52:'52-week high', downgrades:'analyst downgrades'};
const OPS = {'<':'below', '<=':'at or below', '>':'above', '>=':'at or above'};
const DEFAULTS = {fx:3.75, display:'SAR', cash:0, contribMin:3000, contribMax:5000, contribDay:1, hurdle:15, minCagr:8, maxWeight:20, approachPct:5, benchmark:'S&P 500', notify:{inapp:true, push:false, email:false}};
function cfg(){ const s = S.settings.main || {}; return Object.assign({}, DEFAULTS, s, {notify:Object.assign({}, DEFAULTS.notify, s.notify||{})}); }

function metricLabel(k){ if (k && k.startsWith('custom:')) return k.slice(7); if (EXTRA_LABELS[k]) return EXTRA_LABELS[k]; return (METRICS[k] || [k||'—'])[0]; }
function fmtMetric(k, v){
  if (!isNum(v)) return '—';
  if (k && k.startsWith('custom:')) return fmtN(v, Math.abs(v) < 100 ? 2 : 0).replace(/\.00$/,'');
  const u = (METRICS[k] || [,''])[1];
  if (u === '$') return usd(v);
  if (u === 'chg') return pct(v, 1);
  if (u === '%') return v.toFixed(1) + '%';
  if (u === 'x') return v.toFixed(1) + 'x';
  if (u === 'mo') return Math.round(v) + ' mo';
  if (u === 'd') return Math.round(v) + ' days';
  if (u === 'flag') return v ? 'Yes' : 'No';
  return String(v);
}
function condThreshold(c){ return (OPS[c.op] || c.op) + ' ' + fmtMetric(c.metric, num(c.value)); }
function condText(c){
  let s = metricLabel(c.metric) + ' ' + condThreshold(c);
  if (c.and && c.and.metric) s += ' AND ' + metricLabel(c.and.metric) + ' ' + condThreshold(c.and);
  return s;
}

/* ------------------------------------------------------------------ */
/* Live market data overlay                                            */
/* ------------------------------------------------------------------ */
/* Quotes and company data live in shared server tables (MK), never in the
   user's documents. A manual price entered after the last quote wins. */
const MK = {quotes:{}, market:{}, lastPoll:null, error:null};
function livePrice(t, doc, fallback){
  const q = MK.quotes[t], manualTs = num(doc && doc.priceTs) || 0;
  if (q && Date.parse(q.updated_at) > manualTs) return {price:+q.price, prev:q.prev_close != null ? +q.prev_close : null, live:true, at:q.updated_at};
  return {price:num(doc && doc.price) ?? fallback ?? null, prev:num(doc && doc.prevClose), live:false, at: manualTs ? new Date(manualTs).toISOString() : (doc && doc.priceAt) || null};
}
const AUTO_FIELDS = ['revGrowth','grossMargin','high52','shares','downgrades'];
function fundOf(t){
  const u = S.fundamentals[t] || {}, a = (MK.market[t] || {}).data;
  if (!a) return u;
  const f = Object.assign({}, u), uTs = pd(u.asOf) || 0, aTs = Date.parse(a.asOf) || 0, auto = [];
  AUTO_FIELDS.forEach(k => { if (a[k] != null && (u[k] == null || aTs > uTs + 86400000)) { f[k] = a[k]; auto.push(k); } });
  f.auto = {fields:auto, asOf:a.asOf, company:a.company, industry:a.industry, peTTM:a.peTTM, low52:a.low52, nextEarnings:a.nextEarnings, analystBuyPct:a.analystBuyPct, analystBuyPct3m:a.analystBuyPct3m, news:a.news || [], exchange:a.exchange};
  return f;
}
function catalystOf(doc, f){
  const nc = doc && doc.nextCatalyst, today = todayISO();
  if (nc && nc.label && (!nc.date || nc.date >= today)) return nc;
  const e = f && f.auto && f.auto.nextEarnings;
  return e && e >= today ? {label:'Earnings (reported date)', date:e, auto:true} : (nc && nc.label ? nc : null);
}
function newsOf(f){
  const own = (f.notes || []).map(n => ({date:n.date, headline:n.text, own:true}));
  const auto = ((f.auto && f.auto.news) || []).map(n => ({date:n.date, headline:n.headline, url:n.url, source:n.source}));
  return [...own, ...auto].sort((a, b) => (b.date || '').localeCompare(a.date || ''));
}

/* ------------------------------------------------------------------ */
/* Engine                                                              */
/* ------------------------------------------------------------------ */
function lotsOf(list){
  const L = {shares:0, cost:0, realized:0, buyShares:0, buyAmt:0, sellShares:0, sellAmt:0, firstBuy:null, cycleStart:null, lastSell:null, lastPrice:null, cur:'USD', txs:[]};
  list.slice().sort((a,b) => (a.date||'').localeCompare(b.date||'') || ((a.side==='SELL') - (b.side==='SELL'))).forEach(t => {
    const sh = +t.shares || 0, pr = +t.price || 0, fee = +t.fees || 0;
    L.cur = t.currency || L.cur; L.lastPrice = pr; L.txs.push(t);
    if (t.side === 'SELL') {
      const avg = L.shares > 0 ? L.cost / L.shares : 0, q = Math.min(sh, L.shares);
      L.cost -= avg * q; L.shares -= q; L.realized += q * pr - fee - avg * q; L.sellShares += q; L.sellAmt += q * pr; L.lastSell = t.date;
      if (L.shares < 1e-9) { L.shares = 0; L.cost = 0; }
    } else {
      if (L.shares === 0) L.cycleStart = t.date;
      L.cost += sh * pr + fee; L.shares += sh; L.buyShares += sh; L.buyAmt += sh * pr + fee;
      if (!L.firstBuy) L.firstBuy = t.date;
    }
  });
  L.avg = L.shares > 0 ? L.cost / L.shares : null;
  return L;
}
function chg(a, b){ a = num(a); b = num(b); return (a != null && b != null && b !== 0) ? (a / b - 1) * 100 * (b < 0 ? -1 : 1) : null; }
function deltas(base, f){
  base = base || {}; f = f || {};
  return {revEstChg:chg(f.revEst, base.revEst), epsEstChg:chg(f.epsEst, base.epsEst), analystTargetChg:chg(f.analystTarget, base.analystTarget),
    backlogChg:chg(f.backlog, base.backlog), dilution:(() => { const c = chg(f.shares, base.shares); return c != null && Math.abs(c) > 60 ? null : c; })(), debtChg:(num(base.netDebt) > 0 ? chg(f.netDebt, base.netDebt) : null)};
}
function metricValue(m, k){
  const f = m.f || {};
  switch (k) {
    case 'price': return m.price;
    case 'weight': return m.weight;
    case 'cagr3y': return m.cagr3y;
    case 'priceToFair': return m.priceToFair;
    case 'revGrowth': case 'grossMargin': case 'fwdFcfMult': case 'fwdPE': return num(f[k]);
    case 'revEstChg': case 'epsEstChg': case 'analystTargetChg': case 'backlogChg': case 'dilution': return m.d ? m.d[k] : null;
    case 'monthsHeld': return m.months;
    case 'daysToReview': return (m.plan && m.plan.nextReview) ? daysBetween(todayISO(), m.plan.nextReview) : null;
    case 'guidanceCut': return f.guidance ? (f.guidance === 'Cut' ? 1 : 0) : null;
    case 'customerLoss': return typeof f.customerLoss === 'boolean' ? (f.customerLoss ? 1 : 0) : null;
  }
  if (k && k.startsWith('custom:')) return num((f.custom || {})[k.slice(7)]);
  return null;
}
function cmp(v, op, x){
  if (v == null || x == null) return null;
  switch (op) { case '<': return v < x; case '<=': return v <= x; case '>': return v > x; case '>=': return v >= x; case '==': return v === x; }
  return null;
}
function isNear(v, x, op, k, pctNear){
  if (v == null || x == null) return false;
  if (k === 'daysToReview') return v > 0 && v <= 14;
  if ((METRICS[k]||[])[1] === 'flag') return false;
  const rel = Math.abs(v - x) / Math.max(Math.abs(x), 1e-9) * 100;
  return rel <= pctNear && (op.startsWith('<') ? v > x : v < x);
}
function evalCond(m, c, ctx){
  const x = num(c.value), v = metricValue(m, c.metric);
  let ok = cmp(v, c.op, x), v2 = null, ok2 = null;
  if (c.and && c.and.metric) {
    v2 = metricValue(m, c.and.metric); ok2 = cmp(v2, c.and.op, num(c.and.value));
    ok = (ok === false || ok2 === false) ? false : (ok === null || ok2 === null) ? null : true;
  }
  const primaryFalse = cmp(v, c.op, x) === false;
  const near = primaryFalse && isNear(v, x, c.op, c.metric, ctx.approach) && ok2 !== false;
  const dist = (v != null && x != null && v !== 0) ? (v - x) / Math.abs(v) * 100 : null;
  return {c, v, v2, ok, near, dist};
}
function autoConds(m, ctx){
  const pl = m.plan, out = [];
  if (num(pl.reviewPrice) != null) out.push({id:'auto-review', auto:1, type:'exit', category:'PRICE', label:'Review for sale at ' + usd(num(pl.reviewPrice)), metric:'price', op:'>=', value:num(pl.reviewPrice), action:'REVIEW'});
  if (m.maxW != null) out.push({id:'auto-size', auto:1, type:'exit', category:'SIZE', label:'Position exceeds ' + m.maxW + '% of portfolio', metric:'weight', op:'>', value:m.maxW, action:'TRIM'});
  if (m.hasThesis && m.cagr3y != null && m.minCagr != null) out.push({id:'auto-cagr', auto:1, type:'exit', category:'OPPORTUNITY', label:'Expected 3Y CAGR falls below ' + m.minCagr + '%', metric:'cagr3y', op:'<', value:m.minCagr, action:'REVIEW'});
  if (pl.nextReview) out.push({id:'auto-time', auto:1, type:'exit', category:'TIME', label:'Scheduled thesis review on ' + fmtDate(pl.nextReview), metric:'daysToReview', op:'<=', value:0, action:'REVIEW'});
  if (num(pl.addBelow) != null) out.push({id:'auto-add', auto:1, type:'add', category:'ADD', label:'Add below ' + usd(num(pl.addBelow)) + ', subject to fundamentals', metric:'price', op:'<', value:num(pl.addBelow), action:'ADD'});
  if (num(pl.strongBuy) != null) out.push({id:'auto-strong', auto:1, type:'add', category:'ADD', label:'Strong buy below ' + usd(num(pl.strongBuy)), metric:'price', op:'<', value:num(pl.strongBuy), action:'ADD'});
  return out;
}
function evalExpect(m, e){
  if (!e.metric) return {e, v:null, ok: e.manual === 'met' ? true : e.manual === 'unmet' ? false : null};
  const v = metricValue(m, e.metric);
  return {e, v, ok: cmp(v, e.op, num(e.value))};
}
function autoThesis(m){
  if (m.fail && m.fail.length) return 'BROKEN';
  const r = (m.expect||[]).filter(x => x.ok !== null);
  if (!r.length) return null;
  const met = r.filter(x => x.ok).length / r.length;
  return met === 1 ? 'STRENGTHENING' : met >= .6 ? 'INTACT' : 'WEAKENING';
}
function verify(x){
  const f = x.f || {}, d = x.d || {}, ch = [];
  const add = (q, v, ok) => ch.push({q, v, ok});
  add('Revenue guidance', f.guidance || 'No data', f.guidance ? f.guidance !== 'Cut' : null);
  add('Revenue estimates since ' + x.baseLabel, d.revEstChg != null ? pct(d.revEstChg) : 'No data', d.revEstChg != null ? d.revEstChg > -5 : null);
  add('EPS estimates since ' + x.baseLabel, d.epsEstChg != null ? pct(d.epsEstChg) : 'No data', d.epsEstChg != null ? d.epsEstChg > -10 : null);
  add('Investment thesis', x.thesis || 'Not assessed', x.thesis ? !['WEAKENING','BROKEN'].includes(x.thesis) : null);
  add('Share dilution', d.dilution != null ? pct(d.dilution) : 'No data', d.dilution != null ? d.dilution < 10 : null);
  add('Net debt', d.debtChg != null ? pct(d.debtChg) : 'No data', d.debtChg != null ? d.debtChg < 25 : null);
  add('Why the stock fell', f.declineReason || 'Not recorded', f.declineReason ? f.declineReason !== 'Company-specific' : null);
  add('Major customer lost', f.customerLoss === true ? 'Yes' : f.customerLoss === false ? 'No' : 'No data', typeof f.customerLoss === 'boolean' ? !f.customerLoss : null);
  add('Material analyst downgrades', f.downgrades === true ? 'Yes' : f.downgrades === false ? 'No' : 'No data', typeof f.downgrades === 'boolean' ? !f.downgrades : null);
  add('Analyst consensus target', d.analystTargetChg != null ? pct(d.analystTargetChg) : 'No data', d.analystTargetChg != null ? d.analystTargetChg > -10 : null);
  let vv = 'No data', vok = null;
  if (x.basePrice && x.price) {
    if (x.baseRevEst && num(f.revEst)) {
      const c = ((x.price / num(f.revEst)) / (x.basePrice / x.baseRevEst) - 1) * 100;
      vv = (c < 0 ? 'Cheaper: ' : 'Richer: ') + pct(c, 0) + ' on price-to-sales estimates'; vok = c < 0;
    } else { const c = (x.price / x.basePrice - 1) * 100; vv = 'Price ' + pct(c, 0) + ', no estimate data'; vok = c < 0 ? true : null; }
  }
  add('Valuation vs ' + x.baseLabel, vv, vok);
  if (x.target != null) add('Portfolio exposure', pct(x.weight, 1, false) + ' of ' + x.target + '% target', x.weight < x.target);
  const neg = ch.filter(c => c.ok === false && c.q !== 'Portfolio exposure' && !c.q.startsWith('Valuation'));
  const broken = x.thesis === 'BROKEN' || (d.revEstChg != null && d.revEstChg < -15) || (f.guidance === 'Cut' && f.customerLoss === true);
  const stronger = !neg.length && (x.thesis === 'STRENGTHENING' || (d.revEstChg != null && d.revEstChg >= 5));
  return {code: broken ? 'D' : neg.length ? 'C' : stronger ? 'B' : 'A', checks: ch, unknown: ch.filter(c => c.ok === null).length};
}
function bestAlt(m, ctx){ return ctx.alts.filter(a => a.t !== m.t).sort((a,b) => b.cagr - a.cagr)[0] || null; }

function decide(m, ctx){
  const R = []; let a = 'HOLD', trim = null;
  const cat = k => m.exitTrig.filter(r => r.c.category === k);
  const up = m.fair ? (m.fair / m.price - 1) * 100 : null;
  const estUp = (m.d.revEstChg ?? 0) > 5 || (m.d.epsEstChg ?? 0) > 5;
  if (!m.hasThesis) return {action:'DEFINE THESIS', reasons:['No reason for owning this position is stored, so there is nothing to test it against. Record why you own it and what would make you sell.'], trim:null};
  if (m.thesis === 'BROKEN') {
    a = 'EXIT'; R.push('You marked the thesis BROKEN. The original reason for owning it no longer holds, whatever the price.');
    if (m.gainPct < 0) R.push('Being down ' + pct(m.gainPct) + ' is not a reason to hold. Waiting to get back to ' + usd(m.avg) + ' is anchoring.');
  } else if (m.fail.length) {
    a = 'IMMEDIATE THESIS REVIEW';
    m.fail.forEach(r => R.push('Thesis-failure condition met: ' + r.c.label + ' (now ' + fmtMetric(r.c.metric, r.v) + ').'));
    R.push('Decide whether the thesis is broken. If it is, exit. Your purchase price does not matter to that decision.');
  } else if (m.thesis === 'COMPLETED' || cat('COMPLETION').length) {
    if (m.cagr3y != null && m.cagr3y < m.minCagr) { a = 'EXIT'; R.push('The investment case has played out and the expected 3Y CAGR from here is ' + pct(m.cagr3y) + ', below your ' + m.minCagr + '% minimum.'); }
    else { a = 'TRIM 50%'; R.push('The original case has played out' + (m.cagr3y != null ? ', but your model still implies ' + pct(m.cagr3y) + ' a year' : '') + '. Take half and re-underwrite the rest as a new decision.'); }
  } else if (m.priceHit) {
    R.push('Price reached your review level (' + m.priceHit.c.label + ').');
    if (m.fair == null) { a = 'IMMEDIATE THESIS REVIEW'; R.push('No updated fair value is recorded. Update your model before deciding. The old target alone is not a reason to sell.'); }
    else if (up >= 15 && m.thesis !== 'WEAKENING') {
      a = 'HOLD'; R.push('Your updated fair value is ' + usd(m.fair) + ', ' + pct(up, 0) + ' above the current price.');
      if (estUp) R.push('Revenue estimates are ' + pct(m.d.revEstChg, 0) + ' and EPS estimates ' + pct(m.d.epsEstChg, 0) + ' since purchase. The business improved, not only the price.');
      R.push('Reaching a price target is not the same as completing the thesis. Raise the review price instead of selling.');
    } else if (up >= -10) { a = 'TRIM 25%'; R.push('Price is close to your updated fair value of ' + usd(m.fair) + ' (' + pct(up, 0) + '). A partial sale keeps you in the thesis with less valuation risk.'); }
    else { a = 'TRIM 50%'; R.push('Price is ' + pct(-up, 0, false) + ' above your updated fair value of ' + usd(m.fair) + '.'); }
    if (m.cagr3y != null && m.cagr3y < m.minCagr && a !== 'TRIM 50%') { a = 'TRIM 50%'; R.push('Expected 3Y CAGR from here is ' + pct(m.cagr3y) + ', below your ' + m.minCagr + '% minimum.'); }
  } else if (cat('SIZE').length) {
    a = m.weight / m.maxW > 1.5 ? 'TRIM 50%' : 'TRIM 25%';
    const tgt = m.targetW ?? m.maxW; trim = Math.max(0, m.shares * (1 - tgt / m.weight));
    R.push('At ' + pct(m.weight, 1, false) + ' of the portfolio it is above your ' + m.maxW + '% limit. Selling about ' + fmtN(trim, trim < 10 ? 1 : 0) + ' shares brings it back to ' + tgt + '%.');
    R.push('This is a sizing decision, not a verdict on the thesis.');
  } else if (cat('VALUATION').length) {
    a = 'TRIM 25%'; cat('VALUATION').forEach(r => R.push('Valuation condition met: ' + r.c.label + ' (now ' + fmtMetric(r.c.metric, r.v) + ').'));
  } else if (cat('OPPORTUNITY').length) {
    const best = bestAlt(m, ctx);
    if (best && best.cagr - (m.cagr3y ?? 0) >= 8) { a = 'TRIM 25%'; R.push('Expected 3Y CAGR here is ' + pct(m.cagr3y) + '. ' + best.t + ' offers ' + pct(best.cagr) + ' with an intact thesis.'); }
    else { a = 'STOP ADDING'; R.push('Expected 3Y CAGR of ' + pct(m.cagr3y) + ' is below your ' + m.minCagr + '% minimum, and nothing else offers a clearly better return yet.'); }
  } else if (cat('TIME').length) {
    a = 'IMMEDIATE THESIS REVIEW'; R.push('Your scheduled review date has passed. Check whether the catalyst you expected has happened.');
  } else if (cat('LIQUIDITY').length) {
    a = 'TRIM 25%'; R.push('Your personal liquidity condition is active. Raise cash from the position with the weakest forward return first.');
  } else if (m.addHits.length && m.ver) {
    if (m.ver.code === 'D') { a = 'STOP ADDING'; R.push('Your add price was reached, but the thesis has broken. A cheaper stock is not a better business.'); }
    else if (m.ver.code === 'C') { a = 'HOLD'; R.push('Your add price was reached, but fundamentals have deteriorated. Investigate before adding.'); m.ver.checks.filter(c => c.ok === false).forEach(c => R.push(c.q + ': ' + c.v)); }
    else if (m.targetW != null && m.weight >= m.targetW) { a = 'STOP ADDING'; R.push('Add condition met, but the position is already ' + pct(m.weight, 1, false) + ' against a ' + m.targetW + '% target.'); }
    else { a = 'ADD'; R.push('Price ' + usd(m.price) + ' met your add condition and the fundamentals check passed' + (m.ver.code === 'B' ? ', with a stronger thesis' : '') + '.'); if (m.targetW != null) R.push('Room to target: ' + pct(m.targetW - m.weight, 1, false) + ' of the portfolio.'); }
  } else if ((num(m.plan.stopAddingAbove) != null && m.price > num(m.plan.stopAddingAbove)) || (m.targetW != null && m.weight >= m.targetW)) {
    a = 'STOP ADDING';
    R.push(m.targetW != null && m.weight >= m.targetW ? 'At ' + pct(m.weight, 1, false) + ' it has reached your ' + m.targetW + '% target size.' : 'Price is above your stop-adding level of ' + usd(num(m.plan.stopAddingAbove)) + '.');
    R.push('Hold, and send new money elsewhere.');
  } else {
    R.push(m.thesis ? 'Thesis ' + m.thesis.toLowerCase() + '. No exit or add condition is active.' : 'No exit or add condition is active.');
    if (m.cagr3y != null) R.push('Expected 3Y CAGR from ' + usd(m.price) + ': ' + pct(m.cagr3y) + '.');
  }
  if (ctx.set.taxEnabled && /TRIM|EXIT/.test(a) && m.avg) {
    const frac = a === 'EXIT' ? 1 : a === 'TRIM 50%' ? .5 : .25, gain = (m.price - m.avg) * m.shares * frac;
    const rate = (m.days > 365 ? num(ctx.set.taxLong) : num(ctx.set.taxShort)) || 0;
    if (gain > 0 && rate > 0) R.push('Estimated tax on this sale: ' + money(ctx.toSAR(gain * rate / 100, m.cur)) + ' at ' + rate + '% (' + (m.days > 365 ? 'long' : 'short') + '-term).');
  }
  return {action:a, reasons:R, trim};
}
function statusOf(m){
  if (m.thesis === 'BROKEN' || m.fail.length) return 'THESIS BREAK';
  if (m.priceHit || m.thesis === 'COMPLETED') return 'TARGET REACHED';
  if (m.exitTrig.length) return 'REVIEW';
  if (m.addHits.length && m.ver) {
    if (m.ver.code === 'D') return 'REVIEW';
    if (m.ver.code === 'C') return 'WATCH';
    if (m.targetW == null || m.weight < m.targetW) return (m.ver.code === 'B' || m.strongHit) ? 'ADD' : 'ACCUMULATE';
  }
  if (m.thesis === 'WEAKENING' || m.near.length) return 'WATCH';
  return 'HOLD';
}
function lessonsFor(m, ctx){
  const L = [], mc = ctx.mistakes;
  const bigEst = Math.max(m.d.epsEstChg ?? -999, m.d.revEstChg ?? -999);
  if (m.priceHit && mc['SOLD WINNER TOO EARLY'] && bigEst > 10) L.push(['SOLD WINNER TOO EARLY', 'You previously identified selling winners too early as a recurring mistake. ' + m.t + ' has reached your original target, but earnings estimates have risen ' + pct(bigEst, 0) + ' since purchase. Reassess intrinsic value before selling.']);
  if ((m.fail.length || m.thesis === 'BROKEN' || m.thesis === 'WEAKENING') && mc['HELD BROKEN THESIS']) L.push(['HELD BROKEN THESIS', 'You have held a broken thesis before. Decide on the evidence now rather than waiting another quarter.']);
  if ((m.fail.length || m.thesis === 'BROKEN' || m.thesis === 'WEAKENING') && m.gainPct < -10 && mc['ANCHORING TO PURCHASE PRICE']) L.push(['ANCHORING TO PURCHASE PRICE', 'You have tagged anchoring before. ' + usd(m.avg) + ' is your cost, not the value of the business.']);
  if (m.addHits.length && m.ver && (m.ver.code === 'C' || m.ver.code === 'D') && mc['AVERAGED DOWN WITHOUT THESIS']) L.push(['AVERAGED DOWN WITHOUT THESIS', 'You have averaged down without a thesis before. Price met your add level, but the fundamentals check did not pass.']);
  if (m.exitTrig.some(r => r.c.category === 'SIZE') && mc['POSITION TOO LARGE']) L.push(['POSITION TOO LARGE', 'Oversized positions are in your mistake library. Trim back to plan.']);
  if ((m.d.dilution ?? 0) > 5 && mc['IGNORED DILUTION']) L.push(['IGNORED DILUTION', 'Share count is up ' + pct(m.d.dilution) + ' since purchase. You have ignored dilution before.']);
  if (m.priceHit && (m.priceToFair ?? -1) > 0 && mc['IGNORED VALUATION']) L.push(['IGNORED VALUATION', 'Price is above your own fair value, and ignoring valuation is in your mistake library.']);
  return L;
}
function lessonsForWatch(m, ctx){
  const L = [], mc = ctx.mistakes;
  if (m.wstatus === 'MISSED ENTRY' && (mc['CHASED PRICE'] || mc['FOMO'])) L.push(['CHASED PRICE', 'Chasing price is in your mistake library. Only the updated buy price matters, not the move you missed.']);
  if (m.ver && (m.ver.code === 'C' || m.ver.code === 'D') && mc['AVERAGED DOWN WITHOUT THESIS']) L.push(['CHEAPER IS NOT BETTER', 'The price fell to your level, but the business may have become worse. Check the failed items first.']);
  return L;
}

function alertsForPos(m, ctx){
  const A = [], href = 'pos-' + m.t;
  const base = [['Current', usd(m.price)], ['Average cost', usd(m.avg)], ['Return', pct(m.gainPct)]];
  const broke = m.thesis === 'BROKEN' || m.fail.length;
  if (broke) A.push({key:m.t+':break', sev:'THESIS BREAK', tone:'r', t:m.t, href, title: m.fail.length ? 'THESIS BREAK CONDITION TRIGGERED' : 'THESIS MARKED BROKEN',
    text: m.fail.map(r => r.c.label + '. Now ' + fmtMetric(r.c.metric, r.v) + '; your sell-review condition was ' + condThreshold(r.c) + '.'), lines: base, action: m.rec.action === 'EXIT' ? 'EXIT' : 'REVIEW POSITION'});
  if (!broke && (m.priceHit || m.thesis === 'COMPLETED')) {
    const but = [];
    if (m.d.revEstChg != null) but.push(['Revenue estimates since purchase', pct(m.d.revEstChg, 0)]);
    if (m.d.epsEstChg != null) but.push(['EPS estimates since purchase', pct(m.d.epsEstChg, 0)]);
    if (m.fair) but.push(['Updated model fair value', usd(m.fair)]);
    A.push({key:m.t+':target', sev:'IMPORTANT', tone:'b', t:m.t, href, title: m.thesis === 'COMPLETED' ? 'THESIS COMPLETED' : 'TARGET REACHED',
      lines:[['Current', usd(m.price)], ['Original review price', usd(num(m.plan.reviewPrice) ?? (m.priceHit ? num(m.priceHit.c.value) : null))]], but,
      action: m.rec.action === 'HOLD' ? 'REASSESS — DO NOT AUTO-SELL' : m.rec.action});
  }
  if (!broke) m.exitTrig.filter(r => !['FAILURE','PRICE'].includes(r.c.category)).forEach(r => {
    const sev = r.c.category === 'SIZE' ? 'ACTION REQUIRED' : 'IMPORTANT';
    A.push({key:m.t+':'+r.c.id, sev, tone: sev === 'ACTION REQUIRED' ? 'r' : 'o', t:m.t, href, title: CAT_TITLE[r.c.category] || 'EXIT CONDITION MET',
      text:[r.c.label + '. Now ' + fmtMetric(r.c.metric, r.v) + '.'], lines: base.concat([['Weight', pct(m.weight, 1, false)]]), action: m.rec.action});
  });
  if (!broke && m.addHits.length && m.ver) {
    const V = VER[m.ver.code], hit = m.addHits.find(r => r.c.id === 'auto-strong') || m.addHits[0];
    A.push({key:m.t+':add', sev:'IMPORTANT', tone:V.tone, t:m.t, href, title: (m.ver.code === 'A' || m.ver.code === 'B') ? 'BUY CONDITION MET' : 'PRICE CONDITION MET',
      lines:[['Current', usd(m.price)], ['Your buy zone', condThreshold(hit.c)], ['Revenue estimates', m.d.revEstChg == null ? 'No data' : Math.abs(m.d.revEstChg) < 2 ? 'UNCHANGED' : pct(m.d.revEstChg)], ['Thesis', m.thesis || 'Not set']],
      stack: V.plines, action: V.pverdict});
  }
  if (!broke && !m.priceHit) m.near.forEach(r => {
    A.push({key:m.t+':near:'+r.c.id, sev:'WATCH', tone:'y', t:m.t, href, title: r.c.type === 'add' ? 'APPROACHING BUY' : r.c.category === 'SIZE' ? 'APPROACHING SIZE LIMIT' : r.c.category === 'TIME' ? 'THESIS REVIEW DUE SOON' : 'APPROACHING ' + (CAT_TITLE[r.c.category] || 'TRIGGER').replace(/ (REACHED|MET|DUE)$/,''),
      lines:[['Current', fmtMetric(r.c.metric, r.v)], [r.c.type === 'add' ? 'Buy zone' : 'Trigger', condThreshold(r.c)], ['Distance', r.c.metric === 'daysToReview' ? Math.round(r.v) + ' days' : pct(Math.abs(r.dist), 1, false)]], action:'WATCH'});
  });
  const nc = m.nc;
  if (nc && nc.date) { const dd = daysBetween(todayISO(), nc.date); if (dd != null && dd >= 0 && dd <= 7) A.push({key:m.t+':cat:'+nc.date, sev:'INFO', tone:'n', t:m.t, href, title:'CATALYST IN ' + dd + ' DAYS', text:[nc.label + ' on ' + fmtDate(nc.date) + '.'], action:'PREPARE'}); }
  if (!m.hasThesis) A.push({key:m.t+':nothesis', sev:'INFO', tone:'y', t:m.t, href, kind:'missing', title:'NO INVESTMENT THESIS',
    text:['You own ' + m.t + ' but there is no stored investment thesis. Would you like to define why you own it and what would make you sell?'], action:'DEFINE THESIS'});
  else if (num(m.plan.reviewPrice) == null && !(m.p.conditions||[]).some(c => c.type !== 'add')) A.push({key:m.t+':noexit', sev:'INFO', tone:'y', t:m.t, href, kind:'missing', title:'NO EXIT PLAN',
    text:['There is a thesis for ' + m.t + ' but no review price or sell condition. Decide now, while you are calm, what would make you sell.'], action:'ADD EXIT PLAN'});
  return A;
}
function alertsForWatch(m, ctx){
  const A = [], href = 'watch-' + m.t;
  const news = newsOf(m.f)[0];
  const info = [['Thesis', m.thesis || 'Not assessed'], ['Valuation', m.cagr3y != null ? pct(m.cagr3y) + ' expected 3Y CAGR' : 'No model'], ['Analyst consensus', m.d.analystTargetChg == null ? 'No data' : Math.abs(m.d.analystTargetChg) < 2 ? 'UNCHANGED' : pct(m.d.analystTargetChg)], ['Important news', news ? news.headline : 'None recorded']];
  if (m.wstatus === 'APPROACHING BUY') A.push({key:m.t+':wnear', sev:'WATCH', tone:'y', t:m.t, href, title:'APPROACHING BUY', text:[m.t + ' is now ' + pct(m.dist, 1, false) + ' from your ' + usd(m.buy) + ' entry.'], lines:[['Current', usd(m.price)], ['Buy zone', 'below ' + usd(m.buy)]].concat(info), action:'WATCH'});
  if (m.ver) {
    const V = VER[m.ver.code], strong = m.wstatus === 'STRONG BUY REACHED';
    A.push({key:m.t+':wbuy', sev:'IMPORTANT', tone:V.tone, t:m.t, href, title: strong ? 'STRONG BUY PRICE REACHED' : 'BUY PRICE REACHED',
      text:[m.t + ' reached ' + usd(m.price) + '. Your ' + (strong ? 'high-conviction level' : 'desired entry') + ' was ' + usd(strong ? m.strong : m.buy) + '.'],
      lines:[['Thesis', m.thesis || 'Not assessed'], ['Analyst consensus', info[2][1]], ['Revenue estimates since added', m.d.revEstChg != null ? pct(m.d.revEstChg) : 'No data'], ['Reason for decline', m.f.declineReason || 'Not recorded']],
      stack: V.lines, action: V.verdict});
  }
  if (m.wstatus === 'MISSED ENTRY') A.push({key:m.t+':missed', sev:'INFO', tone:'n', t:m.t, href, title:'MISSED ENTRY — ' + m.missed.verdict, text:['Price moved from ' + usd(num(m.w.priceAdded)) + ' to ' + usd(m.price) + ' without reaching your ' + usd(m.buy) + ' buy price. Do not chase: judge it against an updated buy price.'], lines:[['Old buy price', usd(m.buy)], ['Updated buy price', usd(m.updatedBuy)], ['Current', usd(m.price)]], action: m.missed.verdict});
  const nc = m.nc;
  if (nc && nc.date) { const dd = daysBetween(todayISO(), nc.date); if (dd != null && dd >= 0 && dd <= 7) A.push({key:m.t+':cat:'+nc.date, sev:'INFO', tone:'n', t:m.t, href, title:'CATALYST IN ' + dd + ' DAYS', text:[nc.label + ' on ' + fmtDate(nc.date) + '.'], action:'PREPARE'}); }
  return A;
}
function mistakeCounts(){ const c = {}; Object.values(S.closed).forEach(x => (x.mistakes||[]).forEach(t => c[t] = (c[t]||0) + 1)); return c; }

function watchMetrics(w, ctx){
  const f = fundOf(w.ticker), lp = livePrice(w.ticker, w), price = lp.price;
  const m = {t:w.ticker, w, f, price, prev:lp.prev, live:lp.live, priceAt:lp.at, cur:w.currency || 'USD', isWatch:true, nc:catalystOf(w, f)};
  m.buy = num(w.buyBelow); m.strong = num(w.strongBuy);
  m.dist = (m.buy && price) ? (price - m.buy) / price * 100 : null;
  m.v3y = num(w.model3y); m.v5y = num(w.model5y);
  m.cagr3y = (m.v3y && price) ? (Math.pow(m.v3y / price, 1/3) - 1) * 100 : null;
  m.cagr5y = (m.v5y && price) ? (Math.pow(m.v5y / price, 1/5) - 1) * 100 : null;
  m.analystTarget = num(f.analystTarget) ?? num(w.analystTarget1y);
  m.upside = (m.analystTarget && price) ? (m.analystTarget / price - 1) * 100 : null;
  m.base = w.baseline || {}; m.d = deltas(m.base, f); m.thesis = w.thesisStatus || null;
  m.drawdown = (num(f.high52) && price) ? (price / num(f.high52) - 1) * 100 : null;
  m.dayPct = m.prev ? (price / m.prev - 1) * 100 : null;
  m.targetW = num(w.maxPosition);
  m.hurdle = num(w.hurdle) ?? num(ctx.set.hurdle) ?? 15;
  const priceAdded = num(w.priceAdded);
  m.computedBuy = m.v3y ? m.v3y / Math.pow(1 + m.hurdle / 100, 3) : null;
  m.updatedBuy = num(w.updatedBuy) ?? m.computedBuy;
  if (price == null || m.buy == null) m.wstatus = 'NO BUY PRICE';
  else if (m.strong != null && price <= m.strong) m.wstatus = 'STRONG BUY REACHED';
  else if (price <= m.buy) m.wstatus = 'BUY PRICE REACHED';
  else if (m.dist <= ctx.approach) m.wstatus = 'APPROACHING BUY';
  else if (priceAdded && price >= priceAdded * 1.25 && price >= m.buy * 1.4) m.wstatus = 'MISSED ENTRY';
  else m.wstatus = 'WAIT';
  if (m.wstatus === 'MISSED ENTRY') {
    const ub = m.updatedBuy;
    m.missed = !ub ? {verdict:'REASSESS VALUE', tone:'n'} : price <= ub ? {verdict:'AT UPDATED ENTRY', tone:'g'} : (price - ub) / price * 100 <= 10 ? {verdict:'NEAR UPDATED ENTRY', tone:'y'} : {verdict:'STILL TOO EXPENSIVE', tone:'n'};
  }
  if (m.wstatus === 'BUY PRICE REACHED' || m.wstatus === 'STRONG BUY REACHED')
    m.ver = verify({thesis:m.thesis, f, d:m.d, price, basePrice:num(m.base.price) ?? priceAdded, baseRevEst:num(m.base.revEst), weight:0, target:m.targetW, baseLabel:'added to watchlist'});
  return m;
}

function computeAll(){
  const set = cfg(), fx = num(set.fx) || 3.75;
  const toSAR = (v, cur) => v == null ? null : (cur === 'SAR' ? v : v * fx);
  const byT = {};
  Object.values(S.transactions).forEach(t => { if (t && t.ticker) (byT[t.ticker] = byT[t.ticker] || []).push(t); });
  const lots = {}; Object.keys(byT).forEach(t => lots[t] = lotsOf(byT[t]));
  const pos = [];
  new Set([...Object.keys(S.positions), ...Object.keys(lots)]).forEach(t => {
    const L = lots[t]; if (!L || L.shares <= 0) return;
    const p = S.positions[t] || {ticker:t};
    const cur = p.currency || L.cur || 'USD', lp = livePrice(t, p, L.lastPrice), price = lp.price, prev = lp.prev;
    const f = fundOf(t);
    const m = {t, p, f, L, cur, shares:L.shares, avg:L.avg, price, prev, live:lp.live, priceAt:lp.at, nc:catalystOf(p, f)};
    if (!p.company && f.auto && f.auto.company) m.p = p = Object.assign({}, p, {company:f.auto.company});
    m.valueSAR = toSAR(price * L.shares, cur); m.costSAR = toSAR(L.cost, cur);
    m.gainPct = L.avg ? (price / L.avg - 1) * 100 : null; m.gainSAR = m.valueSAR - m.costSAR;
    m.dayPct = prev ? (price / prev - 1) * 100 : null; m.daySAR = prev ? toSAR((price - prev) * L.shares, cur) : 0;
    m.start = L.cycleStart || L.firstBuy; m.days = m.start ? daysBetween(m.start, todayISO()) : null; m.months = m.days != null ? m.days / 30.44 : null;
    m.annPct = (m.days >= 90 && m.gainPct != null) ? (Math.pow(1 + m.gainPct / 100, 365 / m.days) - 1) * 100 : null;
    const md = p.model || {}, th = p.thesis || {};
    m.fair = num(md.fairValue); m.v3y = num(md.v3y) ?? num(th.v3y); m.v5y = num(md.v5y) ?? num(th.v5y);
    m.cagr3y = (m.v3y && price) ? (Math.pow(m.v3y / price, 1/3) - 1) * 100 : null;
    m.cagr5y = (m.v5y && price) ? (Math.pow(m.v5y / price, 1/5) - 1) * 100 : null;
    m.priceToFair = m.fair ? (price / m.fair - 1) * 100 : null;
    m.analystTarget = num(f.analystTarget);
    m.base = p.baseline || {}; m.d = deltas(m.base, f);
    m.thesis = p.thesisStatus || null;
    m.hasThesis = !!(th.summary || th.whyOwn);
    m.plan = p.plan || {};
    m.targetW = num(m.plan.targetWeight); m.maxW = num(m.plan.maxWeight) ?? num(set.maxWeight);
    m.hurdle = num(m.plan.hurdle) ?? num(set.hurdle); m.minCagr = num(m.plan.minCagr) ?? num(set.minCagr);
    pos.push(m);
  });
  const valueSAR = sum(pos.map(m => m.valueSAR)), cash = num(set.cash) || 0, total = valueSAR + cash;
  pos.forEach(m => m.weight = total ? m.valueSAR / total * 100 : 0);
  const owned = new Set(pos.map(m => m.t));
  const ctx = {set, fx, toSAR, total, cash, approach: num(set.approachPct) ?? 5, mistakes: mistakeCounts()};
  const watch = Object.values(S.watchlist).filter(w => w && w.ticker && !owned.has(w.ticker)).map(w => watchMetrics(w, ctx));
  ctx.alts = [...watch, ...pos].filter(x => x.cagr3y != null && !['WEAKENING','BROKEN'].includes(x.thesis)).map(x => ({t:x.t, cagr:x.cagr3y}));
  pos.forEach(m => {
    const conds = [...autoConds(m, ctx), ...(m.p.conditions || [])];
    m.conds = conds.map(c => evalCond(m, c, ctx));
    m.exitTrig = m.conds.filter(r => r.c.type !== 'add' && r.ok === true);
    m.fail = m.exitTrig.filter(r => r.c.category === 'FAILURE');
    m.priceHit = m.exitTrig.find(r => r.c.category === 'PRICE');
    m.addHits = m.conds.filter(r => r.c.type === 'add' && r.ok === true);
    m.strongHit = m.addHits.some(r => r.c.id === 'auto-strong');
    m.near = m.conds.filter(r => r.near);
    m.expect = (m.p.expectations || []).map(e => evalExpect(m, e));
    m.autoThesis = autoThesis(m);
    m.ver = m.addHits.length ? verify({thesis:m.thesis, f:m.f, d:m.d, price:m.price, basePrice:num(m.base.price), baseRevEst:num(m.base.revEst), weight:m.weight, target:m.targetW, baseLabel:'purchase'}) : null;
    m.rec = decide(m, ctx);
    m.status = statusOf(m);
    m.lessons = lessonsFor(m, ctx);
    m.alerts = alertsForPos(m, ctx);
  });
  watch.forEach(m => { m.alerts = alertsForWatch(m, ctx); m.lessons = lessonsForWatch(m, ctx); });
  const invested = sum(pos.map(m => m.costSAR)), daySAR = sum(pos.map(m => m.daySAR));
  const ytdStart = num(set.ytdStart), ytdContrib = num(set.ytdContrib) || 0;
  const totals = {valueSAR, cash, total, invested, daySAR, dayPct: (total - daySAR) ? daySAR / (total - daySAR) * 100 : null,
    gainSAR: valueSAR - invested, gainPct: invested ? (valueSAR / invested - 1) * 100 : null,
    ytdPct: ytdStart ? (total - ytdStart - ytdContrib) / ytdStart * 100 : null};
  const alerts = [...pos.flatMap(m => m.alerts), ...watch.flatMap(m => m.alerts)].map(a => Object.assign(a, {sig: a.sev + '|' + a.title + '|' + a.action}));
  alerts.sort((a,b) => SEV_RANK[a.sev] - SEV_RANK[b.sev]);
  return Object.assign(ctx, {pos, watch, totals, alerts, byT, lots});
}
function isAcked(a){ const s = S.alertState[safeId(a.key)]; return !!(s && s.ack && s.sig === a.sig); }

function allocate(C, amount){
  const totalAfter = C.totals.total + amount, step = amount < 2000 ? 100 : 250, minTicket = amount < 2000 ? 300 : 500;
  const cands = [];
  C.pos.forEach(m => {
    let eligible = false, tier = 1, note = '';
    if (!m.hasThesis) note = 'Define a thesis first';
    else if (m.status === 'ADD' || m.status === 'ACCUMULATE') { eligible = true; note = m.status === 'ADD' ? 'Add condition met, fundamentals verified' : 'Accumulation condition satisfied'; }
    else if (m.status === 'HOLD' && m.cagr3y != null && m.cagr3y >= m.hurdle && (m.targetW == null || m.weight < m.targetW) && m.rec.action !== 'STOP ADDING') { eligible = true; tier = .6; note = 'Below target weight; ' + pct(m.cagr3y) + ' expected 3Y CAGR clears your ' + m.hurdle + '% hurdle'; }
    else if (m.status === 'THESIS BREAK') note = 'Thesis review pending';
    else if (m.status === 'TARGET REACHED') note = 'At review price. Reassess before adding';
    else if (m.status === 'REVIEW') note = 'An exit condition is active';
    else if (m.targetW != null && m.weight >= m.targetW) note = 'At target size';
    else if (num(m.plan.addBelow) != null) note = 'Wait for ' + usd(num(m.plan.addBelow));
    else if (m.cagr3y != null && m.cagr3y < m.hurdle) note = pct(m.cagr3y) + ' expected CAGR is below your ' + m.hurdle + '% hurdle';
    else note = 'No add condition recorded';
    const tgt = m.targetW ?? m.maxW;
    const room = tgt != null ? Math.max(0, tgt / 100 * totalAfter - m.valueSAR) : amount;
    const score = eligible ? Math.max(m.cagr3y ?? m.hurdle, 1) / 100 * (CONV[m.p.conviction] || .75) * (m.ver && m.ver.code === 'B' ? 1.15 : 1) * tier : 0;
    cands.push({t:m.t, eligible, score, room, note, owned:true, wait: num(m.plan.addBelow)});
  });
  C.watch.forEach(m => {
    let eligible = false, note = '';
    if (m.ver && (m.ver.code === 'A' || m.ver.code === 'B')) { eligible = true; note = VER[m.ver.code].verdict; }
    else if (m.ver) note = VER[m.ver.code].verdict;
    else if (m.wstatus === 'MISSED ENTRY') note = 'Missed entry: ' + m.missed.verdict.toLowerCase();
    else if (m.buy) note = 'Wait for ' + usd(m.buy);
    else note = 'No buy price set';
    const room = m.targetW != null ? m.targetW / 100 * totalAfter : amount * .5;
    const score = eligible ? Math.max(m.cagr3y ?? m.hurdle, 1) / 100 * (CONV[m.w.conviction] || .75) * (m.ver.code === 'B' ? 1.15 : 1) : 0;
    cands.push({t:m.t, eligible, score, room, note, owned:false});
  });
  const cashScore = (num(C.set.hurdle) || 15) / 100;
  const el = cands.filter(c => c.eligible && c.room >= minTicket);
  const give = {}; let excess = 0;
  const tot = el.reduce((s,c) => s + c.score, 0) + cashScore;
  el.forEach(c => { const g = amount * c.score / tot; give[c.t] = Math.min(g, c.room); excess += g - give[c.t]; });
  const open = el.filter(c => give[c.t] < c.room - 1);
  if (excess > 0 && open.length) { const t2 = open.reduce((s,c) => s + c.score, 0) + cashScore; open.forEach(c => give[c.t] = Math.min(c.room, give[c.t] + excess * c.score / t2)); }
  el.forEach(c => { let g = Math.floor(give[c.t] / step) * step; if (g < minTicket) g = 0; give[c.t] = g; });
  cands.forEach(c => c.amt = give[c.t] || 0);
  const deployed = sum(cands.map(c => c.amt)), cash = amount - deployed;
  cands.sort((a,b) => b.amt - a.amt || (b.eligible - a.eligible) || (a.owned === b.owned ? 0 : a.owned ? -1 : 1));
  let why;
  if (!el.length) why = 'Nothing meets your buy or add conditions today. Hold the cash until a position reaches its buy zone with the thesis intact.';
  else if (cash / amount >= .3) why = 'Current opportunities are not attractive enough to deploy all available capital. The rest stays in cash until more positions reach your buy conditions.';
  else if (cash > 0) why = 'Most of the money goes to positions that meet your conditions. The remainder stays in cash because of position-size limits and minimum ticket size.';
  else why = 'Every rand of this contribution has a position that meets your conditions and has room below its target weight.';
  return {rows:cands, cash, deployed, why};
}

/* ------------------------------------------------------------------ */
/* Rendering helpers                                                   */
/* ------------------------------------------------------------------ */
const chip = (txt, tone, extra='') => '<span class="chip t-' + (tone||'n') + ' ' + extra + '">' + esc(txt) + '</span>';
const sChip = s => chip(s, STATUS_TONE[s]);
const tChip = s => s ? chip(s, THESIS_TONE[s]) : chip('NOT SET', 'n', 'nd');
const aChip = s => chip(s, ACTION_TONE[s] || 'n', 'nd');
const kv = rows => '<dl class="kv">' + rows.map(r => '<dt>' + esc(r[0]) + '</dt><dd' + (r[2] ? ' class="txt"' : '') + '>' + esc(r[1]) + '</dd>').join('') + '</dl>';
const lbl = t => '<div class="lbl">' + esc(t) + '</div>';
const secHead = (title, sub, right) => '<div class="sh"><h2>' + esc(title) + '</h2>' + (sub ? '<span class="sub">' + sub + '</span>' : '') + (right ? '<span class="grow"></span>' + right : '') + '</div>';
function mdLite(t){
  const lines = esc(t).split(/\n/); let out = '', inList = false;
  lines.forEach(l => {
    const b = l.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    if (/^\s*[-*•]\s+/.test(l)) { if (!inList) { out += '<ul>'; inList = true; } out += '<li>' + b.replace(/^\s*[-*•]\s+/, '') + '</li>'; }
    else { if (inList) { out += '</ul>'; inList = false; } if (l.trim()) out += '<p>' + b.replace(/^#+\s*/, '') + '</p>'; }
  });
  return out + (inList ? '</ul>' : '');
}
function sellIfText(m){
  const pl = m.plan;
  if (pl.sellIfText) return pl.sellIfText;
  const f = (m.p.conditions || []).filter(c => c.type !== 'add' && c.category === 'FAILURE').map(c => c.label);
  if (f.length) return f.join(' OR ');
  return (m.p.thesis && m.p.thesis.sellReason) || '—';
}
function catalystText(nc){
  if (!nc || !nc.label) return '—';
  const dd = nc.date ? daysBetween(todayISO(), nc.date) : null;
  return nc.label + (dd != null ? (dd >= 0 ? ' — ' + dd + ' days' : ' — ' + fmtDate(nc.date)) : '');
}
function hasExamples(){ return COLS.some(c => Object.values(S[c]).some(d => d && d.example)); }

/* ------------------------------------------------------------------ */
/* Top bar                                                             */
/* ------------------------------------------------------------------ */
function syncState(){ return mode === 'local' ? 'local' : !navigator.onLine || outbox.length ? 'local' : synced ? 'db' : ''; }
function syncText(){
  if (mode === 'local') return 'On this device only';
  if (!navigator.onLine) return 'Offline' + (outbox.length ? ' · ' + outbox.length + ' to sync' : '');
  if (outbox.length) return 'Syncing ' + outbox.length + '…';
  return synced ? 'Synced' : 'Connecting…';
}
function renderTop(C){
  const r = route().name;
  const n = C ? C.alerts.filter(a => a.sev !== 'INFO' && !isAcked(a)).length : 0;
  const link = (h, t, on) => '<a href="#' + h + '" class="' + (on ? 'on' : '') + '">' + t + '</a>';
  $('#top').innerHTML =
    '<a class="brand" href="#home"><span class="mark" aria-hidden="true"></span>Thesis Ledger</a>' +
    '<nav class="nav" aria-label="Main">' + link('home', 'Home', r === 'home') + link('exitmap', 'Exit map', r === 'exitmap') + link('journal', 'Journal', r === 'journal') +
      link('inbox', 'Alerts' + (n ? '<span class="cnt">' + n + '</span>' : ''), r === 'inbox') + '</nav>' +
    '<span class="grow"></span>' +
    '<span class="sync ' + syncState() + '" title="' + esc(user ? user.email : '') + '"><i></i>' + syncText() + '</span>' +
    (installEvt ? '<button class="btn sm" data-act="install">Install app</button>' : '') +
    (mode === 'cloud' && CFG.ai !== false ? '<button class="btn sm" data-act="shot">Import screenshot</button>' : '') +
    '<button class="btn sm" data-act="prices">Update prices</button>' +
    '<button class="btn sm pri" data-act="tx" data-side="BUY">Record buy</button>' +
    '<button class="icon" data-act="settings" aria-label="Settings" title="Settings">Settings</button>';
}

/* ------------------------------------------------------------------ */
/* Home                                                                */
/* ------------------------------------------------------------------ */
function renderHome(C){
  const top = (ui.updateReady ? '<div class="banner"><strong>A new version is ready.</strong><span class="grow"></span><button class="btn sm pri" data-act="reload">Reload</button></div>' : '') +
    (ui.offerImport ? '<div class="banner"><strong>Data found on this device.</strong><span>You used Thesis Ledger here without an account. Move that data into your account?</span><span class="grow"></span><button class="btn sm pri" data-act="import-guest">Move it into my account</button><button class="btn sm ghost" data-act="dismiss-import">Not now</button></div>' : '') +
    (mode === 'local' && cloudReady ? '<div class="banner"><strong>Guest mode.</strong><span>Your data is saved on this device only. Create a free account to sync it across your phone and computer.</span><span class="grow"></span><button class="btn sm pri" data-act="to-auth">Create account</button></div>' : '');
  if (!C.pos.length && !C.watch.length && !Object.keys(S.closed).length) return top + renderEmpty();
  return top + (hasExamples() ? '<div class="banner"><strong>Example portfolio.</strong><span>These positions, prices and theses are illustrations, not your records. Record your own buys, then remove the examples.</span><span class="grow"></span><button class="btn sm warn" data-act="rm-examples">Remove example data</button></div>' : '') +
    '<div class="home">' + secPortfolio(C) + secAttention(C) + secBuyOps(C) + secPositions(C) + secWatch(C) + secAlloc(C) + secBoard(C) + secCatalysts(C) + secXray(C) + secMacro(C) + '</div>';
}
function renderEmpty(){
  return '<div class="block" style="border-top:0"><div class="empty"><h2 style="margin:0;font-family:var(--cond);letter-spacing:.05em;text-transform:uppercase;font-size:18px">Your investment memory starts with one decision</h2>' +
    '<p class="prose">Record a buy and write down why you bought it, what you expected, and what would make you sell. From then on, every alert compares current reality with that decision.</p>' +
    '<div class="btns"><button class="btn pri" data-act="tx" data-side="BUY">Record a buy</button>' + (mode === 'cloud' && CFG.ai !== false ? '<button class="btn pri" data-act="shot">Import a screenshot of my portfolio</button>' : '') + '<button class="btn" data-act="watch-edit">Add to watchlist</button><button class="btn" data-act="settings">Set cash and rules</button></div>' +
    '<p class="fhint">Want to see how it works first? <button class="btn sm" data-act="load-examples">Load an example portfolio</button> You can remove it with one click.</p></div></div>';
}
function ago(iso){ const s = Math.max(0, (Date.now() - Date.parse(iso)) / 1000); return s < 90 ? 'just now' : s < 3600 ? Math.round(s / 60) + ' min ago' : s < 86400 ? Math.round(s / 3600) + ' h ago' : fmtDate(iso); }
function liveText(C){
  const all = [...C.pos, ...C.watch], live = all.filter(m => m.live);
  if (!all.length) return '—';
  if (!live.length) return mode === 'cloud' && CFG.quotes !== false ? (MK.error ? '<small>' + esc(MK.error) + '</small>' : '<small>Waiting for first update</small>') : '<small>Manual</small>';
  const newest = live.map(m => m.priceAt).sort().pop();
  return '<span class="live-dot" aria-hidden="true"></span>Live · ' + ago(newest) + (live.length < all.length ? ' <small>(' + (all.length - live.length) + ' manual)</small>' : '');
}
function secPortfolio(C){
  const T = C.totals, set = C.set, disp = ui.display || set.display || 'SAR';
  const contrib = (num(set.contribMin) || num(set.contribMax)) ? 'SAR ' + fmtN(num(set.contribMin) || 0, 0) + '–' + fmtN(num(set.contribMax) || 0, 0) : '—';
  const nd = (() => { const d = new Date(), day = num(set.contribDay) || 1; let n = new Date(d.getFullYear(), d.getMonth(), day); if (n <= d) n = new Date(d.getFullYear(), d.getMonth() + 1, day); return n.getDate() + ' ' + MON[n.getMonth()]; })();
  const st = (k, v, cl) => '<div class="stat">' + lbl(k) + '<div class="v ' + (cl||'') + '">' + v + '</div></div>';
  return '<section class="pbar o-head" aria-label="Portfolio">' +
    '<div class="main">' + lbl('Total portfolio value') + '<div class="v">' + money(T.total) + '</div></div>' +
    '<div class="stats">' +
      st('Today', money(T.daySAR, true) + ' <small>(' + pct(T.dayPct, 2) + ')</small>', cls(T.daySAR)) +
      st('Total return', money(T.gainSAR, true) + ' <small>(' + pct(T.gainPct) + ')</small>', cls(T.gainSAR)) +
      st('Cash available', money(T.cash)) +
      st('Invested capital', money(T.invested)) +
      st('Return YTD', T.ytdPct != null ? pct(T.ytdPct) : '<button class="btn sm ghost" data-act="settings">Set start value</button>', cls(T.ytdPct)) +
      st('Next monthly contribution', contrib + ' <small>· ' + nd + '</small>') +
      st('Prices', liveText(C)) +
    '</div>' +
    '<div class="seg" role="group" aria-label="Currency"><button data-act="display" data-v="SAR" class="' + (disp === 'SAR' ? 'on' : '') + '">SAR</button><button data-act="display" data-v="USD" class="' + (disp === 'USD' ? 'on' : '') + '">USD</button></div>' +
  '</section>';
}
function alertCard(a){
  return '<article class="acard">' +
    '<header class="ah t-' + a.tone + '"><span>' + esc(a.sev) + '</span><a class="tk" href="#' + a.href + '">' + esc(a.t) + '</a></header>' +
    '<div class="ab"><div class="atitle">' + esc(a.title) + '</div>' +
      (a.text || []).map(x => '<p>' + esc(x) + '</p>').join('') +
      (a.lines && a.lines.length ? kv(a.lines) : '') +
      (a.but && a.but.length ? '<div class="but">' + lbl('But') + kv(a.but) + '</div>' : '') +
      (a.stack ? '<div class="stack">' + a.stack.map(s => '<div>' + esc(s) + '</div>').join('') + '</div>' : '') +
      '<div class="aaction">' + lbl('Action') + '<strong>' + esc(a.action) + '</strong></div>' +
    '</div>' +
    '<footer class="af"><a class="btn sm" href="#' + a.href + '">Open</a>' +
      (a.kind === 'missing' ? '<button class="btn sm pri" data-act="thesis" data-t="' + esc(a.t) + '">' + (a.title === 'NO EXIT PLAN' ? 'Add exit plan' : 'Define thesis') + '</button>' : '') +
      '<span class="grow"></span><button class="btn sm ghost" data-act="ack" data-key="' + esc(a.key) + '" data-sig="' + esc(a.sig) + '">Dismiss</button></footer>' +
  '</article>';
}
function secAttention(C){
  const act = C.alerts.filter(a => a.sev !== 'INFO' && !isAcked(a));
  const prompts = C.alerts.filter(a => a.kind === 'missing' && !isAcked(a));
  const dismissed = C.alerts.filter(a => a.sev !== 'INFO' && isAcked(a)).length;
  const weak = C.pos.filter(m => m.thesis === 'WEAKENING').length;
  const body = act.length ? '<div class="agrid">' + act.map(alertCard).join('') + '</div>'
    : '<div class="calm"><div class="t">NO ACTION REQUIRED TODAY</div><p>' + (weak ? 'Nothing crossed a trigger. ' + weak + ' thesis ' + (weak > 1 ? 'are' : 'is') + ' marked weakening; see positions.' : 'Portfolio theses remain intact. Nothing crossed a buy, sell or size condition.') + '</p></div>';
  return '<section class="block o-attn">' + secHead('What needs my attention?', act.length ? act.length + ' material ' + (act.length > 1 ? 'events' : 'event') : '', dismissed ? '<a class="btn sm ghost" href="#inbox">' + dismissed + ' dismissed</a>' : '') + body +
    (prompts.length ? '<div class="sh" style="margin-top:4px"><h2 style="font-size:13px">Missing investment memory</h2></div><div class="agrid">' + prompts.map(alertCard).join('') + '</div>' : '') + '</section>';
}
function secBuyOps(C){
  const items = [...C.pos.filter(m => m.status === 'ADD' || m.status === 'ACCUMULATE').map(m => ({t:m.t, href:'pos-'+m.t, s:m.status, tone:'g', v:usd(m.price), n:'Add condition met'})),
    ...C.watch.filter(m => m.ver || m.wstatus === 'APPROACHING BUY').map(m => ({t:m.t, href:'watch-'+m.t, s: m.ver ? VER[m.ver.code].verdict : 'APPROACHING BUY', tone: m.ver ? VER[m.ver.code].tone : 'y', v:usd(m.price), n: m.ver ? 'Buy price reached' : pct(m.dist, 1, false) + ' from ' + usd(m.buy)}))];
  return '<section class="block o-buyops">' + secHead('Buy opportunities') + (items.length ? '<div class="cats">' + items.map(i => '<div class="cat" style="grid-template-columns:64px 76px 1fr"><a class="tk" href="#' + i.href + '">' + esc(i.t) + '</a><span class="mono">' + i.v + '</span><span>' + chip(i.s, i.tone) + ' <span class="muted">' + esc(i.n) + '</span></span></div>').join('') + '</div>' : '<p class="muted" style="margin:0">Nothing is at or near a buy price.</p>') + '</section>';
}
function posCard(m){
  const collapsed = !!ui.collapsed[m.t], pl = m.plan, th = m.p.thesis || {};
  const head = '<div class="pc-head"><a class="tk big" href="#pos-' + m.t + '">' + esc(m.t) + '</a><span class="mono">' + px(m.price, m.cur) + '</span><span class="mono ' + cls(m.dayPct) + '" style="font-size:12.5px">' + pct(m.dayPct) + '</span><span class="grow"></span>' + sChip(m.status) +
    '<button class="icon" data-act="collapse" data-t="' + m.t + '" aria-expanded="' + !collapsed + '" aria-label="' + (collapsed ? 'Expand' : 'Collapse') + ' ' + m.t + '">' + (collapsed ? '+' : '−') + '</button></div>' +
    '<div class="pc-co">' + esc(m.p.company || '') + '</div>';
  if (collapsed) return '<article class="pcard">' + head + '<div class="pc-mini"><span>' + lbl('Return') + '<span class="mono ' + cls(m.gainPct) + '">' + pct(m.gainPct) + '</span></span><span>' + lbl('Weight') + '<span class="mono">' + pct(m.weight, 1, false) + '</span></span><span>' + lbl('Thesis') + tChip(m.thesis) + '</span><span>' + lbl('Action') + aChip(m.rec.action) + '</span></div></article>';
  const st = (k, v, c) => '<div>' + lbl(k) + '<div class="v ' + (c||'') + '">' + v + '</div></div>';
  const row = (k, v) => '<div class="row"><span class="lbl">' + esc(k) + '</span><span class="val">' + v + '</span></div>';
  return '<article class="pcard">' + head +
    '<div class="pc-stats">' + st('Position value', money(m.valueSAR)) + st('Return', pct(m.gainPct), cls(m.gainPct)) + st('Weight', pct(m.weight, 1, false) + (m.targetW != null ? ' <span class="muted" style="font-size:11.5px">/ ' + m.targetW + '%</span>' : '')) +
      st('Avg cost', px(m.avg, m.cur)) + st('Thesis', tChip(m.thesis)) + st('Conviction', esc(m.p.conviction || '—')) + '</div>' +
    (m.hasThesis ? '<div class="pc-body">' +
      row('Why I own it', esc(th.whyOwn || th.summary)) +
      row('Add below', num(pl.addBelow) != null ? '<span class="mono">' + usd(num(pl.addBelow)) + '</span> <span class="muted">subject to fundamentals</span>' : '—') +
      row('Review above', num(pl.reviewPrice) != null ? '<span class="mono">' + usd(num(pl.reviewPrice)) + '</span>' : '—') +
      row('Sell if', esc(sellIfText(m))) +
      row('Next catalyst', esc(catalystText(m.nc))) +
    '</div>' : '<div class="nothesis"><span>You own ' + esc(m.t) + ' but there is no stored investment thesis. Define why you own it and what would make you sell.</span><span><button class="btn sm pri" data-act="thesis" data-t="' + m.t + '">Define thesis</button></span></div>') +
    '<div class="pc-foot">' + lbl('Next action') + '<strong>' + esc(m.rec.action) + '</strong><span class="grow"></span><a class="btn sm ghost" href="#pos-' + m.t + '">Open</a></div>' +
  '</article>';
}
function secPositions(C){
  const order = {'THESIS BREAK':0,'TARGET REACHED':1,'REVIEW':2,'ADD':3,'ACCUMULATE':4,'WATCH':5,'HOLD':6};
  const list = C.pos.slice().sort((a,b) => order[a.status] - order[b.status] || b.weight - a.weight);
  const allC = list.length && list.every(m => ui.collapsed[m.t]);
  return '<section class="block o-pos">' + secHead('My positions — lifecycle', list.length + ' holdings', list.length ? '<button class="btn sm ghost" data-act="collapse-all">' + (allC ? 'Expand all' : 'Collapse all') + '</button>' : '') +
    (list.length ? '<div class="pgrid">' + list.map(posCard).join('') + '</div>' : '<p class="muted" style="margin:0">No open positions.</p>') + '</section>';
}
const WSORTS = {closest:'Closest to buy price', upside:'Largest analyst upside', cagr3y:'Largest 3Y expected CAGR', cagr5y:'Largest 5Y expected CAGR', conviction:'Highest conviction', valuation:'Best valuation', growth:'Highest revenue growth', catalyst:'Upcoming catalyst', recent:'Recently added', drawdown:'Largest drawdown'};
function sortWatch(list){
  const nl = v => v == null ? Infinity : v, key = ui.wsort;
  const f = {
    closest: (a,b) => nl(a.dist) - nl(b.dist), upside: (a,b) => nl(-a.upside) - nl(-b.upside), cagr3y: (a,b) => nl(-a.cagr3y) - nl(-b.cagr3y), cagr5y: (a,b) => nl(-a.cagr5y) - nl(-b.cagr5y),
    conviction: (a,b) => (CONV[b.w.conviction]||0) - (CONV[a.w.conviction]||0), valuation: (a,b) => nl(a.updatedBuy ? a.price / a.updatedBuy : null) - nl(b.updatedBuy ? b.price / b.updatedBuy : null),
    growth: (a,b) => nl(-num(a.f.revGrowth)) - nl(-num(b.f.revGrowth)), catalyst: (a,b) => nl(pd(a.nc && a.nc.date)) - nl(pd(b.nc && b.nc.date)),
    recent: (a,b) => (b.w.addedAt||'').localeCompare(a.w.addedAt||''), drawdown: (a,b) => nl(a.drawdown) - nl(b.drawdown)
  }[key] || ((a,b) => 0);
  return list.slice().sort(f);
}
function proxBar(m){
  if (m.dist == null) return '';
  const reached = m.dist <= 0, close = reached ? 10 : Math.max(0, Math.round((1 - m.dist / 30) * 10));
  const tone = reached ? 't-g' : m.dist <= 5 ? 't-y' : '';
  return '<div class="prox"><div class="bar ' + tone + '" aria-hidden="true">' + Array.from({length:10}, (_, i) => '<i class="' + (i < close ? 'f' : '') + '"></i>').join('') + '</div><span class="mono">' + (reached ? 'AT BUY PRICE' : pct(m.dist, 1, false) + ' AWAY') + '</span></div>';
}
function watchCard(m){
  const w = m.w, st = m.ver ? VER[m.ver.code].verdict : m.wstatus;
  const tone = m.ver ? VER[m.ver.code].tone : WSTATUS_TONE[m.wstatus];
  const n = (k, v) => '<div>' + lbl(k) + '<div class="v">' + v + '</div></div>';
  const missed = m.wstatus === 'MISSED ENTRY' ? '<div class="pc-body" style="border-top:1px solid var(--line)"><div class="row"><span class="lbl">Old buy</span><span class="val mono">' + usd(m.buy) + '</span></div><div class="row"><span class="lbl">Updated buy</span><span class="val"><span class="mono">' + usd(m.updatedBuy) + '</span> <span class="muted">' + (num(w.updatedBuy) != null ? 'your reassessment' : 'from 3Y model at ' + m.hurdle + '% hurdle') + '</span></span></div><div class="row"><span class="lbl">Status</span><span class="val">' + chip(m.missed.verdict, m.missed.tone) + '</span></div></div>' : '';
  return '<article class="pcard wcard">' +
    '<div class="pc-head"><a class="tk big" href="#watch-' + m.t + '">' + esc(m.t) + '</a><span class="pc-co" style="padding:0">' + esc(w.company || '') + '</span><span class="grow"></span>' + chip(st, tone) + '</div>' +
    '<div class="nums">' + n('Current', usd(m.price)) + n('Buy below', usd(m.buy)) + n('Strong buy', usd(m.strong)) + n('Target position', m.targetW != null ? m.targetW + '%' : '—') + '</div>' +
    proxBar(m) +
    '<div class="pc-body"><div class="row"><span class="lbl">Why I want it</span><span class="val">' + esc(w.whyWant || '—') + '</span></div>' +
    '<div class="row"><span class="lbl">3Y CAGR</span><span class="val mono">' + pct(m.cagr3y) + '</span></div>' +
    '<div class="row"><span class="lbl">Next catalyst</span><span class="val">' + esc(catalystText(m.nc)) + '</span></div></div>' + missed +
  '</article>';
}
function secWatch(C){
  const list = sortWatch(C.watch);
  const sel = '<label class="wtools"><span class="lbl">Sort</span><select id="wsort">' + Object.entries(WSORTS).map(([k,v]) => '<option value="' + k + '"' + (k === ui.wsort ? ' selected' : '') + '>' + v + '</option>').join('') + '</select></label>';
  return '<section class="block o-watch">' + secHead('Watchlist — waiting for my price', 'Which stocks am I closest to buying?', sel + '<button class="btn sm" data-act="watch-edit">Add company</button>') +
    (list.length ? '<div class="pgrid">' + list.map(watchCard).join('') + '</div>' : '<p class="muted" style="margin:0">Add the companies you want to own and the price you want to pay.</p>') + '</section>';
}
function secAlloc(C){
  const amt = ui.alloc || 5000, r = allocate(C, amt);
  const presets = [1000, 3000, 5000, 10000];
  const rows = r.rows.map(c => '<div class="arow ' + (c.amt ? '' : 'zero') + '"><a class="tk" href="#' + (c.owned ? 'pos-' : 'watch-') + c.t + '">' + esc(c.t) + '</a><span class="amt">SAR ' + fmtN(c.amt, 0) + '</span><span class="muted">' + esc(c.amt ? c.note : c.note.replace(/^Wait for/, 'WAIT FOR')) + '</span></div>').join('') +
    '<div class="arow cash"><strong>CASH</strong><span class="amt">SAR ' + fmtN(r.cash, 0) + '</span><span class="muted">' + (r.cash ? 'Held until conditions are met' : '') + '</span></div>';
  return '<section class="block o-alloc">' + secHead('Where should my next SAR ' + fmtN(amt, 0) + ' go?', 'Holdings and watchlist compete for the same money') +
    '<div class="amts" role="group" aria-label="Amount">' + presets.map(p => '<button class="btn sm' + (p === amt ? ' pri' : '') + '" data-act="alloc" data-v="' + p + '">SAR ' + fmtN(p, 0) + '</button>').join('') +
      '<input type="number" id="alloc-custom" min="100" step="100" placeholder="Custom" value="' + (presets.includes(amt) ? '' : amt) + '" aria-label="Custom amount in SAR"></div>' +
    '<div class="alloc"><div class="alist">' + rows + '</div><div class="why">' + lbl('Why') + '<p>' + esc(r.why) + '</p>' +
      '<p class="muted" style="font-size:12.5px">Money goes only to positions whose add or buy condition is met with fundamentals verified, or to holdings below target weight whose expected 3Y CAGR clears your hurdle. Each is capped by room to its target weight. Cash competes as an option at your ' + (num(C.set.hurdle) || 15) + '% hurdle.</p></div></div></section>';
}
function secBoard(C){
  const rows = [...C.pos.map(m => ({t:m.t, href:'pos-'+m.t, owned:true, price:m.price, buy:num(m.plan.addBelow), cagr:m.cagr3y, cagr5:m.cagr5y, up:m.analystTarget && m.price ? (m.analystTarget / m.price - 1) * 100 : null, thesis:m.thesis, action:m.rec.action, tone:ACTION_TONE[m.rec.action]})),
    ...C.watch.map(m => ({t:m.t, href:'watch-'+m.t, owned:false, price:m.price, buy:m.buy, cagr:m.cagr3y, cagr5:m.cagr5y, up:m.upside, thesis:m.thesis, action: m.ver ? VER[m.ver.code].verdict : m.wstatus === 'APPROACHING BUY' ? 'WATCH' : m.wstatus === 'MISSED ENTRY' ? m.missed.verdict : 'WAIT', tone: m.ver ? VER[m.ver.code].tone : m.wstatus === 'APPROACHING BUY' ? 'y' : 'n'}))]
    .sort((a,b) => (b.cagr ?? -999) - (a.cagr ?? -999));
  return '<section class="block o-board">' + secHead('My opportunity board', 'The underlying numbers, no composite score') +
    '<div class="tscroll"><table class="tbl"><thead><tr><th>Ticker</th><th></th><th class="n">Price</th><th class="n">My buy</th><th class="n">Distance</th><th class="n">Analyst upside</th><th class="n">3Y CAGR</th><th class="n">5Y CAGR</th><th>Thesis</th><th>Action</th></tr></thead><tbody>' +
    rows.map(r => '<tr><td><a class="tk" href="#' + r.href + '">' + esc(r.t) + '</a></td><td class="muted" style="font-size:12px">' + (r.owned ? 'Owned' : 'Watchlist') + '</td><td class="n">' + usd(r.price) + '</td><td class="n">' + usd(r.buy) + '</td><td class="n">' + (r.buy && r.price ? pct((r.price - r.buy) / r.price * 100, 1, false) : '—') + '</td><td class="n ' + cls(r.up) + '">' + pct(r.up, 0) + '</td><td class="n">' + pct(r.cagr, 0, false) + '</td><td class="n">' + pct(r.cagr5, 0, false) + '</td><td>' + tChip(r.thesis) + '</td><td>' + chip(r.action, r.tone, 'nd') + '</td></tr>').join('') +
    '</tbody></table></div></section>';
}
function catalystList(C){
  const out = [];
  C.pos.forEach(m => { const nc = m.nc; if (nc && nc.date) out.push({t:m.t, href:'pos-'+m.t, date:nc.date, label:nc.label, k:'Catalyst', owned:true});
    if (m.plan.nextReview) out.push({t:m.t, href:'pos-'+m.t, date:m.plan.nextReview, label:'Scheduled thesis review', k:'Review', owned:true}); });
  C.watch.forEach(m => { const nc = m.nc; if (nc && nc.date) out.push({t:m.t, href:'watch-'+m.t, date:nc.date, label:nc.label, k:'Watchlist', owned:false}); });
  return out.map(x => Object.assign(x, {days: daysBetween(todayISO(), x.date)})).filter(x => x.days != null && x.days >= -3).sort((a,b) => a.days - b.days);
}
function secCatalysts(C){
  const list = catalystList(C).slice(0, 12);
  return '<section class="block o-cat">' + secHead('Upcoming catalysts') + (list.length ? '<div class="cats">' + list.map(x => '<div class="cat"><span class="mono">' + fmtDate(x.date).replace(/ \d{4}$/, '') + '</span><a class="tk" href="#' + x.href + '">' + esc(x.t) + '</a><span>' + esc(x.label) + '</span><span class="k muted" style="font-size:12px">' + (x.days === 0 ? 'Today' : x.days < 0 ? Math.abs(x.days) + 'd ago' : x.days + ' days') + ' · ' + x.k + '</span></div>').join('') + '</div>' : '<p class="muted" style="margin:0">No catalysts recorded. Add a next catalyst to each thesis.</p>') + '</section>';
}
function hbars(rows, max){ return rows.map(r => '<div class="hb"><span>' + esc(r[0]) + '</span><span class="track"><i style="width:' + Math.min(100, r[1] / (max || 100) * 100).toFixed(1) + '%"></i></span><span class="mono">' + r[1].toFixed(1) + '%</span></div>').join(''); }
function secXray(C){
  const byW = C.pos.slice().sort((a,b) => b.weight - a.weight);
  const top3 = sum(byW.slice(0, 3).map(m => m.weight)), cashPct = C.totals.total ? C.cash / C.totals.total * 100 : 0;
  const clsW = {}; C.pos.forEach(m => (m.p.classifications && m.p.classifications.length ? m.p.classifications : ['UNCLASSIFIED']).forEach(c => clsW[c] = (clsW[c]||0) + m.weight));
  const themes = {}; C.pos.forEach(m => (m.p.themes || []).forEach(t => { (themes[t] = themes[t] || {w:0, list:[]}); themes[t].w += m.weight; themes[t].list.push(m.t); }));
  const dup = Object.entries(themes).filter(([,v]) => v.list.length > 1).sort((a,b) => b[1].w - a[1].w);
  const usdW = sum(C.pos.filter(m => m.cur !== 'SAR').map(m => m.weight));
  const gaps = [...C.pos.filter(m => !m.hasThesis).map(m => m.t + ': no thesis'), ...C.pos.filter(m => m.hasThesis && num(m.plan.reviewPrice) == null && !(m.p.conditions||[]).some(c => c.type !== 'add')).map(m => m.t + ': no exit plan'),
    ...C.pos.filter(m => m.hasThesis && (!m.f.asOf || daysBetween(m.f.asOf, todayISO()) > 120)).map(m => m.t + ': current reality not updated in 120+ days')];
  return '<section class="block o-xray">' + secHead('Portfolio X-ray') + '<div class="xgrid">' +
    '<div class="xcard"><h3>Concentration</h3>' + kv([['Largest position', byW[0] ? byW[0].t + ' · ' + pct(byW[0].weight, 1, false) : '—'], ['Top 3 weight', pct(top3, 1, false)], ['Holdings', String(C.pos.length)], ['Cash', pct(cashPct, 1, false)], ['USD exposure', pct(usdW, 1, false)]]) + '</div>' +
    '<div class="xcard"><h3>Why I own what I own</h3>' + hbars(Object.entries(clsW).sort((a,b) => b[1] - a[1]), Math.max(...Object.values(clsW), 1)) + '<p class="fhint">Weight by entry classification. A position with several classifications counts in each.</p></div>' +
    '<div class="xcard"><h3>Duplicate exposure</h3>' + (dup.length ? dup.map(([t,v]) => '<div class="gap"><strong>' + esc(t) + '</strong><span class="mono">' + pct(v.w, 1, false) + '</span><span class="muted">' + v.list.join(', ') + '</span></div>').join('') : '<p class="fhint">No theme appears in more than one holding.</p>') + '</div>' +
    '<div class="xcard"><h3>Memory gaps</h3>' + (gaps.length ? gaps.map(g => '<div class="gap">' + esc(g) + '</div>').join('') : '<p class="fhint">Every holding has a thesis, an exit plan and recent data.</p>') + '</div>' +
  '</div></section>';
}
function secMacro(C){
  const s = C.set;
  return '<section class="block o-macro">' + secHead('Market / macro context', '', '<button class="btn sm ghost" data-act="settings">Edit</button>') + '<div class="xgrid">' +
    '<div class="xcard">' + kv([['USD/SAR', fmtN(C.fx, 4)], [s.benchmark || 'Benchmark', num(s.benchmarkYtd) != null ? pct(num(s.benchmarkYtd)) + ' YTD' : '—'], ['My portfolio', pct(C.totals.ytdPct) + ' YTD'], ['Hurdle / minimum CAGR', (num(s.hurdle) ?? '—') + '% / ' + (num(s.minCagr) ?? '—') + '%']]) + '</div>' +
    '<div class="xcard" style="grid-column:span 2;min-width:0"><h3>My notes</h3><div class="prose">' + (s.marketNotes ? mdLite(s.marketNotes) : '<p class="muted">Write down the macro view that matters to your holdings, such as rates, the USD peg or AI capex.</p>') + '</div></div>' +
  '</div></section>';
}

/* ------------------------------------------------------------------ */
/* Position detail                                                     */
/* ------------------------------------------------------------------ */
function historyFor(t){ return Object.entries(S.history).map(([id, h]) => Object.assign({id}, h)).filter(h => h.ticker === t).sort((a,b) => (a.at||'').localeCompare(b.at||'') || (a.ts||0) - (b.ts||0)); }
function originalOf(t, p){ const h = historyFor(t).find(x => x.kind === 'THESIS' && x.snapshot); return h ? Object.assign({at:h.at, price:h.price}, h.snapshot) : null; }
function renderPos(C, t){
  const m = C.pos.find(x => x.t === t);
  if (!m) { const w = C.watch.find(x => x.t === t); if (w) return renderWatchDetail(C, t); return renderClosedOrMissing(t); }
  const p = m.p, th = p.thesis || {}, pl = m.plan, f = m.f;
  const orig = originalOf(t, p);
  const d = (k, v, extra) => '<div class="' + (extra||'') + '">' + lbl(k) + '<div class="v' + (extra && extra.includes('t') ? ' txt' : '') + '">' + v + '</div></div>';
  const dash = '<div class="dash">' +
    d('Bought', fmtDate(m.start)) + d('Avg cost', px(m.avg, m.cur)) + d('Current', px(m.price, m.cur)) + d('Return', '<span class="' + cls(m.gainPct) + '">' + pct(m.gainPct) + '</span>') +
    '<div class="wide">' + lbl('Why I own it') + '<div class="v txt">' + esc(th.whyOwn || th.summary || 'Not recorded') + '</div></div>' +
    d('Thesis', tChip(m.thesis)) + d('Conviction', esc(p.conviction || '—')) + d('Next catalyst', esc(catalystText(m.nc)), 'x t') +
    d('Add below', num(pl.addBelow) != null ? usd(num(pl.addBelow)) + ' <span class="muted" style="font-size:11.5px">subject to fundamentals</span>' : '—') + d('Review above', usd(num(pl.reviewPrice))) +
    '<div class="wide">' + lbl('Sell if') + '<div class="v txt">' + esc(sellIfText(m)) + '</div></div>' +
    d('Target position', m.targetW != null ? m.targetW + '%' : '—') + d('Current position', pct(m.weight, 1, false)) + d('Next action', aChip(m.rec.action)) +
  '</div>';
  const lv = (k, v, note, mine) => '<div class="lv' + (mine ? ' mine' : '') + '"><span class="lbl">' + esc(k) + '</span><span class="v">' + v + '</span><span class="k">' + esc(note) + '</span></div>';
  const sellCond = (p.conditions || []).find(c => c.type !== 'add' && c.category === 'FAILURE');
  const levels = '<div class="levels">' + lv('My buy', usd(num(pl.addBelow)), 'my add level', 1) + lv('My strong buy', usd(num(pl.strongBuy)), 'high conviction', 1) + lv('My review price', usd(num(pl.reviewPrice)), 'review for sale, not a sell order', 1) +
    lv('My sell condition', sellCond ? fmtMetric(sellCond.metric, num(sellCond.value)) : '—', sellCond ? metricLabel(sellCond.metric) : 'none recorded', 1) +
    lv('Analyst 12M', usd(m.analystTarget), 'consensus target') + lv('Model fair value', usd(m.fair), 'my intrinsic value today') + lv('Model 3Y', usd(m.v3y), 'expected value in 3 years') + lv('Model 5Y', usd(m.v5y), 'expected value in 5 years') + '</div>';

  const best = bestAlt(m, C);
  const eng = kv([['Current price', px(m.price, m.cur)], ['Average cost', px(m.avg, m.cur)], ['Total return', pct(m.gainPct) + ' · ' + money(m.gainSAR, true)], ['Annualized return', m.annPct != null ? pct(m.annPct) : (m.days != null ? 'Held ' + heldFor(m.days) + ', too short' : '—')],
    ['Portfolio weight', pct(m.weight, 1, false) + (m.targetW != null ? ' (target ' + m.targetW + '%, max ' + m.maxW + '%)' : '')], ['Original target', usd(num(pl.originalTarget) ?? (orig && orig.plan ? num(orig.plan.reviewPrice) : null))],
    ['Analyst consensus', usd(m.analystTarget) + (m.d.analystTargetChg != null ? ' (' + pct(m.d.analystTargetChg, 0) + ' since purchase)' : '')], ['Intrinsic value (updated)', usd(m.fair) + (m.priceToFair != null ? ' · price ' + pct(m.priceToFair, 0) + ' vs value' : '')],
    ['Expected 3Y CAGR', pct(m.cagr3y) + (m.minCagr != null ? ' (minimum ' + m.minCagr + '%)' : '')], ['Tax', C.set.taxEnabled ? 'Configured: ' + (num(C.set.taxShort)||0) + '% short / ' + (num(C.set.taxLong)||0) + '% long' : 'Not configured'],
    ['Best alternative', best ? best.t + ' · ' + pct(best.cagr) + ' 3Y CAGR' : '—'], ['Thesis status', (m.thesis || 'Not set') + (m.autoThesis && m.autoThesis !== m.thesis ? ' (data suggests ' + m.autoThesis + ')' : '')]]);
  const rec = '<div class="rec"><div class="lbl">Recommendation</div><div class="act">' + esc(m.rec.action) + '</div><div class="ladder" aria-hidden="true">' + ACTIONS.map(a => '<span class="' + (a === m.rec.action ? 'on' : '') + '">' + a + '</span>').join('') + '</div><ul>' + m.rec.reasons.map(r => '<li>' + esc(r) + '</li>').join('') + '</ul></div>';
  const lessons = m.lessons.map(l => '<div class="lesson"><b>From your mistake library · ' + esc(l[0]) + '</b>' + esc(l[1]) + '</div>').join('');

  const verBlock = m.ver ? '<div class="panel-b"><h3>Buy-more check ' + chip(VER[m.ver.code].pverdict, VER[m.ver.code].tone) + '</h3><div class="stack">' + VER[m.ver.code].plines.join('<br>') + '</div>' + checksHTML(m.ver) + '</div>' : '';

  return '<div class="crumbs"><a href="#home">← Home</a></div>' +
    '<div class="dhead"><h1>' + esc(t) + '</h1><span class="co">' + esc(p.company || '') + '</span>' + sChip(m.status) + (p.classifications || []).map(c => chip(c, 'n', 'nd')).join(' ') + '</div>' +
    '<div class="btns" style="padding-block:8px 14px"><button class="btn pri" data-act="thesis" data-t="' + t + '">' + (m.hasThesis ? 'Revise thesis & plan' : 'Define thesis') + '</button><button class="btn" data-act="fund" data-t="' + t + '">Update current reality</button><button class="btn" data-act="tx" data-side="BUY" data-t="' + t + '">Record buy</button><button class="btn" data-act="tx" data-side="SELL" data-t="' + t + '">Record sell</button><button class="btn" data-act="note" data-t="' + t + '">Add note</button></div>' +
    dash +
    '<section class="block">' + secHead('Price targets', 'Each number means something different') + levels + '</section>' +
    '<section class="block">' + secHead('Sell decision engine', 'Rising is never a reason to sell by itself') + '<div class="two"><div class="panel-b">' + eng + '</div><div style="display:flex;flex-direction:column;gap:10px">' + rec + lessons + '</div></div>' + verBlock + '</section>' +
    '<section class="block">' + secHead('Then vs now', f.asOf ? 'Current reality as of ' + fmtDate(f.asOf) : 'No current-reality update yet', '<button class="btn sm" data-act="fund" data-t="' + t + '">Update after earnings</button>') + thenNow(m, orig) + '</section>' +
    '<section class="block">' + secHead('Exit plan & alerts', 'Every condition is checked each time the ledger opens', '<button class="btn sm" data-act="thesis" data-t="' + t + '" data-focus="conds">Edit conditions</button>') + condsHTML(m) + '</section>' +
    '<section class="block">' + secHead('Why I bought this') + thesisHTML(th) + '</section>' +
    '<section class="block">' + secHead('Ask my analyst', 'Answers from your own recorded framework') + aiHTML(t, ['Should I sell ' + t + '?', 'Should I add to ' + t + '?', 'What has changed since I bought ' + t + '?']) + '</section>' +
    '<section class="block">' + secHead('Decision history', 'What I believed at each point. Entries are never overwritten') + timeline(historyFor(t).reverse()) + '</section>' +
    '<section class="block">' + secHead('Transactions') + txTable(m.L.txs) + '</section>';
}
function checksHTML(v){
  const mk = ok => ok === true ? '<span class="mk t-g">OK</span>' : ok === false ? '<span class="mk t-r">FAIL</span>' : '<span class="mk t-n">NO DATA</span>';
  return '<div class="checks">' + v.checks.map(c => '<div class="ck">' + mk(c.ok) + '<span>' + esc(c.q) + '</span><span class="mono" style="font-size:12.5px">' + esc(c.v) + '</span></div>').join('') + '</div>' +
    (v.unknown > 3 ? '<p class="fhint">' + v.unknown + ' checks have no data. Update current reality to complete the check.</p>' : '');
}
function thenNow(m, orig){
  const o = orig || {thesis:m.p.thesis || {}, expectations:m.p.expectations || [], at:m.start, price:m.base.price};
  const oth = o.thesis || {};
  const expOrig = (o.expectations && o.expectations.length ? o.expectations : m.p.expectations || []);
  const exps = expOrig.map(e => '<div class="exp"><span>' + esc(e.label || metricLabel(e.metric)) + '</span><span class="mono">' + (e.metric ? (OPS[e.op] || e.op) + ' ' + fmtMetric(e.metric, num(e.value)) : 'Yes') + '</span></div>').join('');
  const now = m.expect.map(x => { const tone = x.ok === true ? 'g' : x.ok === false ? 'r' : 'n'; return '<div class="exp"><span>' + esc(x.e.label || metricLabel(x.e.metric)) + '</span><span class="mono">' + (x.e.metric ? fmtMetric(x.e.metric, x.v) : '') + ' ' + chip(x.ok === true ? 'MET' : x.ok === false ? 'NOT MET' : 'NO DATA', tone, 'nd') + '</span></div>'; }).join('');
  const f = m.f;
  const reality = [['Revenue growth', f.revGrowth != null ? f.revGrowth + '%' : '—'], ['Margin', f.marginTrend || '—'], ['Guidance', f.guidance || '—'], ['Revenue estimates vs purchase', pct(m.d.revEstChg)], ['EPS estimates vs purchase', pct(m.d.epsEstChg)], ['Dilution since purchase', pct(m.d.dilution)]];
  const nws = newsOf(f).slice(0, 3);
  return '<div class="tvn"><div><div class="col-h">Then</div>' + kv([['Bought', fmtDate(o.at || m.start)], ['Price', usd(num(o.price) ?? num(m.base.price) ?? m.avg)]]) +
      lbl('Why I bought') + '<div>' + esc(oth.summary || oth.whyOwn || 'Not recorded') + '</div>' +
      lbl('Original expectations') + (exps || '<p class="fhint">No measurable expectations recorded.</p>') +
      (oth.base || oth.v5y ? lbl('5Y thesis') + '<div>' + esc(oth.base || '') + (oth.v5y ? ' <span class="mono muted">5Y value ' + usd(num(oth.v5y)) + '</span>' : '') + '</div>' : '') +
    '</div><div><div class="col-h">Now</div>' + kv([['Date', f.asOf ? fmtDate(f.asOf) : '—'], ['Price', usd(m.price)]]) +
      lbl('Expectations vs reality') + (now || '<p class="fhint">Add expectations with a metric to compare them automatically.</p>') +
      lbl('Current reality') + kv(reality) + (f.auto ? kv([['P/E (TTM)', f.auto.peTTM != null ? (+f.auto.peTTM).toFixed(1) + 'x' : '—'], ['52-week range', usd(num(f.auto.low52)) + ' – ' + usd(num(f.high52))], ['Analysts rating buy', f.auto.analystBuyPct != null ? f.auto.analystBuyPct + '%' + (f.auto.analystBuyPct3m != null ? ' (3 months ago ' + f.auto.analystBuyPct3m + '%)' : '') : '—'], ['Next earnings', f.auto.nextEarnings ? fmtDate(f.auto.nextEarnings) : '—']]) + '<div class="fhint">Updated automatically ' + fmtDate(f.auto.asOf) + (f.auto.fields.length ? ' · auto: ' + f.auto.fields.map(metricLabel).join(', ') : '') + '</div>' : '') +
      (nws.length ? lbl('Latest news') + nws.map(n => '<div class="fhint">' + fmtDate(n.date) + ' · ' + (n.url ? '<a href="' + esc(n.url) + '" target="_blank" rel="noopener">' + esc(n.headline) + '</a>' : esc(n.headline)) + (n.source ? ' <span class="muted">(' + esc(n.source) + ')</span>' : '') + '</div>').join('') : '') +
    '</div><div class="foot">' + lbl('Thesis') + tChip(m.thesis) + (m.autoThesis ? '<span class="muted" style="font-size:12.5px">Data suggests ' + m.autoThesis + ' from ' + m.expect.filter(x => x.ok !== null).length + ' measurable expectations</span>' : '') + '<span class="grow"></span><button class="btn sm" data-act="thesis" data-t="' + m.t + '">Change status</button></div></div>';
}
function condsHTML(m){
  const st = r => r.ok === true ? chip(r.c.type === 'add' ? 'MET' : 'TRIGGERED', r.c.type === 'add' ? 'g' : r.c.category === 'FAILURE' ? 'r' : 'o') : r.near ? chip('NEAR', 'y') : r.ok === false ? chip('NOT MET', 'n', 'nd') : chip('NO DATA', 'n', 'nd');
  const row = r => '<div class="cd"><span class="lbl">' + esc(CATS[r.c.category] || r.c.category) + '</span><span><div>' + esc(r.c.label || condText(r.c)) + '</div><div class="meta">' + esc(condText(r.c)) + ' · now ' + esc(fmtMetric(r.c.metric, r.v)) + (r.c.and && r.c.and.metric ? ' / ' + esc(fmtMetric(r.c.and.metric, r.v2)) : '') + (r.c.auto ? ' · from plan' : '') + (r.c.action ? ' · then ' + esc(r.c.action) : '') + '</div></span>' + st(r) + '</div>';
  const ex = m.conds.filter(r => r.c.type !== 'add'), ad = m.conds.filter(r => r.c.type === 'add');
  return '<div class="two"><div class="panel-b"><h3>Exit conditions</h3><div class="conds">' + (ex.length ? ex.map(row).join('') : '<p class="fhint">No exit conditions. Add a review price and at least one thesis-failure condition.</p>') + '</div></div>' +
    '<div class="panel-b"><h3>Add / buy-more conditions</h3><div class="conds">' + (ad.length ? ad.map(row).join('') : '<p class="fhint">No add conditions recorded.</p>') + '</div><p class="fhint">When a price condition is met, the ledger checks estimates, guidance, dilution, debt, news and thesis before calling it satisfied.</p></div></div>';
}
function thesisHTML(th){
  const rows = [['Investment thesis', th.summary], ['Expected holding period', th.holdingPeriod], ['Expected revenue growth', th.expRevGrowth], ['Expected earnings / FCF growth', th.expEarnGrowth], ['Expected catalysts', th.catalysts], ['Expected valuation', th.expValuation],
    ['Expected value 1Y / 3Y / 5Y', [th.v1y, th.v3y, th.v5y].some(x => num(x) != null) ? [th.v1y, th.v3y, th.v5y].map(x => usd(num(x))).join(' / ') : ''], ['Bull case', th.bull], ['Base case', th.base], ['Bear case', th.bear], ['Return I originally expected', th.expectedReturn],
    ['Main reason I could be wrong', th.wrongReason], ['What would make me sell', th.sellReason], ['When I intended to sell', th.intendedSell]].filter(r => r[1]);
  if (!rows.length) return '<p class="muted" style="margin:0">Nothing recorded yet. The ledger will not invent your reasons. Write them down in your own words.</p>';
  return '<div class="panel-b"><dl class="kv" style="grid-template-columns:minmax(150px,220px) 1fr;gap:8px 16px">' + rows.map(r => '<dt>' + esc(r[0]) + '</dt><dd class="txt">' + esc(r[1]) + '</dd>').join('') + '</dl></div>';
}
function aiHTML(key, quick){
  const s = ai[key];
  if (!aiOn()) return '<p class="muted" style="margin:0">' + (mode === 'local' ? 'Sign in to ask the analyst. It answers from your own recorded thesis and rules.' : 'The analyst is not enabled on this server.') + '</p>';
  return '<div class="ai"><div class="qs">' + quick.map(q => '<button class="btn sm" data-act="ask" data-t="' + esc(key) + '" data-q="' + esc(q) + '"' + (s && s.busy ? ' disabled' : '') + '>' + esc(q) + '</button>').join('') + '</div>' +
    '<form data-ask="' + esc(key) + '"><input type="text" id="ask-' + esc(key) + '" placeholder="Ask about this decision…" aria-label="Question"><button class="btn" type="submit"' + (s && s.busy ? ' disabled' : '') + '>Ask</button>' + '' + '</form>' +
    (s ? '<div class="lbl">' + esc(s.q) + '</div><div class="out" id="ai-' + esc(key) + '">' + (s.text ? mdLite(s.text) : (s.busy ? '<span class="muted">Reading your ledger and thinking. This can take up to a minute…</span>' : '')) + '</div>' + (s.err ? '<p class="fhint">' + esc(aiErr(s.err)) + '</p>' : '') : '') + '</div>';
}
function aiErr(code){ return ({not_configured:'The analyst is not set up on this server yet.', offline:'You are offline. Try again when connected.', unauthorized:'Sign in again to ask the analyst.', invalid_json:'The draft came back in the wrong format. Try again.', rate_limited:'You have reached today\'s question limit. Try again tomorrow.', cancelled:'Stopped.', refused:'Claude declined this question. Rephrase it.', prompt_too_large:'Too much data to send. Ask about one position at a time.', session_expired:'Sign in again to ask Claude.'})[code] || 'The answer did not complete. Try again.'; }
function timeline(list){
  if (!list.length) return '<p class="muted" style="margin:0">No decisions recorded yet.</p>';
  const tone = {THESIS:'b', BUY:'g', ADD:'g', SELL:'o', CLOSE:'n', REVISION:'y', EARNINGS:'n', NOTE:'n', WATCH:'n'};
  return '<div class="tl">' + list.map(h => {
    const s = h.snapshot || {}, md = s.model || {}, th = s.thesis || {};
    const snap = [md.fairValue != null ? 'Fair value ' + usd(num(md.fairValue)) : '', (md.v3y ?? th.v3y) != null ? '3Y ' + usd(num(md.v3y ?? th.v3y)) : '', (md.v5y ?? th.v5y) != null ? '5Y ' + usd(num(md.v5y ?? th.v5y)) : '', s.conviction ? 'Conviction ' + s.conviction : '', s.thesisStatus ? 'Thesis ' + s.thesisStatus : ''].filter(Boolean).join(' · ');
    return '<div class="te"><div class="d"><span>' + fmtMonth(h.at) + '</span>' + chip(h.kind || 'NOTE', tone[h.kind] || 'n', 'nd') + (h.showTicker ? '<a class="tk" href="#pos-' + esc(h.ticker) + '">' + esc(h.ticker) + '</a>' : '') + '<span class="muted" style="font-weight:500;letter-spacing:0;text-transform:none;font-family:var(--sans)">' + fmtDate(h.at) + '</span></div>' +
      '<div class="t">' + esc(h.title || '') + '</div>' + (h.note ? '<p>' + esc(h.note) + '</p>' : '') + ((h.changes || []).length ? '<p class="snap">' + h.changes.map(esc).join(' · ') + '</p>' : '') + (snap ? '<div class="snap">' + esc(snap) + '</div>' : '') +
      (th.summary ? '<details><summary>What I believed then</summary><div>' + thesisHTML(th) + '</div></details>' : '') + '</div>';
  }).join('') + '</div>';
}
function txTable(txs){
  if (!txs.length) return '<p class="muted" style="margin:0">No transactions.</p>';
  return '<div class="tscroll"><table class="tbl"><thead><tr><th>Date</th><th>Side</th><th class="n">Shares</th><th class="n">Price</th><th class="n">Value</th><th class="n">Weight after</th><th>Why</th></tr></thead><tbody>' +
    txs.slice().reverse().map(t => '<tr><td class="mono">' + fmtDate(t.date) + '</td><td>' + chip(t.side, t.side === 'BUY' ? 'g' : 'o', 'nd') + '</td><td class="n">' + fmtN(+t.shares, (+t.shares % 1) ? 2 : 0) + '</td><td class="n">' + px(+t.price, t.currency) + '</td><td class="n">' + px((+t.shares) * (+t.price), t.currency).replace('.00','') + '</td><td class="n">' + (t.weightAfter != null ? pct(+t.weightAfter, 1, false) : '—') + '</td><td class="wrap-t">' + esc([t.reason, t.note].filter(Boolean).join(' — ')) + '</td></tr>').join('') + '</tbody></table></div>';
}
function renderClosedOrMissing(t){
  const cl = Object.entries(S.closed).map(([id, c]) => Object.assign({id}, c)).filter(c => c.ticker === t);
  if (!cl.length) return '<div class="crumbs"><a href="#home">← Home</a></div><div class="empty" style="margin-top:14px">No open position or watchlist entry for ' + esc(t) + '.</div>';
  return '<div class="crumbs"><a href="#journal">← Journal</a></div><div class="dhead"><h1>' + esc(t) + '</h1>' + chip('CLOSED', 'n') + '</div>' + scoreTable(cl) +
    '<section class="block">' + secHead('Decision history') + timeline(historyFor(t).reverse()) + '</section>';
}

/* ------------------------------------------------------------------ */
/* Watch detail                                                        */
/* ------------------------------------------------------------------ */
function renderWatchDetail(C, t){
  const m = C.watch.find(x => x.t === t); if (!m) return renderClosedOrMissing(t);
  const w = m.w, st = m.ver ? VER[m.ver.code].verdict : m.wstatus, tone = m.ver ? VER[m.ver.code].tone : WSTATUS_TONE[m.wstatus];
  const lv = (k, v, note, mine) => '<div class="lv' + (mine ? ' mine' : '') + '"><span class="lbl">' + esc(k) + '</span><span class="v">' + v + '</span><span class="k">' + esc(note) + '</span></div>';
  return '<div class="crumbs"><a href="#home">← Home</a></div>' +
    '<div class="dhead"><h1>' + esc(t) + '</h1><span class="co">' + esc(w.company || '') + '</span>' + chip(st, tone) + chip('WATCHLIST', 'n', 'nd') + '</div>' +
    '<div class="btns" style="padding-block:8px 14px"><button class="btn pri" data-act="tx" data-side="BUY" data-t="' + t + '">I bought it</button><button class="btn" data-act="watch-edit" data-t="' + t + '">Edit watchlist entry</button><button class="btn" data-act="fund" data-t="' + t + '">Update current reality</button><button class="btn" data-act="note" data-t="' + t + '">Add note</button><span class="grow"></span><button class="btn sm warn" data-act="del-watch" data-t="' + t + '">Remove from watchlist</button></div>' +
    '<div class="levels">' + lv('Current', usd(m.price), pct(m.dayPct) + ' today') + lv('My buy', usd(m.buy), 'desired entry', 1) + lv('My strong buy', usd(m.strong), 'high conviction', 1) + lv('Updated buy', usd(m.updatedBuy), num(w.updatedBuy) != null ? 'my reassessment' : '3Y model at ' + m.hurdle + '% hurdle', 1) + lv('Analyst 12M', usd(m.analystTarget), pct(m.upside, 0) + ' upside') + lv('Model 3Y', usd(m.v3y), pct(m.cagr3y) + ' a year') + lv('Model 5Y', usd(m.v5y), pct(m.cagr5y) + ' a year') + lv('Max position', m.targetW != null ? m.targetW + '%' : '—', 'of portfolio') + '</div>' +
    proxBar(m) +
    (m.ver ? '<section class="block">' + secHead('Buy price trigger engine', 'A stock becoming cheaper is not a business becoming worse') + '<div class="panel-b"><div class="stack">' + VER[m.ver.code].lines.join('<br>') + '</div><div class="rec"><div class="act">' + esc(VER[m.ver.code].verdict) + '</div></div>' + checksHTML(m.ver) + '</div>' + m.lessons.map(l => '<div class="lesson"><b>From your mistake library</b>' + esc(l[1]) + '</div>').join('') + '</section>' : '') +
    (m.wstatus === 'MISSED ENTRY' ? '<section class="block">' + secHead('Missed entry', 'Do not chase. Judge the price against an updated buy price') + '<div class="panel-b">' + kv([['Watchlist price', usd(num(w.priceAdded))], ['Old buy price', usd(m.buy)], ['Updated buy price', usd(m.updatedBuy)], ['Current', usd(m.price)], ['Status', m.missed.verdict]]) + '<p class="fhint">Has intrinsic value increased enough that the original ' + usd(m.buy) + ' entry is no longer relevant? Revise your 3Y model or set an updated buy price in the watchlist entry.</p></div>' + m.lessons.map(l => '<div class="lesson"><b>From your mistake library</b>' + esc(l[1]) + '</div>').join('') + '</section>' : '') +
    '<section class="block">' + secHead('Why I want it') + '<div class="two"><div class="panel-b">' + kv([['Added', fmtDate(w.addedAt)], ['Price when added', usd(num(w.priceAdded))], ['Expected holding period', w.holdingPeriod || '—'], ['Conviction', w.conviction || '—'], ['Business quality', w.quality || '—'], ['Thesis status', w.thesisStatus || '—']]) + '</div>' +
      '<div class="panel-b">' + lbl('Why I want it') + '<div>' + esc(w.whyWant || '—') + '</div>' + lbl('Investment thesis') + '<div>' + esc(w.thesis || '—') + '</div>' + lbl('Why I am not buying yet') + '<div>' + esc(w.whyNotYet || '—') + '</div>' + lbl('Valuation') + '<div>' + esc(w.valuationNote || '—') + '</div></div></div></section>' +
    '<section class="block">' + secHead('Ask my analyst') + aiHTML(t, ['Should I buy ' + t + ' now?', 'Has the ' + t + ' thesis changed since I added it?']) + '</section>' +
    '<section class="block">' + secHead('History') + timeline(historyFor(t).reverse()) + '</section>';
}

/* ------------------------------------------------------------------ */
/* Exit map, journal, inbox                                            */
/* ------------------------------------------------------------------ */
function renderExitMap(C){
  const rows = C.pos.slice().sort((a,b) => b.weight - a.weight).map(m => {
    const trig = m.exitTrig.length || m.thesis === 'BROKEN', near = m.near.some(r => r.c.type !== 'add');
    const failC = (m.p.conditions || []).filter(c => c.type !== 'add' && (c.category === 'FAILURE' || c.category === 'VALUATION'));
    const rp = num(m.plan.reviewPrice);
    return '<tr class="' + (m.status === 'THESIS BREAK' ? 'hl-r' : trig ? 'hl-o' : near ? 'hl-y' : '') + '"><td><a class="tk" href="#pos-' + m.t + '">' + m.t + '</a></td><td class="n">' + usd(m.price) + '</td><td class="n">' + usd(m.avg) + '</td><td class="n">' + usd(num(m.plan.originalTarget)) + '</td><td class="n">' + usd(m.fair) + '</td><td class="n">' + usd(m.analystTarget) + '</td><td>' + tChip(m.thesis) + '</td><td class="n">' + pct(m.weight, 1, false) + '</td><td class="n">' + usd(rp) + (rp ? '<div class="muted" style="font-size:11.5px">' + pct((rp - m.price) / m.price * 100, 0) + ' away</div>' : '') + '</td><td class="wrap-t">' + (failC.length ? failC.map(c => esc(c.label || condText(c))).join('<br>') : '<span class="muted">None</span>') + '</td><td class="mono" style="white-space:nowrap">' + (m.plan.nextReview ? fmtDate(m.plan.nextReview) : '—') + '</td><td>' + aChip(m.rec.action) + '</td></tr>';
  }).join('');
  return '<section class="block" style="border-top:0">' + secHead('Portfolio exit map', 'Orange: a condition is triggered. Yellow: approaching a trigger. Red: thesis break.') +
    '<div class="tscroll"><table class="tbl"><thead><tr><th>Ticker</th><th class="n">Current</th><th class="n">Avg cost</th><th class="n">Original target</th><th class="n">Fair value</th><th class="n">Analyst target</th><th>Thesis</th><th class="n">Weight</th><th class="n">Review price</th><th>Fundamental sell trigger</th><th>Next review</th><th>Action</th></tr></thead><tbody>' + rows + '</tbody></table></div></section>';
}
function scoreTable(list){
  if (!list.length) return '<p class="muted" style="margin:0">No closed positions yet. When you sell a position completely, the ledger asks you to score the decision.</p>';
  return '<div class="tscroll"><table class="tbl"><thead><tr><th>Ticker</th><th>Bought</th><th class="n">Avg buy</th><th>Sold</th><th class="n">Avg sale</th><th>Held</th><th class="n">Return</th><th class="n">Annualized</th><th class="n">Benchmark</th><th class="n">Alpha</th><th>Thesis correct?</th><th>Verdict</th><th></th></tr></thead><tbody>' +
    list.map(c => '<tr><td><a class="tk" href="#pos-' + esc(c.ticker) + '">' + esc(c.ticker) + '</a></td><td class="mono">' + fmtDate(c.buyDate) + '</td><td class="n">' + usd(num(c.avgBuy)) + '</td><td class="mono">' + fmtDate(c.sellDate) + '</td><td class="n">' + usd(num(c.avgSell)) + '</td><td>' + heldFor(daysBetween(c.buyDate, c.sellDate)) + '</td><td class="n ' + cls(num(c.returnPct)) + '">' + pct(num(c.returnPct)) + '</td><td class="n">' + pct(num(c.annPct)) + '</td><td class="n">' + pct(num(c.benchReturnPct)) + '</td><td class="n ' + cls(num(c.alpha)) + '">' + pct(num(c.alpha)) + '</td><td>' + esc(c.thesisCorrect || '—') + '</td><td>' + chip(c.quality || '—', /^GOOD DECISION/.test(c.quality) ? 'g' : 'o', 'nd') + '</td><td><button class="btn sm ghost" data-act="scorecard" data-id="' + esc(c.id) + '">Edit</button></td></tr>').join('') + '</tbody></table></div>';
}
function renderJournal(C){
  const cl = Object.entries(S.closed).map(([id, c]) => Object.assign({id}, c)).sort((a,b) => (b.sellDate||'').localeCompare(a.sellDate||''));
  const q = k => cl.filter(c => c.quality === k).length;
  const mc = mistakeCounts();
  const tickers = [...new Set(Object.values(S.history).map(h => h.ticker))].sort();
  const hist = Object.entries(S.history).map(([id, h]) => Object.assign({id, showTicker:true}, h)).filter(h => !ui.histFilter || h.ticker === ui.histFilter).sort((a,b) => (b.at||'').localeCompare(a.at||'') || (b.ts||0) - (a.ts||0)).slice(0, 80);
  return '<section class="block" style="border-top:0">' + secHead('Investment scorecard', 'Decision quality and outcome are judged separately') +
      '<div class="quad">' + [['GOOD DECISION / GOOD OUTCOME','g'],['GOOD DECISION / BAD OUTCOME','y'],['BAD DECISION / GOOD OUTCOME','o'],['BAD DECISION / BAD OUTCOME','r']].map(([k, t]) => '<div>' + chip(k, t, 'nd') + '<div class="v">' + q(k) + '</div></div>').join('') + '</div>' +
      scoreTable(cl) + '</section>' +
    '<section class="block">' + secHead('Mistake library', 'Tags from closed positions. The engine uses them when it evaluates new decisions') +
      '<div class="tags">' + MISTAKES.map(t => '<span class="tag' + (mc[t] ? '' : ' zero') + '">' + t + (mc[t] ? '<span class="c">×' + mc[t] + '</span>' : '') + '</span>').join('') + '</div>' +
      (cl.filter(c => c.lessons).length ? '<div class="panel-b">' + lbl('Lessons in my own words') + cl.filter(c => c.lessons).map(c => '<div><strong class="mono">' + esc(c.ticker) + '</strong> — ' + esc(c.lessons) + '</div>').join('') + '</div>' : '') +
      (cl.filter(c => c.successes).length ? '<div class="panel-b">' + lbl('What went right') + cl.filter(c => c.successes).map(c => '<div><strong class="mono">' + esc(c.ticker) + '</strong> — ' + esc(c.successes) + '</div>').join('') + '</div>' : '') + '</section>' +
    '<section class="block">' + secHead('Decision history — all positions', '', '<label class="wtools"><span class="lbl">Ticker</span><select id="hist-filter"><option value="">All</option>' + tickers.map(t => '<option' + (t === ui.histFilter ? ' selected' : '') + '>' + esc(t) + '</option>').join('') + '</select></label>') + timeline(hist) + '</section>';
}
function renderInbox(C){
  const set = C.set, n = set.notify;
  const list = C.alerts.filter(a => ui.inbox === 'all' || (ui.inbox === 'dismissed' ? isAcked(a) : !isAcked(a)));
  const tabs = [['active','Active'],['dismissed','Dismissed'],['all','All']].map(([k,v]) => '<button class="btn sm' + (ui.inbox === k ? ' pri' : '') + '" data-act="inbox" data-v="' + k + '">' + v + '</button>').join('');
  return '<section class="block" style="border-top:0">' + secHead('Sell reviews & alerts', 'Only material changes. Ordinary daily moves never raise an alert', tabs) +
    (list.length ? '<div class="agrid">' + list.map(a => { let h = alertCard(a); if (isAcked(a)) h = h.replace('data-act="ack"', 'data-act="unack"').replace('>Dismiss<', '>Restore<'); return h; }).join('') + '</div>' : '<div class="calm"><div class="t">NOTHING HERE</div><p>No alerts in this view.</p></div>') + '</section>' +
    '<section class="block">' + secHead('Notification channels', '', '<button class="btn sm" data-act="settings">Change</button>') + '<div class="panel-b">' +
      kv([['In-app', n.inapp ? 'On' : 'Off'], ['Push', n.push ? 'Requested' : 'Off'], ['Email', n.email ? 'Requested' : 'Off'], ['SMS / WhatsApp / Telegram', 'Not available yet']]) +
      '<p class="fhint">Alerts are evaluated every time you open the app and whenever prices or data change. Push and email delivery need the daily server check, which is not switched on yet.</p>' +
      '<p class="fhint">Severity levels, from lowest: INFO, WATCH, IMPORTANT, ACTION REQUIRED, THESIS BREAK.</p></div></section>';
}

/* ------------------------------------------------------------------ */
/* Router & render                                                     */
/* ------------------------------------------------------------------ */
function route(){ const h = (location.hash || '').replace(/^#/, ''); if (h.startsWith('pos-')) return {name:'pos', t:h.slice(4)}; if (h.startsWith('watch-')) return {name:'watch', t:h.slice(6)}; return {name: ['exitmap','journal','inbox'].includes(h) ? h : 'home'}; }
let pending = false;
function schedule(){ if (pending) return; pending = true; requestAnimationFrame(() => { pending = false; render(); }); }
function render(){
  if (mode === 'boot') { $('#top').innerHTML = ''; return; }
  if (mode === 'auth') { $('#top').innerHTML = ''; $('#app').innerHTML = renderAuth(); return; }
  const C = G = computeAll();
  renderTop(C);
  const r = route();
  const focusId = document.activeElement && document.activeElement.id;
  const html = r.name === 'pos' ? renderPos(C, r.t) : r.name === 'watch' ? renderWatchDetail(C, r.t) : r.name === 'exitmap' ? renderExitMap(C) : r.name === 'journal' ? renderJournal(C) : r.name === 'inbox' ? renderInbox(C) : renderHome(C);
  const askVals = {}; document.querySelectorAll('#app input[id^="ask-"]').forEach(i => askVals[i.id] = i.value);
  $('#app').innerHTML = html;
  Object.entries(askVals).forEach(([id, v]) => { const el = document.getElementById(id); if (el) el.value = v; });
  if (focusId && focusId !== 'wsort') { const el = document.getElementById(focusId); if (el && el.closest('#app')) el.focus(); }
}
let lastRoute = location.hash;
window.addEventListener('hashchange', () => { if (location.hash !== lastRoute) { lastRoute = location.hash; window.scrollTo(0, 0); } render(); });

/* ------------------------------------------------------------------ */
/* Modals & forms                                                      */
/* ------------------------------------------------------------------ */
function openModal(title, kind, body, submit, data){
  const m = $('#modal');
  m.innerHTML = '<div class="ov"><div class="pn" role="dialog" aria-modal="true" aria-labelledby="mtitle"><header><h3 id="mtitle">' + esc(title) + '</h3><button class="btn sm ghost" data-act="close-modal">Close</button></header>' +
    '<form id="mform" data-kind="' + kind + '" novalidate><div class="mbody">' + body + '</div><div class="mfoot"><span class="err" id="merr" role="alert"></span><button type="button" class="btn" data-act="close-modal">Cancel</button><button class="btn pri" type="submit">' + esc(submit || 'Save') + '</button></div></form></div></div>';
  m.hidden = false; m._data = data || {};
  document.body.style.overflow = 'hidden';
  const first = m.querySelector('input:not([type=hidden]):not([readonly]),textarea,select'); if (first) first.focus();
}
function closeModal(){ const m = $('#modal'); m.hidden = true; m.innerHTML = ''; document.body.style.overflow = ''; }
function fld(label, name, val, o){
  o = o || {}; const id = 'f-' + name.replace(/[^a-zA-Z0-9]/g, '-');
  let ctl;
  if (o.area) ctl = '<textarea id="' + id + '" name="' + name + '" rows="' + (o.rows || 2) + '"' + (o.ph ? ' placeholder="' + esc(o.ph) + '"' : '') + '>' + esc(val ?? '') + '</textarea>';
  else if (o.options) ctl = '<select id="' + id + '" name="' + name + '">' + o.options.map(op => { const [v, t] = Array.isArray(op) ? op : [op, op]; return '<option value="' + esc(v) + '"' + (String(val ?? '') === String(v) ? ' selected' : '') + '>' + esc(t) + '</option>'; }).join('') + '</select>';
  else ctl = '<input id="' + id + '" name="' + name + '" type="' + (o.type || 'text') + '"' + (o.type === 'number' ? ' step="any" data-num="1"' : '') + (o.ro ? ' readonly' : '') + (o.ph ? ' placeholder="' + esc(o.ph) + '"' : '') + ' value="' + esc(val ?? '') + '">';
  return '<label class="fld' + (o.wide ? ' wide' : '') + '"><span>' + esc(label) + '</span>' + ctl + (o.hint ? '<small>' + esc(o.hint) + '</small>' : '') + '</label>';
}
const nfld = (l, n, v, o) => fld(l, n, v, Object.assign({type:'number'}, o || {}));
function pick(name, all, sel){ sel = sel || []; return '<div class="pick">' + all.map(v => '<label><input type="checkbox" name="' + name + '" data-multi="1" value="' + esc(v) + '"' + (sel.includes(v) ? ' checked' : '') + '>' + esc(v) + '</label>').join('') + '</div>'; }
function readForm(form){
  const o = {};
  form.querySelectorAll('[name]').forEach(el => {
    if (el.closest('.frow')) return;
    const path = el.name.split('.');
    let v;
    if (el.type === 'checkbox' && el.dataset.multi) { let cur = path.reduce((x, k) => x && x[k], o); if (!Array.isArray(cur)) { cur = []; setPath(o, path, cur); } if (el.checked) cur.push(el.value); return; }
    if (el.type === 'checkbox') v = el.checked; else v = el.dataset.num ? num(el.value) : el.value.trim();
    setPath(o, path, v);
  });
  return o;
}
function setPath(o, path, v){ let x = o; path.slice(0, -1).forEach(k => { x[k] = (x[k] && typeof x[k] === 'object') ? x[k] : {}; x = x[k]; }); x[path[path.length - 1]] = v; }
function getPath(o, path){ return path.split('.').reduce((x, k) => x == null ? undefined : x[k], o); }
function metricOptions(t, withManual){
  const custom = Object.keys((S.fundamentals[t] || {}).custom || {}).map(k => ['custom:' + k, k + ' (custom)']);
  return (withManual ? [['', 'Judged manually']] : []).concat(Object.entries(METRICS).map(([k, v]) => [k, v[0]]), custom);
}
const opOpts = [['<','<'],['<=','≤'],['>','>'],['>=','≥']];
function selHTML(f, opts, val){ return '<select data-f="' + f + '" aria-label="' + f + '">' + opts.map(op => { const [v, t] = Array.isArray(op) ? op : [op, op]; return '<option value="' + esc(v) + '"' + (String(val ?? '') === String(v) ? ' selected' : '') + '>' + esc(t) + '</option>'; }).join('') + '</select>'; }
function expRow(e, t){ e = e || {}; return '<div class="frow" data-kind="exp" data-id="' + esc(e.id || newId()) + '"><input class="w2" data-f="label" placeholder="Expectation, e.g. Revenue growth" value="' + esc(e.label || '') + '" aria-label="Expectation">' + selHTML('metric', metricOptions(t, true), e.metric || '') + selHTML('op', opOpts, e.op || '>') + '<input data-f="value" type="number" step="any" placeholder="Value" value="' + esc(e.value ?? '') + '" aria-label="Value">' + selHTML('manual', [['','If manual…'],['met','Met'],['unmet','Not met']], e.manual || '') + '<button type="button" class="btn sm ghost" data-act="del-row">Remove</button></div>'; }
function condRow(c, t){
  c = c || {type:'exit', category:'FAILURE', op:'<'}; const a = c.and || {};
  return '<div class="frow" data-kind="cond" data-id="' + esc(c.id || newId()) + '">' + selHTML('type', [['exit','Exit / sell review'],['add','Add / buy more']], c.type) + selHTML('category', Object.entries(CATS), c.category) +
    '<input class="w2" data-f="label" placeholder="Alert text, e.g. VOYG backlog falls by more than 20%" value="' + esc(c.label || '') + '" aria-label="Alert text">' +
    selHTML('metric', metricOptions(t), c.metric || 'price') + selHTML('op', opOpts, c.op) + '<input data-f="value" type="number" step="any" placeholder="Value" value="' + esc(c.value ?? '') + '" aria-label="Value">' +
    selHTML('action', ['REVIEW','TRIM','EXIT','ADD','STOP ADDING'], c.action || 'REVIEW') +
    '<span class="andl">Optional second condition (AND)</span>' + selHTML('and-metric', [['','—']].concat(metricOptions(t)), a.metric || '') + selHTML('and-op', opOpts, a.op || '<') + '<input data-f="and-value" type="number" step="any" placeholder="Value" value="' + esc(a.value ?? '') + '" aria-label="Second value">' +
    '<button type="button" class="btn sm ghost" data-act="del-row">Remove</button></div>';
}
function readRows(form, kind){
  return [...form.querySelectorAll('.frow[data-kind="' + kind + '"]')].map(r => {
    const g = f => { const el = r.querySelector('[data-f="' + f + '"]'); return el ? el.value.trim() : ''; };
    if (kind === 'exp') return {id:r.dataset.id, label:g('label'), metric:g('metric') || null, op:g('op'), value:num(g('value')), manual:g('manual') || null};
    const c = {id:r.dataset.id, type:g('type'), category:g('type') === 'add' ? 'ADD' : g('category'), label:g('label'), metric:g('metric'), op:g('op'), value:num(g('value')), action:g('action')};
    if (g('and-metric') && num(g('and-value')) != null) c.and = {metric:g('and-metric'), op:g('and-op'), value:num(g('and-value'))};
    if (!c.label) c.label = condText(c);
    return c;
  }).filter(x => kind === 'exp' ? (x.label || x.metric) : (x.metric && x.value != null));
}
function snapOf(p){ return clean({thesisStatus:p.thesisStatus || null, conviction:p.conviction || null, classifications:p.classifications || [], thesis:p.thesis || {}, plan:p.plan || {}, model:p.model || {}, expectations:p.expectations || [], conditions:p.conditions || [], baseline:p.baseline || {}}); }
function diffOf(a, b){
  const F = [['Fair value','model.fairValue',usd],['3Y model','model.v3y',usd],['5Y model','model.v5y',usd],['Review price','plan.reviewPrice',usd],['Add below','plan.addBelow',usd],['Target weight','plan.targetWeight',v => v + '%'],['Conviction','conviction',String],['Thesis','thesisStatus',String]];
  return F.map(([l, p, f]) => { const x = getPath(a, p), y = getPath(b, p); if ((x ?? null) === (y ?? null) || (y == null || y === '')) return null; return l + ' ' + (x == null || x === '' ? '—' : f(x)) + ' → ' + f(y); }).filter(Boolean);
}

function openThesis(t, focus){
  const C = G, m = C.pos.find(x => x.t === t) || null;
  const p = m ? m.p : (S.positions[t] || {ticker:t}); const w = S.watchlist[t];
  const isNew = !(p.thesis && (p.thesis.summary || p.thesis.whyOwn));
  const th = Object.assign({}, p.thesis || {}), pl = p.plan || {}, md = p.model || {}, bl = p.baseline || {}, nc = p.nextCatalyst || {};
  if (isNew && w) { th.summary = th.summary || w.thesis || ''; th.whyOwn = th.whyOwn || w.whyWant || ''; th.holdingPeriod = th.holdingPeriod || w.holdingPeriod || ''; th.v3y = th.v3y ?? w.model3y; th.v5y = th.v5y ?? w.model5y; }
  const f = S.fundamentals[t] || {};
  const draft = (isNew && aiOn()) ? '<fieldset><legend>Help me write it</legend><p class="fhint">Write, in your own words, why you bought ' + esc(t) + '. Claude structures it and suggests measurable conditions. It will not invent your reasons; anything it cannot take from your words is left blank and turned into a question.</p>' +
    '<textarea id="draft-notes" rows="3" placeholder="e.g. Bought for the launch cadence and Neutron. Thought space infrastructure would re-rate…"></textarea><div class="btns"><button type="button" class="btn" data-act="draft" data-t="' + esc(t) + '">Draft from my words</button><span class="fhint" id="draft-status"></span></div><div id="draft-q"></div></fieldset>' : '';
  const body = draft +
    '<fieldset><legend>Position</legend><div class="fg">' + fld('Company', 'company', p.company || (w && w.company) || '') + fld('Currency', 'currency', p.currency || 'USD', {options:['USD','SAR']}) + fld('Conviction', 'conviction', p.conviction || 'MEDIUM', {options:['HIGH','MEDIUM','LOW']}) +
      fld('Thesis status', 'thesisStatus', p.thesisStatus || (isNew ? 'INTACT' : ''), {options:[['','Not set']].concat(THESIS)}) + fld('Themes (comma separated)', 'themes', (p.themes || (w && w.themes) || []).join(', '), {hint:'Used to detect duplicate exposure'}) + '</div>' +
      '<div class="fld"><span>Why I made this investment (pick all that apply)</span>' + pick('classifications', CLASSES, p.classifications) + '</div></fieldset>' +
    '<fieldset><legend>Why did I buy this?</legend><div class="fg">' + fld('Investment thesis', 'thesis.summary', th.summary, {area:1, wide:1, rows:3}) + fld('Why I own it (one line for the dashboard)', 'thesis.whyOwn', th.whyOwn, {wide:1}) +
      fld('Expected holding period', 'thesis.holdingPeriod', th.holdingPeriod) + fld('Expected revenue growth', 'thesis.expRevGrowth', th.expRevGrowth) + fld('Expected earnings / FCF growth', 'thesis.expEarnGrowth', th.expEarnGrowth) + fld('Expected valuation', 'thesis.expValuation', th.expValuation) +
      fld('Expected catalysts', 'thesis.catalysts', th.catalysts, {area:1, wide:1}) +
      nfld('Expected 1Y value ($)', 'thesis.v1y', th.v1y) + nfld('Expected 3Y value ($)', 'thesis.v3y', th.v3y) + nfld('Expected 5Y value ($)', 'thesis.v5y', th.v5y) + fld('Return I expect', 'thesis.expectedReturn', th.expectedReturn) +
      fld('Bull case', 'thesis.bull', th.bull, {area:1}) + fld('Base case', 'thesis.base', th.base, {area:1}) + fld('Bear case', 'thesis.bear', th.bear, {area:1}) +
      fld('Main reason I could be wrong', 'thesis.wrongReason', th.wrongReason, {area:1, wide:1}) + fld('What would make me sell?', 'thesis.sellReason', th.sellReason, {area:1, wide:1}) + fld('When I intend to sell', 'thesis.intendedSell', th.intendedSell, {wide:1}) + '</div></fieldset>' +
    '<fieldset><legend>Plan: prices, size and review</legend><p class="fhint">Reaching the review price triggers a review for sale, never an automatic sale.</p><div class="fg">' +
      nfld('Add below ($)', 'plan.addBelow', pl.addBelow) + nfld('Strong buy below ($)', 'plan.strongBuy', pl.strongBuy) + nfld('Stop adding above ($)', 'plan.stopAddingAbove', pl.stopAddingAbove) + nfld('Review for sale at ($)', 'plan.reviewPrice', pl.reviewPrice) + nfld('Original price target ($)', 'plan.originalTarget', pl.originalTarget ?? (isNew ? null : pl.reviewPrice)) +
      nfld('Target position (%)', 'plan.targetWeight', pl.targetWeight) + nfld('Maximum position (%)', 'plan.maxWeight', pl.maxWeight, {hint:'Default ' + (num(C.set.maxWeight) ?? 20) + '%'}) + nfld('Minimum 3Y CAGR to keep (%)', 'plan.minCagr', pl.minCagr, {hint:'Default ' + (num(C.set.minCagr) ?? 8) + '%'}) + nfld('Hurdle for new money (%)', 'plan.hurdle', pl.hurdle, {hint:'Default ' + (num(C.set.hurdle) ?? 15) + '%'}) +
      fld('Scheduled thesis review', 'plan.nextReview', pl.nextReview, {type:'date'}) + fld('Next catalyst', 'nextCatalyst.label', nc.label) + fld('Catalyst date', 'nextCatalyst.date', nc.date, {type:'date'}) +
      fld('Sell if (dashboard text)', 'plan.sellIfText', pl.sellIfText, {wide:1, ph:'Leave blank to list your thesis-failure conditions'}) + '</div></fieldset>' +
    '<fieldset><legend>Current model (update as the business changes)</legend><div class="fg">' + nfld('Fair value today ($)', 'model.fairValue', md.fairValue) + nfld('3Y value ($)', 'model.v3y', md.v3y) + nfld('5Y value ($)', 'model.v5y', md.v5y) + '</div></fieldset>' +
    '<fieldset id="fs-exp"><legend>What I expect to happen (measurable)</legend><p class="fhint">These drive the THEN vs NOW comparison. Pick a metric so the ledger can check it, or judge it manually.</p><div class="frows" id="rows-exp">' + (p.expectations || []).map(e => expRow(e, t)).join('') + '</div><div><button type="button" class="btn sm" data-act="add-row" data-kind="exp" data-t="' + esc(t) + '">Add expectation</button></div></fieldset>' +
    '<fieldset id="fs-conds"><legend>Exit plan & add conditions</legend><p class="fhint">Review price, size limit, minimum CAGR, review date and add levels from the plan become alerts automatically. Add the fundamental ones here: thesis failure, valuation, completion, liquidity.</p><div class="frows" id="rows-cond">' + (p.conditions || []).map(c => condRow(c, t)).join('') + '</div><div><button type="button" class="btn sm" data-act="add-row" data-kind="cond" data-t="' + esc(t) + '">Add condition</button></div></fieldset>' +
    '<fieldset><legend>Baseline at purchase</legend><p class="fhint">What the numbers were when you decided. Changes since purchase are measured against these. Units are yours ($M, M shares), but keep them consistent with current-reality updates.</p><div class="fg">' +
      fld('Baseline date', 'baseline.date', bl.date || (m ? m.start : todayISO()), {type:'date'}) + nfld('Price', 'baseline.price', bl.price ?? (m ? (isNew ? m.avg : null) : null)) + nfld('Revenue estimate (NTM)', 'baseline.revEst', bl.revEst ?? (isNew ? f.revEst : null)) + nfld('EPS estimate (NTM)', 'baseline.epsEst', bl.epsEst ?? (isNew ? f.epsEst : null)) +
      nfld('Analyst target', 'baseline.analystTarget', bl.analystTarget ?? (isNew ? f.analystTarget : null)) + nfld('Backlog', 'baseline.backlog', bl.backlog ?? (isNew ? f.backlog : null)) + nfld('Shares outstanding', 'baseline.shares', bl.shares ?? (isNew ? f.shares : null)) + nfld('Net debt', 'baseline.netDebt', bl.netDebt ?? (isNew ? f.netDebt : null)) + '</div></fieldset>' +
    '<fieldset><legend>What changed?</legend>' + fld(isNew ? 'Note for the decision history' : 'Why are you revising? (saved as a new version; the old one is kept)', 'changeNote', '', {area:1, wide:1, ph: isNew ? 'Initial thesis recorded' : 'e.g. Q3 revenue beat; raised 5Y model'}) + '</fieldset>';
  openModal((isNew ? 'Define thesis — ' : 'Revise thesis & plan — ') + t, 'thesis', body, isNew ? 'Save thesis' : 'Save new version', {t, isNew});
  if (focus === 'conds') setTimeout(() => { const el = $('#fs-conds'); if (el) el.scrollIntoView({block:'start'}); }, 30);
}
async function saveThesis(form, d){
  const t = d.t, v = readForm(form);
  const old = S.positions[t] || {ticker:t};
  if (!d.isNew && !v.changeNote) throw new Error('Write one line on why you are revising. The decision history depends on it.');
  const m = G.pos.find(x => x.t === t);
  const doc = Object.assign({}, old, {ticker:t, company:v.company, currency:v.currency, conviction:v.conviction, thesisStatus:v.thesisStatus || null, classifications:v.classifications || [],
    themes:(v.themes || '').split(',').map(s => s.trim()).filter(Boolean), thesis:v.thesis, plan:v.plan, model:v.model, baseline:v.baseline, nextCatalyst:v.nextCatalyst,
    expectations:readRows(form, 'exp'), conditions:readRows(form, 'cond'), updatedAt:new Date().toISOString()});
  if (doc.model.v3y == null && doc.thesis.v3y != null) doc.model.v3y = doc.thesis.v3y;
  if (doc.model.v5y == null && doc.thesis.v5y != null) doc.model.v5y = doc.thesis.v5y;
  if (doc.plan.originalTarget == null && d.isNew) doc.plan.originalTarget = doc.plan.reviewPrice;
  if (m && doc.price == null) doc.price = m.price;
  delete doc.changeNote; delete doc.example;
  const changes = d.isNew ? [] : diffOf(old, doc);
  await store.set('positions', t, doc);
  await store.add('history', {ticker:t, at:todayISO(), ts:Date.now(), kind: d.isNew ? 'THESIS' : 'REVISION', title: d.isNew ? 'Recorded investment thesis at ' + usd(m ? m.price : num(doc.baseline.price)) : (v.changeNote.split('\n')[0].slice(0, 120)), note: d.isNew ? (v.changeNote || '') : v.changeNote.split('\n').slice(1).join(' '), changes, snapshot: snapOf(doc), price: m ? m.price : null});
  toast(d.isNew ? 'Thesis saved' : 'New version saved. The previous one is kept in history.');
}
async function draftThesis(t){
  const notes = ($('#draft-notes') || {}).value || '', st = $('#draft-status');
  if (!notes.trim()) { st.textContent = 'Write a sentence or two first.'; return; }
  const m = G.pos.find(x => x.t === t), f = S.fundamentals[t] || {};
  st.textContent = 'Drafting…';
  const facts = {ticker:t, company:(S.positions[t] || {}).company, price: m && m.price, avgCost: m && m.avg, bought: m && m.start, weightPct: m && +m.weight.toFixed(1), fundamentals:f};
  const prompt = 'Help an investor write down the investment thesis for a stock they already own, so a decision journal can monitor it.\n\n' +
    'Their own words about why they bought it:\n"""' + notes.slice(0, 4000) + '"""\n\nCurrent facts from their ledger (may be stale):\n' + JSON.stringify(facts) + '\n\n' +
    'Rules:\n- Motive fields (thesis.summary, thesis.whyOwn, thesis.expRevGrowth, thesis.catalysts, thesis.holdingPeriod, thesis.wrongReason) must come ONLY from their words. Tidy them, do not add reasons they did not give. If their words do not cover a field, return "" and add a question instead.\n' +
    '- You MAY propose measurable conditions (sell/review/add) as suggestions based on their words and the facts.\n- Never state facts about the company that are not in the data above.\n\n' +
    'Reply with only JSON of this shape: {"fields":{"thesis.summary":"","thesis.whyOwn":"","thesis.holdingPeriod":"","thesis.expRevGrowth":"","thesis.catalysts":"","thesis.wrongReason":"","thesis.sellReason":"","thesis.bull":"","thesis.base":"","thesis.bear":"","plan.reviewPrice":null,"plan.addBelow":null,"plan.maxWeight":null},"questions":["..."]}';
  try {
    const r = parseJSONReply((await callFn('analyst', {prompt, json:true})).text || '');
    const F = (r && r.fields) || {}; let n = 0;
    Object.entries(F).forEach(([k, v]) => { if (v == null || v === '') return; const el = document.getElementById('f-' + k.replace(/[^a-zA-Z0-9]/g, '-')); if (el && !el.value) { el.value = v; el.closest('.fld').classList.add('drafted'); n++; } });
    st.textContent = n ? n + ' fields drafted (highlighted). Edit them until they are your words.' : 'Nothing to fill from your notes.';
    const qs = (r && r.questions) || [];
    $('#draft-q').innerHTML = qs.length ? lbl('Questions to answer yourself') + '<ul style="margin:4px 0 0;padding-left:18px">' + qs.map(q => '<li>' + esc(q) + '</li>').join('') + '</ul>' : '';
  } catch (e) { st.textContent = aiErr(e && e.code); }
}

function openFund(t){
  const f = S.fundamentals[t] || {};
  const custom = Object.entries(f.custom || {}).map(([k, v]) => k + '=' + v).join('\n');
  const body = '<p class="fhint">Record what is true now. The ledger compares it with your baseline and your original expectations. Use the same units as the baseline.</p>' +
    '<fieldset><legend>Growth & estimates</legend><div class="fg">' + nfld('Revenue growth YoY (%)', 'revGrowth', f.revGrowth) + nfld('Gross margin (%)', 'grossMargin', f.grossMargin) + fld('Margin trend', 'marginTrend', f.marginTrend || '', {options:['','Improving','Stable','Deteriorating']}) +
      nfld('Revenue estimate (NTM)', 'revEst', f.revEst) + nfld('EPS estimate (NTM)', 'epsEst', f.epsEst) + nfld('Analyst consensus target ($)', 'analystTarget', f.analystTarget) + nfld('Forward P/E', 'fwdPE', f.fwdPE) + nfld('Forward FCF multiple', 'fwdFcfMult', f.fwdFcfMult) + '</div></fieldset>' +
    '<fieldset><legend>Balance sheet & business</legend><div class="fg">' + nfld('Backlog', 'backlog', f.backlog) + nfld('Shares outstanding', 'shares', f.shares) + nfld('Net debt', 'netDebt', f.netDebt) + nfld('52-week high ($)', 'high52', f.high52) +
      fld('Guidance', 'guidance', f.guidance || '', {options:['','Raised','Maintained','Cut']}) + fld('Why the stock moved', 'declineReason', f.declineReason || '', {options:['','Market','Sector','Company-specific']}) +
      fld('Major customer lost', 'customerLoss', f.customerLoss === true ? 'yes' : f.customerLoss === false ? 'no' : '', {options:[['','No data'],['no','No'],['yes','Yes']]}) + fld('Material analyst downgrades', 'downgrades', f.downgrades === true ? 'yes' : f.downgrades === false ? 'no' : '', {options:[['','No data'],['no','No'],['yes','Yes']]}) +
      fld('Custom metrics (one per line: name=value)', 'custom', custom, {area:1, wide:1, ph:'backlog_months=14'}) + '</div></fieldset>' +
    '<fieldset><legend>What happened</legend>' + fld('Note (earnings, news, filings)', 'note', '', {area:1, wide:1, rows:3}) + '<label class="chk"><input type="checkbox" name="isEarnings" id="f-isEarnings"> This is an earnings update (adds an entry to the decision history)</label></fieldset>';
  openModal('Current reality — ' + t, 'fund', body, 'Save', {t});
}
async function saveFund(form, d){
  const t = d.t, v = readForm(form), old = S.fundamentals[t] || {};
  const custom = {}; (v.custom || '').split('\n').forEach(l => { const i = l.indexOf('='); if (i > 0) { const k = l.slice(0, i).trim().replace(/[^\w.-]/g, '_'), val = num(l.slice(i + 1).trim()); if (k && val != null) custom[k] = val; } });
  const notes = (old.notes || []).slice(-19); if (v.note) notes.push({date:todayISO(), text:v.note});
  const doc = {ticker:t, asOf:todayISO(), revGrowth:v.revGrowth, grossMargin:v.grossMargin, marginTrend:v.marginTrend || null, revEst:v.revEst, epsEst:v.epsEst, analystTarget:v.analystTarget, fwdPE:v.fwdPE, fwdFcfMult:v.fwdFcfMult,
    backlog:v.backlog, shares:v.shares, netDebt:v.netDebt, high52:v.high52, guidance:v.guidance || null, declineReason:v.declineReason || null,
    customerLoss: v.customerLoss === 'yes' ? true : v.customerLoss === 'no' ? false : null, downgrades: v.downgrades === 'yes' ? true : v.downgrades === 'no' ? false : null, custom, notes};
  await store.set('fundamentals', t, doc);
  if (v.isEarnings || v.note) {
    const p = S.positions[t];
    await store.add('history', {ticker:t, at:todayISO(), ts:Date.now(), kind: v.isEarnings ? 'EARNINGS' : 'NOTE', title: v.isEarnings ? 'Earnings update recorded' : 'Reality update', note:v.note || '', snapshot: p ? Object.assign(snapOf(p), {reality:doc}) : {reality:doc}});
  }
  toast('Current reality saved');
}

function openTx(side, t){
  const C = G; t = t || '';
  const m = t ? C.pos.find(x => x.t === t) : null, w = t ? C.watch.find(x => x.t === t) : null;
  const price = m ? m.price : w ? w.price : null;
  let pre = '';
  if (side === 'BUY' && m && m.hasThesis) {
    const addC = m.conds.filter(r => r.c.type === 'add');
    pre = '<div class="precheck">' + lbl('Before you add: your own rules') + (addC.length ? addC.map(r => '<div>' + esc(r.c.label) + ' — ' + (r.ok ? chip('MET', 'g', 'nd') : chip('NOT MET', 'o', 'nd')) + '</div>').join('') : '<div>No add condition recorded.</div>') +
      (m.ver ? '<div class="stack">' + VER[m.ver.code].plines.join(' · ') + ' → ' + VER[m.ver.code].pverdict + '</div>' : '') +
      (m.targetW != null ? '<div class="muted">Weight ' + pct(m.weight, 1, false) + ' of ' + m.targetW + '% target.</div>' : '') +
      (!m.addHits.length ? '<div class="lesson"><b>Outside your add conditions</b>Buying now means buying above the level you set in advance. Write down what changed, or wait.' + ((C.mistakes['CHASED PRICE'] || C.mistakes['FOMO']) ? ' Chasing price is in your mistake library.' : '') + '</div>' : '') + '</div>';
  }
  if (side === 'BUY' && w) pre = '<div class="precheck">' + lbl('From your watchlist') + '<div>Buy below ' + usd(w.buy) + ', strong buy ' + usd(w.strong) + '. Current ' + usd(w.price) + '. ' + chip(w.ver ? VER[w.ver.code].verdict : w.wstatus, w.ver ? VER[w.ver.code].tone : WSTATUS_TONE[w.wstatus]) + '</div><div class="muted">Your watchlist thesis will prefill the investment thesis.</div></div>';
  if (side === 'SELL' && m) pre = '<div class="precheck">' + lbl('Sell decision engine says') + '<div class="rec" style="padding:10px 12px"><div class="act" style="font-size:18px">' + esc(m.rec.action) + '</div><ul>' + m.rec.reasons.map(r => '<li>' + esc(r) + '</li>').join('') + '</ul></div>' + m.lessons.map(l => '<div class="lesson"><b>' + esc(l[0]) + '</b>' + esc(l[1]) + '</div>').join('') + '<div class="muted">You hold ' + fmtN(m.shares, m.shares % 1 ? 2 : 0) + ' shares at ' + usd(m.avg) + ' average cost. A rising price alone is not a reason to sell.</div></div>';
  const body = pre + '<fieldset><legend>' + (side === 'BUY' ? 'Buy' : 'Sell') + '</legend><div class="fg">' + fld('Ticker', 'ticker', t, {ph:'e.g. PL', ro: !!t}) + fld('Date', 'date', todayISO(), {type:'date'}) + nfld('Shares', 'shares', side === 'SELL' && m ? m.shares : '') + nfld('Price per share', 'price', price) + nfld('Fees', 'fees', '') +
      fld('Currency', 'currency', (m && m.cur) || (w && w.cur) || 'USD', {options:['USD','SAR']}) + (side === 'SELL' ? fld('Reason for selling', 'reason', '', {options:[['','Choose…']].concat(Object.entries(CATS).filter(([k]) => k !== 'ADD').map(([k, v]) => [v, v]), [['Other','Other']])}) : '') + '</div>' +
      fld(side === 'BUY' ? (m ? 'Why am I adding?' : 'Why am I buying? (you will write the full thesis next)') : 'What made me sell?', 'note', '', {area:1, wide:1}) +
      '<label class="chk"><input type="checkbox" name="useCash" id="f-useCash" checked> ' + (side === 'BUY' ? 'Paid from available cash' : 'Add proceeds to available cash') + '</label></fieldset>';
  openModal(side === 'BUY' ? 'Record buy' + (t ? ' — ' + t : '') : 'Record sell — ' + t, 'tx', body, side === 'BUY' ? 'Save buy' : 'Save sell', {side, t});
}
async function saveTx(form, d){
  const v = readForm(form), t = safeId(v.ticker);
  if (!t) throw new Error('Enter a ticker.');
  if (!(v.shares > 0) || !(v.price > 0)) throw new Error('Enter shares and a price above zero.');
  const C = G, m = C.pos.find(x => x.t === t), fx = C.fx, cur = v.currency || 'USD';
  if (d.side === 'SELL' && (!m || v.shares > m.shares + 1e-9)) throw new Error('You hold ' + (m ? fmtN(m.shares, 2) : 0) + ' shares of ' + t + '.');
  const amtSAR = (v.shares * v.price + (d.side === 'BUY' ? 1 : -1) * (v.fees || 0)) * (cur === 'SAR' ? 1 : fx);
  const newVal = (m ? m.valueSAR : 0) + (d.side === 'BUY' ? amtSAR : -v.shares * (m ? m.price : v.price) * (cur === 'SAR' ? 1 : fx));
  const newTotal = C.totals.total + (v.useCash ? 0 : (d.side === 'BUY' ? amtSAR : -amtSAR));
  const tx = {ticker:t, side:d.side, date:v.date || todayISO(), shares:v.shares, price:v.price, fees:v.fees || 0, currency:cur, weightAfter: newTotal ? Math.max(0, newVal / newTotal * 100) : null, note:v.note || '', reason:v.reason || ''};
  await store.add('transactions', tx);
  if (v.useCash) { const s = Object.assign({}, S.settings.main || {}); s.cash = Math.max(0, (num(s.cash) || 0) + (d.side === 'BUY' ? -amtSAR : amtSAR)); delete s.example; await store.set('settings', 'main', s); }
  const p = S.positions[t];
  if (!p) { const w = S.watchlist[t]; await store.set('positions', t, {ticker:t, company: w ? w.company : '', currency:cur, price:v.price, prevClose: w ? w.prevClose : null, priceAt:todayISO(), themes: w ? (w.themes || []) : [], conviction: w ? w.conviction : null}); }
  const isNewPos = !m;
  await store.add('history', {ticker:t, at:tx.date, ts:Date.now(), kind: d.side === 'SELL' ? 'SELL' : (isNewPos ? 'BUY' : 'ADD'),
    title: (d.side === 'SELL' ? 'Sold ' : isNewPos ? 'Bought ' : 'Added ') + fmtN(v.shares, v.shares % 1 ? 2 : 0) + ' at ' + px(v.price, cur) + (d.side === 'SELL' && v.reason ? ' — ' + v.reason : ''),
    note:v.note || '', changes: (d.side === 'BUY' && m && !m.addHits.length) ? ['Bought outside recorded add conditions'] : [], snapshot: p ? snapOf(p) : {}, price:v.price});
  closeModal();
  toast(d.side === 'SELL' ? 'Sale recorded' : 'Buy recorded');
  if (d.side === 'BUY' && (isNewPos || !(p && p.thesis && (p.thesis.summary || p.thesis.whyOwn)))) setTimeout(() => { G = computeAll(); openThesis(t); }, 60);
  if (d.side === 'SELL' && m && Math.abs(v.shares - m.shares) < 1e-9) setTimeout(() => { G = computeAll(); openScorecard(t); }, 60);
  return 'closed';
}

function openScorecard(t, id){
  let c;
  if (id) c = Object.assign({}, S.closed[id]);
  else {
    const L = G.lots[t] || lotsOf(G.byT[t] || []), p = S.positions[t] || {}, avgBuy = L.buyShares ? L.buyAmt / L.buyShares : null, avgSell = L.sellShares ? L.sellAmt / L.sellShares : null;
    const buyDate = L.cycleStart || L.firstBuy, days = daysBetween(buyDate, L.lastSell), ret = avgBuy && avgSell ? (avgSell / avgBuy - 1) * 100 : null;
    c = {ticker:t, company:p.company || '', buyDate, avgBuy, sellDate:L.lastSell, avgSell, shares:L.sellShares, returnPct:ret, annPct: (ret != null && days > 0) ? (Math.pow(1 + ret / 100, 365 / days) - 1) * 100 : null, thesisSummary:(p.thesis || {}).summary || '', process:{}, mistakes:[]};
  }
  const pr = c.process || {};
  const body = '<div class="precheck">' + kv([['Bought', fmtDate(c.buyDate) + ' at ' + usd(num(c.avgBuy)) + ' average'], ['Sold', fmtDate(c.sellDate) + ' at ' + usd(num(c.avgSell)) + ' average'], ['Held', heldFor(daysBetween(c.buyDate, c.sellDate))], ['Total return', pct(num(c.returnPct))], ['Annualized', pct(num(c.annPct))], ['Original thesis', c.thesisSummary || 'Not recorded', 1]]) + '</div>' +
    '<fieldset><legend>Outcome</legend><div class="fg">' + nfld('Benchmark return over the same period (%)', 'benchReturnPct', c.benchReturnPct, {hint:(G.set.benchmark || 'Benchmark') + ', ' + fmtDate(c.buyDate) + ' to ' + fmtDate(c.sellDate)}) + '</div></fieldset>' +
    '<fieldset><legend>Was the original thesis correct?</legend>' + fld('Thesis', 'thesisCorrect', c.thesisCorrect || '', {options:[['','Choose…'],['YES','Yes, it played out'],['PARTLY','Partly'],['NO','No, it was wrong']]}) +
      '<p class="fhint">Judge the decision on what you knew then, not on the result.</p>' +
      ['thesis|I had a written thesis and exit plan before buying', 'sized|I sized the position according to my rules', 'followedPlan|I followed my own exit or add conditions', 'reasoning|The reasoning was sound given the information available then'].map(s => { const [k, l] = s.split('|'); return '<label class="chk"><input type="checkbox" name="process.' + k + '" id="f-process-' + k + '"' + (pr[k] ? ' checked' : '') + '> ' + l + '</label>'; }).join('') + '</fieldset>' +
    '<fieldset><legend>Lessons</legend><div class="fld"><span>Mistakes (tag any that apply)</span>' + pick('mistakes', MISTAKES, c.mistakes) + '</div>' + fld('What went right', 'successes', c.successes, {area:1, wide:1}) + fld('Lesson in my own words', 'lessons', c.lessons, {area:1, wide:1, rows:3}) + '</fieldset>';
  openModal('Score the decision — ' + t, 'score', body, 'Save scorecard', {t, id, c});
}
async function saveScore(form, d){
  const v = readForm(form), c = Object.assign({}, d.c);
  c.benchReturnPct = v.benchReturnPct; c.alpha = (num(c.returnPct) != null && v.benchReturnPct != null) ? num(c.returnPct) - v.benchReturnPct : null;
  c.thesisCorrect = v.thesisCorrect || null; c.process = v.process || {}; c.mistakes = v.mistakes || []; c.successes = v.successes; c.lessons = v.lessons;
  const score = Object.values(c.process).filter(Boolean).length + (c.thesisCorrect === 'YES' ? 1 : c.thesisCorrect === 'PARTLY' ? .5 : 0);
  c.decision = score >= 3 ? 'GOOD' : 'BAD';
  c.outcome = c.alpha != null ? (c.alpha >= 0 ? 'GOOD' : 'BAD') : (num(c.returnPct) >= 0 ? 'GOOD' : 'BAD');
  c.quality = c.decision + ' DECISION / ' + c.outcome + ' OUTCOME';
  delete c.id;
  const id = d.id || safeId(d.t + '-' + (c.sellDate || todayISO()));
  await store.set('closed', id, c);
  if (!d.id) await store.add('history', {ticker:d.t, at:c.sellDate || todayISO(), ts:Date.now(), kind:'CLOSE', title:'Position closed: ' + c.quality, note:c.lessons || '', changes:(c.mistakes || []).map(x => 'Mistake: ' + x)});
  toast('Scorecard saved');
}

function openWatch(t){
  const w = t ? Object.assign({}, S.watchlist[t]) : {addedAt:todayISO()}, nc = w.nextCatalyst || {};
  const body = '<fieldset><legend>Company</legend><div class="fg">' + fld('Ticker', 'ticker', w.ticker || '', {ro: !!t}) + fld('Company', 'company', w.company) + fld('Currency', 'currency', w.currency || 'USD', {options:['USD','SAR']}) + nfld('Current price', 'price', w.price) + nfld('Previous close', 'prevClose', w.prevClose) + fld('Date added', 'addedAt', w.addedAt, {type:'date'}) + nfld('Price when added', 'priceAdded', w.priceAdded, {hint:'Defaults to the current price'}) + '</div></fieldset>' +
    '<fieldset><legend>My price</legend><div class="fg">' + nfld('Buy below ($)', 'buyBelow', w.buyBelow) + nfld('Strong buy below ($)', 'strongBuy', w.strongBuy) + nfld('Maximum position (%)', 'maxPosition', w.maxPosition) + nfld('Updated buy price ($)', 'updatedBuy', w.updatedBuy, {hint:'Set when you reassess after the price ran away'}) + nfld('Hurdle (%)', 'hurdle', w.hurdle, {hint:'Default ' + (num(G.set.hurdle) ?? 15) + '%'}) + '</div></fieldset>' +
    '<fieldset><legend>Why I want it</legend><div class="fg">' + fld('Why I want it', 'whyWant', w.whyWant, {area:1, wide:1}) + fld('Investment thesis', 'thesis', w.thesis, {area:1, wide:1, rows:3}) + fld('Why I am not buying yet', 'whyNotYet', w.whyNotYet, {area:1, wide:1}) +
      fld('Expected holding period', 'holdingPeriod', w.holdingPeriod) + fld('Conviction', 'conviction', w.conviction || 'MEDIUM', {options:['HIGH','MEDIUM','LOW']}) + fld('Business quality', 'quality', w.quality || 'MEDIUM', {options:['HIGH','MEDIUM','LOW']}) + fld('Thesis status', 'thesisStatus', w.thesisStatus || 'INTACT', {options:THESIS}) + fld('Themes (comma separated)', 'themes', (w.themes || []).join(', ')) + '</div></fieldset>' +
    '<fieldset><legend>Value</legend><div class="fg">' + nfld('Analyst 1Y target ($)', 'analystTarget1y', w.analystTarget1y) + nfld('My 3Y model value ($)', 'model3y', w.model3y) + nfld('My 5Y model value ($)', 'model5y', w.model5y) + fld('Valuation note', 'valuationNote', w.valuationNote, {wide:1}) + fld('Next catalyst', 'nextCatalyst.label', nc.label) + fld('Catalyst date', 'nextCatalyst.date', nc.date, {type:'date'}) + '</div></fieldset>' +
    (t ? '' : '<p class="fhint">Revenue and EPS estimates from any current-reality update are stored as the baseline, so later checks can tell a cheaper stock from a worse business.</p>');
  openModal(t ? 'Watchlist — ' + t : 'Add to watchlist', 'watch', body, 'Save', {t});
}
async function saveWatch(form, d){
  const v = readForm(form), t = safeId(v.ticker);
  if (!t) throw new Error('Enter a ticker.');
  const old = S.watchlist[t] || {}, f = S.fundamentals[t] || {};
  const doc = Object.assign({}, old, v, {ticker:t, themes:(v.themes || '').split(',').map(s => s.trim()).filter(Boolean), priceAdded: v.priceAdded ?? v.price, priceAt: v.price !== old.price ? todayISO() : old.priceAt});
  if (!old.baseline) doc.baseline = {date:v.addedAt || todayISO(), price:doc.priceAdded, revEst:f.revEst ?? null, epsEst:f.epsEst ?? null, analystTarget:f.analystTarget ?? v.analystTarget1y ?? null, shares:f.shares ?? null, netDebt:f.netDebt ?? null};
  delete doc.example;
  await store.set('watchlist', t, doc);
  if (!d.t) await store.add('history', {ticker:t, at:doc.addedAt || todayISO(), ts:Date.now(), kind:'WATCH', title:'Added to watchlist at ' + usd(doc.priceAdded) + '; buy below ' + usd(doc.buyBelow), note:doc.whyWant || ''});
  toast('Watchlist saved');
}
function openNote(t){
  const p = S.positions[t] || S.watchlist[t] || {};
  openModal('Add note — ' + t, 'note', fld('Title', 'title', '', {wide:1, ph:'e.g. Major defense contract awarded'}) + fld('Note', 'note', '', {area:1, wide:1, rows:4}) + '<div class="fg">' + fld('Conviction now', 'conviction', p.conviction || '', {options:[['','Unchanged'],'HIGH','MEDIUM','LOW']}) + fld('Thesis status now', 'thesisStatus', '', {options:[['','Unchanged']].concat(THESIS)}) + '</div>', 'Save note', {t});
}
async function saveNote(form, d){
  const v = readForm(form), t = d.t;
  if (!v.title && !v.note) throw new Error('Write a title or a note.');
  const isPos = !!S.positions[t] && G.pos.some(x => x.t === t), col = isPos ? 'positions' : 'watchlist', old = S[col][t];
  const changes = [];
  if (old && ((v.conviction && v.conviction !== old.conviction) || (v.thesisStatus && v.thesisStatus !== old.thesisStatus))) {
    const doc = Object.assign({}, old); if (v.conviction) doc.conviction = v.conviction; if (v.thesisStatus) doc.thesisStatus = v.thesisStatus;
    changes.push(...diffOf(old, doc)); await store.set(col, t, doc);
  }
  await store.add('history', {ticker:t, at:todayISO(), ts:Date.now(), kind:'NOTE', title:v.title || 'Note', note:v.note || '', changes, snapshot: old && isPos ? snapOf(Object.assign({}, old, v.conviction ? {conviction:v.conviction} : {}, v.thesisStatus ? {thesisStatus:v.thesisStatus} : {})) : {}});
  toast('Note saved');
}
function openPrices(){
  const C = G;
  const lines = [...C.pos.map(m => m.t + ' ' + (m.price ?? '') + (m.prev != null ? ' ' + m.prev : '')), ...C.watch.map(m => m.t + ' ' + (m.price ?? '') + (m.prev != null ? ' ' + m.prev : ''))].join('\n');
  openModal('Update prices', 'prices', '<p class="fhint">One line per ticker: <span class="mono">TICKER PRICE [PREVIOUS CLOSE]</span>. The previous close drives the daily change. Nothing is traded; alerts are re-evaluated immediately.' + (mode === 'cloud' && CFG.quotes !== false ? ' Live prices refresh every ' + (num(CFG.refreshMinutes) || 5) + ' minutes; use this for tickers the feed does not cover (such as Tadawul), or to override until the next live update.' : '') + '</p>' + fld('Prices', 'lines', lines, {area:1, wide:1, rows:Math.min(16, C.pos.length + C.watch.length + 2)}) + nfld('USD/SAR rate', 'fx', C.fx), 'Update prices', {});
}
async function savePrices(form){
  const v = readForm(form); let n = 0; const bad = [];
  for (const line of (v.lines || '').split('\n')) {
    const parts = line.trim().split(/[\s,;]+/); if (!parts[0]) continue;
    const t = safeId(parts[0]), price = num(parts[1]), prev = num(parts[2]);
    if (price == null) { bad.push(parts[0]); continue; }
    const col = S.watchlist[t] && !G.pos.some(x => x.t === t) ? 'watchlist' : 'positions';
    const old = S[col][t] || (col === 'positions' && G.pos.some(x => x.t === t) ? {ticker:t} : null);
    if (!old) { bad.push(parts[0]); continue; }
    if (old.price === price && (prev == null || old.prevClose === prev)) continue;
    await store.set(col, t, Object.assign({}, old, {price, prevClose: prev ?? old.prevClose ?? null, priceAt:todayISO(), priceTs:Date.now()})); n++;
  }
  if (v.fx && v.fx !== G.fx) { const s = Object.assign({}, S.settings.main || {}, {fx:v.fx}); await store.set('settings', 'main', s); }
  toast(n + ' price' + (n === 1 ? '' : 's') + ' updated' + (bad.length ? '. Not recognised: ' + bad.join(', ') : ''));
}
function openSettings(){
  const s = cfg();
  const body = '<fieldset><legend>Money</legend><div class="fg">' + nfld('Cash available (SAR)', 'cash', s.cash) + nfld('USD/SAR rate', 'fx', s.fx) + fld('Show totals in', 'display', s.display, {options:['SAR','USD']}) + nfld('Monthly contribution, low (SAR)', 'contribMin', s.contribMin) + nfld('Monthly contribution, high (SAR)', 'contribMax', s.contribMax) + nfld('Contribution day of month', 'contribDay', s.contribDay) +
      nfld('Portfolio value on 1 January (SAR)', 'ytdStart', s.ytdStart) + nfld('Contributions this year (SAR)', 'ytdContrib', s.ytdContrib) + '</div></fieldset>' +
    '<fieldset><legend>My rules</legend><div class="fg">' + nfld('Hurdle for new money: 3Y CAGR (%)', 'hurdle', s.hurdle) + nfld('Minimum 3Y CAGR to keep holding (%)', 'minCagr', s.minCagr) + nfld('Default maximum position (%)', 'maxWeight', s.maxWeight) + nfld('Approaching-trigger distance (%)', 'approachPct', s.approachPct) + '</div></fieldset>' +
    '<fieldset><legend>Benchmark & tax</legend><div class="fg">' + fld('Benchmark', 'benchmark', s.benchmark) + nfld('Benchmark return YTD (%)', 'benchmarkYtd', s.benchmarkYtd) + '</div>' +
      '<label class="chk"><input type="checkbox" name="taxEnabled" id="f-taxEnabled"' + (s.taxEnabled ? ' checked' : '') + '> Show estimated tax on sales</label><div class="fg">' + nfld('Short-term rate (%)', 'taxShort', s.taxShort) + nfld('Long-term rate (%)', 'taxLong', s.taxLong) + '</div></fieldset>' +
    '<fieldset><legend>Notifications</legend><label class="chk"><input type="checkbox" name="notify.inapp" id="f-notify-inapp"' + (s.notify.inapp ? ' checked' : '') + '> In-app alerts</label><label class="chk"><input type="checkbox" name="notify.push" id="f-notify-push"' + (s.notify.push ? ' checked' : '') + '> Push notifications</label><label class="chk"><input type="checkbox" name="notify.email" id="f-notify-email"' + (s.notify.email ? ' checked' : '') + '> Email</label>' +
      '<p class="fhint">Push and email need the daily server check, which is not switched on yet; your choice is saved for when it is. SMS, WhatsApp and Telegram are not available yet.</p></fieldset>' +
    '<fieldset><legend>Market / macro notes</legend>' + fld('Notes', 'marketNotes', s.marketNotes, {area:1, wide:1, rows:4}) + '</fieldset>' +
    '<fieldset><legend>Account & data</legend>' +
      (mode === 'cloud' ? '<p class="fhint">Signed in as <strong>' + esc(user.email) + '</strong>. Your records are private to your account and sync across your devices.</p>' : '<p class="fhint">Guest mode: records are saved on this device only.' + (cloudReady ? '' : ' Cloud sync is not configured on this server.') + '</p>') +
      '<div class="btns">' + (mode === 'cloud' ? '<button type="button" class="btn" data-act="signout">Sign out</button>' : (cloudReady ? '<button type="button" class="btn pri" data-act="to-auth">Create account / sign in</button>' : '')) +
        '<button type="button" class="btn" data-act="export">Download backup</button><label class="btn" for="import-file">Restore from backup</label><input type="file" id="import-file" accept="application/json,.json" hidden>' +
        (hasExamples() ? '' : '<button type="button" class="btn" data-act="load-examples">Load example portfolio</button>') + '</div>' +
      '<div class="btns"><button type="button" class="btn warn" data-act="delete-all">' + (mode === 'cloud' ? 'Delete my account and all data' : 'Delete all data on this device') + '</button></div>' +
      '<p class="fhint">Install: on iPhone, open in Safari, tap Share, then Add to Home Screen. On Android or desktop Chrome, use Install app in the top bar or the browser menu.</p>' +
      '<p class="fhint">Thesis Ledger is a decision journal, not investment advice.</p></fieldset>';
  openModal('Settings', 'settings', body, 'Save settings', {});
}
async function saveSettings(form){
  const v = readForm(form), s = Object.assign({}, S.settings.main || {}, v); delete s.example;
  await store.set('settings', 'main', s); ui.display = null; saveUI(); toast('Settings saved');
}

/* ------------------------------------------------------------------ */
/* AI                                                                  */
/* ------------------------------------------------------------------ */
function compactHistory(t){ return historyFor(t).map(h => ({date:h.at, kind:h.kind, title:h.title, note:h.note, changes:h.changes, believed: h.snapshot ? {thesisStatus:h.snapshot.thesisStatus, conviction:h.snapshot.conviction, model:h.snapshot.model, reviewPrice:h.snapshot.plan && h.snapshot.plan.reviewPrice, addBelow:h.snapshot.plan && h.snapshot.plan.addBelow} : undefined})); }
function memoryFor(t){
  const C = G, m = C.pos.find(x => x.t === t), w = C.watch.find(x => x.t === t);
  const lessons = Object.values(S.closed).map(c => ({ticker:c.ticker, quality:c.quality, mistakes:c.mistakes, lesson:c.lessons}));
  const rules = {hurdlePct:num(C.set.hurdle), minCagrPct:num(C.set.minCagr), defaultMaxWeightPct:num(C.set.maxWeight)};
  if (m) {
    const orig = originalOf(t, m.p);
    return {type:'holding', ticker:t, company:m.p.company, today:todayISO(), rules,
      position:{shares:m.shares, avgCost:m.avg, price:m.price, priceAsOf:m.p.priceAt, firstBought:m.start, returnPct:m.gainPct, weightPct:m.weight, targetWeightPct:m.targetW, maxWeightPct:m.maxW},
      originalDecision: orig || 'No thesis history recorded', currentThesis:m.p.thesis || 'NOT RECORDED', thesisStatus:m.thesis, dataSuggestedStatus:m.autoThesis, conviction:m.p.conviction, classifications:m.p.classifications,
      plan:m.plan, currentModel:{fairValue:m.fair, v3y:m.v3y, v5y:m.v5y, expected3yCagrPct:m.cagr3y}, baselineAtPurchase:m.base, currentReality:m.f, changesSincePurchasePct:m.d,
      expectationsVsReality:m.expect.map(x => ({expectation:x.e.label, metric:x.e.metric, threshold:x.e.metric ? x.e.op + ' ' + x.e.value : null, current:x.v, met:x.ok})),
      conditions:m.conds.map(r => ({label:r.c.label, category:r.c.category, type:r.c.type, rule:condText(r.c), current:r.v, triggered:r.ok, near:r.near})),
      engine:{status:m.status, action:m.rec.action, reasons:m.rec.reasons, buyCheck: m.ver ? {code:m.ver.code, verdict:VER[m.ver.code].pverdict, failed:m.ver.checks.filter(c => c.ok === false)} : null},
      decisionHistory:compactHistory(t), transactions:m.L.txs, mistakeLibrary:C.mistakes, pastLessons:lessons, mistakeWarnings:m.lessons.map(l => l[1])};
  }
  if (w) return {type:'watchlist', ticker:t, today:todayISO(), rules, watchlistEntry:w.w, currentReality:w.f, changesSinceAddedPct:w.d, status:w.wstatus, buyCheck: w.ver ? {code:w.ver.code, verdict:VER[w.ver.code].verdict, checks:w.ver.checks} : null, updatedBuy:w.updatedBuy, expected3yCagrPct:w.cagr3y, history:compactHistory(t), mistakeLibrary:C.mistakes, pastLessons:lessons};
  return {type:'portfolio', today:todayISO(), rules, totals:C.totals,
    holdings:C.pos.map(m => ({t:m.t, whyOwn:(m.p.thesis || {}).whyOwn, thesis:m.thesis, status:m.status, action:m.rec.action, weightPct:+m.weight.toFixed(1), returnPct:m.gainPct && +m.gainPct.toFixed(1), cagr3y:m.cagr3y && +m.cagr3y.toFixed(1), hasThesis:m.hasThesis})),
    watchlist:C.watch.map(m => ({t:m.t, price:m.price, buyBelow:m.buy, status:m.wstatus, cagr3y:m.cagr3y && +m.cagr3y.toFixed(1)})), activeAlerts:C.alerts.filter(a => a.sev !== 'INFO').map(a => ({t:a.t, sev:a.sev, title:a.title, action:a.action})), mistakeLibrary:C.mistakes, pastLessons:lessons};
}
function buildPrompt(t, q){
  const data = memoryFor(t === '_portfolio' ? null : t);
  return 'You are the investor\'s personal investment analyst inside their decision journal. Answer from THEIR recorded framework, not with a generic stock analysis.\n\n' +
    'Rules:\n' +
    '- Start from why they bought: "You bought X at $Y because you expected ...". Quote their recorded thesis and expectations. Never invent reasons they did not record; if something is not recorded, say so and suggest they record it.\n' +
    '- Compare each original expectation with current reality and say whether it improved, is unchanged, or deteriorated.\n' +
    '- Refer to their review price, add levels, sell conditions, position-size rules and past mistakes where relevant.\n' +
    '- Distinguish a price target being reached from the thesis being completed, and a stock becoming cheaper from a business becoming worse.\n' +
    '- Never recommend selling only because the price rose, and never recommend holding only to get back to the purchase price.\n' +
    '- You cannot browse. The data below is everything you have and may be stale (check the dates). Say what is missing.\n' +
    '- End with: "Based on your own investment framework, the original reason for owning ' + (t === '_portfolio' ? 'these positions' : t) + ' remains intact / has weakened / has broken." Pick one. Then state the engine\'s suggested action and whether you agree, with one reason.\n' +
    '- Under 320 words. Plain text, short paragraphs, "- " bullets. No tables.\n\n' +
    'Question: ' + q + '\n\nLEDGER DATA (JSON):\n' + JSON.stringify(data).slice(0, 56000);
}
async function ask(key, q){
  if (!aiOn() || !q || !q.trim()) return;
  if (ai[key] && ai[key].busy) return;
  const st = ai[key] = {q:q.trim(), text:'', busy:true, err:null};
  render();
  try {
    const r = await callFn('analyst', {prompt:buildPrompt(key, st.q)});
    if (ai[key] !== st) return;
    st.text = r.text || ''; if (r.truncated) st.err = 'truncated';
  } catch (e) { st.err = (e && e.code) || 'upstream_error'; }
  st.busy = false; render();
}

/* ------------------------------------------------------------------ */
/* Events                                                              */
/* ------------------------------------------------------------------ */
let toastTimer;
function toast(msg){ const el = $('#toast'); el.textContent = msg; el.hidden = false; clearTimeout(toastTimer); toastTimer = setTimeout(() => el.hidden = true, 3200); }
function arm(el, label, fn){
  if (el.dataset.armed) { fn(); return; }
  el.dataset.armed = '1'; const orig = el.textContent; el.textContent = label;
  setTimeout(() => { if (el.isConnected) { delete el.dataset.armed; el.textContent = orig; } }, 4000);
}
async function removeExamples(){
  let n = 0;
  for (const c of COLS) for (const [id, d] of Object.entries(S[c])) if (d && d.example) { await store.del(c, id); n++; }
  toast(n + ' example records removed');
}
document.addEventListener('click', e => {
  const el = e.target.closest('[data-act]'); if (!el) return;
  const act = el.dataset.act, t = el.dataset.t;
  const run = p => Promise.resolve(p).catch(err => toast('Could not save: ' + ((err && (err.code || err.message)) || 'unknown error')));
  switch (act) {
    case 'collapse': ui.collapsed[t] = !ui.collapsed[t]; saveUI(); render(); break;
    case 'collapse-all': { const all = G.pos.every(m => ui.collapsed[m.t]); G.pos.forEach(m => ui.collapsed[m.t] = !all); saveUI(); render(); break; }
    case 'display': ui.display = el.dataset.v; saveUI(); render(); break;
    case 'alloc': ui.alloc = +el.dataset.v; render(); break;
    case 'inbox': ui.inbox = el.dataset.v; render(); break;
    case 'ack': run(store.set('alertState', safeId(el.dataset.key), {sig:el.dataset.sig, ack:true, at:new Date().toISOString()})); break;
    case 'unack': run(store.del('alertState', safeId(el.dataset.key))); break;
    case 'thesis': openThesis(t, el.dataset.focus); break;
    case 'fund': openFund(t); break;
    case 'tx': openTx(el.dataset.side, t); break;
    case 'prices': openPrices(); break;
    case 'watch-edit': openWatch(t); break;
    case 'note': openNote(t); break;
    case 'settings': openSettings(); break;
    case 'scorecard': { const c = S.closed[el.dataset.id]; if (c) openScorecard(c.ticker, el.dataset.id); break; }
    case 'close-modal': closeModal(); break;
    case 'add-row': { const box = $('#rows-' + el.dataset.kind); box.insertAdjacentHTML('beforeend', el.dataset.kind === 'exp' ? expRow(null, t) : condRow(null, t)); const r = box.lastElementChild.querySelector('input'); if (r) r.focus(); break; }
    case 'del-row': el.closest('.frow').remove(); break;
    case 'ask': ask(t, el.dataset.q); break;
    case 'guest': try { localStorage.setItem('tl-guest', 'true'); } catch(e){} startLocal(); break;
    case 'to-auth': closeModal(); try { localStorage.removeItem('tl-guest'); } catch(e){} user = null; clearState(); mode = 'auth'; render(); break;
    case 'auth-back': auth.step = 'email'; auth.msg = ''; render(); break;
    case 'load-examples': closeModal(); run(loadExamples()); break;
    case 'import-guest': run(importGuest()); break;
    case 'dismiss-import': ui.offerImport = false; render(); break;
    case 'export': exportData(); break;
    case 'signout': run(signOut()); break;
    case 'delete-all': arm(el, 'Click again to delete everything', () => run(deleteEverything())); break;
    case 'install': if (installEvt) { installEvt.prompt(); installEvt.userChoice.finally(() => { installEvt = null; schedule(); }); } break;
    case 'reload': location.reload(); break;
    case 'shot': openShot(); break;
    case 'shot-rm': { const f = shot.files.splice(+el.dataset.i, 1)[0]; if (f) URL.revokeObjectURL(f.url); $('.mbody').innerHTML = shotBody(); break; }
    case 'draft': draftThesis(t); break;
    case 'rm-examples': arm(el, 'Click again to remove', () => run(removeExamples())); break;
    case 'del-watch': arm(el, 'Click again to remove', () => run(store.del('watchlist', t).then(() => { location.hash = 'home'; toast(t + ' removed from watchlist'); }))); break;
  }
});
document.addEventListener('change', e => {
  const id = e.target.id;
  if (id === 'wsort') { ui.wsort = e.target.value; saveUI(); render(); }
  else if (id === 'alloc-custom') { const v = num(e.target.value); if (v && v > 0) { ui.alloc = Math.round(v); render(); } }
  else if (id === 'hist-filter') { ui.histFilter = e.target.value; render(); }
  else if (id === 'import-file' && e.target.files[0]) importData(e.target.files[0]);
  else if (id === 'shot-files') addShotFiles(e.target.files);
  else if (e.target.classList && e.target.classList.contains('shot-ck')) { shot.rows[+e.target.dataset.i].checked = e.target.checked; const b = $('#mform button[type=submit]'); if (b && shot.result) b.textContent = 'Apply ' + shot.rows.filter(r => r.checked).length + ' changes'; }
});
document.addEventListener('submit', async e => {
  const f = e.target;
  if (f.id === 'authform') { e.preventDefault(); submitAuth().catch(x => { auth.busy = false; auth.msg = (x && x.message) || 'Could not reach the server.'; render(); }); return; }
  if (f.dataset.ask) { e.preventDefault(); const inp = f.querySelector('input'); ask(f.dataset.ask, inp.value); return; }
  if (f.id !== 'mform') return;
  e.preventDefault();
  const d = $('#modal')._data || {}, kind = f.dataset.kind, btn = f.querySelector('button[type=submit]'), err = $('#merr');
  btn.disabled = true; err.textContent = '';
  try {
    if (kind === 'shot' && !shot.result) { await readShot(); btn.disabled = false; return; }
    const H = {shot:applyShot, thesis:saveThesis, fund:saveFund, tx:saveTx, score:saveScore, watch:saveWatch, note:saveNote, prices:savePrices, settings:saveSettings}[kind];
    const r = await H(f, d);
    if (r !== 'closed') closeModal();
  } catch (x) { err.textContent = (x && (x.message || x.code)) || 'Could not save.'; btn.disabled = false; }
});
document.addEventListener('paste', e => { if ($('#mform[data-kind="shot"]') && !shot.result) { const fs = [...(e.clipboardData || {}).files || []]; if (fs.length) { e.preventDefault(); addShotFiles(fs); } } });
document.addEventListener('dragover', e => { if ($('#shot-drop')) e.preventDefault(); });
document.addEventListener('drop', e => { if ($('#shot-drop') && !shot.result) { e.preventDefault(); addShotFiles(e.dataTransfer.files); } });
document.addEventListener('keydown', e => { if (e.key === 'Escape' && !$('#modal').hidden) closeModal(); });

/* ------------------------------------------------------------------ */
/* Boot                                                                */
/* ------------------------------------------------------------------ */
function renderAuth(){
  const codeStep = auth.step === 'code';
  return '<div class="auth"><div class="auth-card">' +
    '<div class="brand" style="font-size:22px"><span class="mark" aria-hidden="true"></span>Thesis Ledger</div>' +
    '<p class="auth-lead">Remember why you bought every position, what would make you sell, and whether that reason is still true.</p>' +
    '<ul class="auth-points"><li>Every buy tied to a written thesis and exit plan</li><li>Alerts only when a thesis, price or size condition is met</li><li>A price target reached is reviewed, never sold automatically</li></ul>' +
    '<form id="authform" class="auth-form" novalidate>' +
      (codeStep
        ? '<label class="fld"><span>Enter the 6-digit code sent to ' + esc(auth.email) + '</span><input id="auth-code" inputmode="numeric" autocomplete="one-time-code" maxlength="10" placeholder="123456"></label>' +
          '<button class="btn pri" type="submit"' + (auth.busy ? ' disabled' : '') + '>' + (auth.busy ? 'Checking…' : 'Sign in') + '</button>' +
          '<button class="btn ghost" type="button" data-act="auth-back">Use a different email</button>' +
          '<p class="fhint">You can also open the link in the email on this device.</p>'
        : '<label class="fld"><span>Email</span><input id="auth-email" type="email" autocomplete="email" placeholder="you@example.com" value="' + esc(auth.email) + '"></label>' +
          '<button class="btn pri" type="submit"' + (auth.busy ? ' disabled' : '') + '>' + (auth.busy ? 'Sending…' : 'Email me a sign-in code') + '</button>' +
          '<p class="fhint">No password. New here? The same code creates your account. Your portfolio is private to you.</p>') +
      '<p class="err" role="alert">' + esc(auth.msg) + '</p>' +
    '</form>' +
    '<div class="auth-alt"><button class="btn ghost" data-act="guest">Try it without an account</button><span class="fhint">Data stays on this device only.</span></div>' +
  '</div></div>';
}
async function submitAuth(){
  auth.msg = '';
  if (auth.step === 'email') {
    const email = ($('#auth-email').value || '').trim();
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) { auth.msg = 'Enter a valid email address.'; render(); return; }
    auth.email = email; auth.busy = true; render();
    const {error} = await sb.auth.signInWithOtp({email, options:{emailRedirectTo: location.origin + location.pathname}});
    auth.busy = false;
    if (error) auth.msg = error.message; else auth.step = 'code';
    render(); const c = $('#auth-code'); if (c) c.focus();
  } else {
    const token = ($('#auth-code').value || '').replace(/\s/g, '');
    if (!token) { auth.msg = 'Enter the code from the email.'; render(); return; }
    auth.busy = true; render();
    const {data, error} = await sb.auth.verifyOtp({email:auth.email, token, type:'email'});
    auth.busy = false;
    if (error) { auth.msg = error.message.includes('expired') ? 'That code has expired or is wrong. Request a new one.' : error.message; render(); return; }
    auth.step = 'email'; startCloud(data.user);
  }
}
async function loadExamples(){
  try {
    const d = await (await fetch('examples.json', {cache:'no-cache'})).json();
    for (const c of COLS) for (const [id, doc] of Object.entries(d[c] || {})) if (!S[c][id]) await store.set(c, id, doc);
    toast('Example portfolio loaded. Remove it any time from the banner.');
  } catch (e) { toast('Could not load the examples.'); }
}
function exportData(){
  const blob = new Blob([JSON.stringify({app:'thesis-ledger', exportedAt:new Date().toISOString(), data:S}, null, 1)], {type:'application/json'});
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'thesis-ledger-' + todayISO() + '.json';
  document.body.appendChild(a); a.click(); setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 500);
}
async function importData(file){
  try {
    const j = JSON.parse(await file.text()), d = j.data || j; let n = 0;
    for (const c of COLS) for (const [id, doc] of Object.entries(d[c] || {})) { if (doc && typeof doc === 'object') { await store.set(c, id, doc); n++; } }
    closeModal(); toast(n + ' records imported');
  } catch (e) { toast('That file is not a Thesis Ledger backup.'); }
}
async function deleteEverything(){
  if (mode === 'cloud') {
    try { await callFn('delete-account', {}); }
    catch (e) {
      const {error} = await sb.from('docs').delete().eq('user_id', user.id);
      if (error) { toast('Could not delete: ' + error.message); return; }
    }
    outbox = []; saveOutbox(); toast('Your data was deleted.'); await signOut(); return;
  }
  try { localStorage.removeItem(GUEST_KEY); } catch(e){}
  clearState(); closeModal(); render(); toast('Data on this device was deleted.');
}
/* Live prices: the app asks the server every few minutes while open; the server
   refreshes stale quotes from the market-data provider and pushes changes back
   over realtime to every device that tracks the same tickers. */
const refreshMs = () => Math.max(1, num(CFG.refreshMinutes) || 5) * 60000;
function trackedSymbols(){ return [...new Set([...Object.keys(S.positions), ...Object.keys(S.watchlist), ...Object.values(S.transactions).map(t => t && t.ticker)])].filter(s => /^[A-Z0-9.\-]{1,12}$/.test(s || '')).sort(); }
let mkChannel = null, mkKey = '';
async function loadMarket(){
  if (mode !== 'cloud') return;
  const syms = trackedSymbols(); if (!syms.length) return;
  const [q, md] = await Promise.all([sb.from('quotes').select('*').in('symbol', syms), sb.from('market_data').select('*').in('symbol', syms)]);
  const newer = (cur, r) => !cur || Date.parse(r.updated_at) >= Date.parse(cur.updated_at);
  (q.data || []).forEach(r => { if (newer(MK.quotes[r.symbol], r)) MK.quotes[r.symbol] = r; });
  (md.data || []).forEach(r => { if (newer(MK.market[r.symbol], r)) MK.market[r.symbol] = r; });
  const key = syms.join(',');
  if (key !== mkKey) {
    mkKey = key; if (mkChannel) sb.removeChannel(mkChannel);
    const filter = 'symbol=in.(' + syms.slice(0, 100).join(',') + ')';
    mkChannel = sb.channel('mk-' + user.id + '-' + Date.now())
      .on('postgres_changes', {event:'*', schema:'public', table:'quotes', filter}, p => { if (p.new && p.new.symbol) { MK.quotes[p.new.symbol] = p.new; schedule(); } })
      .on('postgres_changes', {event:'*', schema:'public', table:'market_data', filter}, p => { if (p.new && p.new.symbol) { MK.market[p.new.symbol] = p.new; schedule(); } })
      .subscribe();
  }
  schedule();
}
async function pollQuotes(force){
  if (mode !== 'cloud' || CFG.quotes === false || document.visibilityState !== 'visible' || !navigator.onLine) return;
  const syms = trackedSymbols(); if (!syms.length) return;
  if (!force && MK.lastPoll && Date.now() - MK.lastPoll < refreshMs() - 5000) return;
  MK.lastPoll = Date.now();
  try { const r = await callFn('quotes', {symbols:syms}); Object.entries(r.quotes || {}).forEach(([s, q]) => MK.quotes[s] = q); MK.error = null; }
  catch (e) { MK.error = e.code === 'not_configured' ? 'Live prices not set up on the server' : e.code === 'offline' ? 'Offline' : 'Price update failed; retrying'; }
  await loadMarket().catch(() => {});
  schedule();
}

/* Portfolio screenshot import: Claude reads the broker screen, the app shows the
   differences against the ledger, and the user applies them with one tap. */
const shot = {files:[], result:null, rows:[], busy:false};
function openShot(){
  shot.files = []; shot.result = null; shot.rows = []; shot.busy = false;
  openModal('Import portfolio screenshot', 'shot', shotBody(), 'Read screenshot', {});
}
function shotBody(){
  if (!shot.result) return '<p class="fhint">Take screenshots of your broker\'s holdings screen (for example Al Rajhi Capital, Derayah, SNB Capital, Interactive Brokers, Robinhood). Scroll and add up to 5 if the list is long. Show shares, average cost and price if your broker can.</p>' +
    '<label class="drop" for="shot-files" id="shot-drop"><strong>Choose screenshots</strong><span class="fhint">or drop / paste images here</span><input type="file" id="shot-files" accept="image/*" multiple hidden></label>' +
    '<div class="thumbs" id="shot-thumbs">' + shot.files.map((f, i) => '<div class="thumb"><img alt="Screenshot ' + (i + 1) + '" src="' + f.url + '"><button type="button" class="icon" data-act="shot-rm" data-i="' + i + '" aria-label="Remove">×</button></div>').join('') + '</div>' +
    '<p class="fhint">Screenshots are sent to Claude to read the numbers and are not stored. Nothing changes in your ledger until you review and apply.</p>';
  const pf = shot.result, fx = G.fx;
  const ch = (r, i) => '<input type="checkbox" data-i="' + i + '" class="shot-ck"' + (r.checked ? ' checked' : '') + (r.action === 'same' || r.action === 'skip' ? ' disabled' : '') + ' aria-label="Apply ' + esc(r.t) + '">';
  const label = {new:chip('NEW POSITION','g','nd'), buy:chip('BOUGHT MORE','g','nd'), sell:chip('SOLD SOME','o','nd'), price:chip('PRICE ONLY','n','nd'), same:chip('NO CHANGE','n','nd'), missing:chip('NOT IN SCREENSHOT','y','nd'), skip:chip('UNREADABLE','n','nd'), cost:chip('COST DIFFERS','y','nd')};
  return '<div class="precheck">' + kv([['Broker', pf.broker || '—'], ['As of', pf.as_of || '—'], ['Holdings read', String((pf.holdings || []).length)], ['Total value', pf.total_value != null ? fmtN(pf.total_value, 0) + ' ' + (pf.base_currency || '') : '—']]) + (pf.notes ? '<p class="fhint">' + esc(pf.notes) + '</p>' : '') + '</div>' +
    '<div class="tscroll"><table class="tbl"><thead><tr><th></th><th>Ticker</th><th>Change</th><th class="n">Broker shares</th><th class="n">Ledger shares</th><th class="n">Broker avg cost</th><th class="n">Price</th></tr></thead><tbody>' +
    shot.rows.map((r, i) => '<tr class="' + (r.h && r.h.confidence === 'low' ? 'hl-y' : '') + '"><td>' + ch(r, i) + '</td><td><strong class="mono">' + esc(r.t) + '</strong><div class="muted" style="font-size:11.5px">' + esc((r.h && r.h.name) || (r.m && r.m.p.company) || '') + (r.h && r.h.confidence === 'low' ? ' · check ticker' : '') + '</div></td><td>' + label[r.action] + '<div class="muted" style="font-size:11.5px">' + esc(r.note || '') + '</div></td><td class="n">' + (r.h ? fmtN(num(r.h.shares), 2).replace(/\.00$/, '') : '—') + '</td><td class="n">' + (r.m ? fmtN(r.m.shares, 2).replace(/\.00$/, '') : '—') + '</td><td class="n">' + (r.h ? px(num(r.h.avg_cost), r.cur) : '—') + '</td><td class="n">' + (r.h ? px(num(r.h.last_price), r.cur) : px(r.m && r.m.price, r.cur)) + '</td></tr>').join('') +
    '</tbody></table></div>' +
    (pf.cash != null ? '<label class="chk"><input type="checkbox" id="shot-cash" checked> Set available cash to ' + fmtN(pf.cash, 0) + ' ' + esc(pf.base_currency || 'SAR') + (pf.base_currency === 'USD' ? ' (SAR ' + fmtN(pf.cash * fx, 0) + ')' : '') + '</label>' : '') +
    '<p class="fhint">Share differences are recorded as buy or sell transactions dated today, priced so your average cost matches the broker. New positions will ask you for a thesis. Rows marked "check ticker" are unticked: fix them in the ledger if needed.</p>';
}
async function fileToJpeg(file){
  const url = URL.createObjectURL(file);
  try {
    const img = await new Promise((ok, no) => { const i = new Image(); i.onload = () => ok(i); i.onerror = no; i.src = url; });
    const s = Math.min(1, 1800 / Math.max(img.naturalWidth, img.naturalHeight));
    const c = document.createElement('canvas'); c.width = Math.round(img.naturalWidth * s); c.height = Math.round(img.naturalHeight * s);
    c.getContext('2d').drawImage(img, 0, 0, c.width, c.height);
    const dataUrl = c.toDataURL('image/jpeg', 0.85);
    return {media_type:'image/jpeg', data:dataUrl.split(',')[1]};
  } finally { URL.revokeObjectURL(url); }
}
function addShotFiles(list){
  for (const f of list) if (f && f.type && f.type.startsWith('image/') && shot.files.length < 5) shot.files.push({file:f, url:URL.createObjectURL(f)});
  $('.mbody').innerHTML = shotBody();
}
function reconcile(pf){
  const C = G, rows = [], seen = new Set();
  (pf.holdings || []).forEach(h => {
    const t = safeId(String(h.symbol || '').replace(/\s+/g, '')); if (!t || seen.has(t)) return; seen.add(t);
    const m = C.pos.find(x => x.t === t), sh = num(h.shares), avg = num(h.avg_cost), cur = (h.currency === 'SAR' || /\.SR$/.test(t)) ? 'SAR' : (m ? m.cur : 'USD');
    let action, note = '', diff = 0;
    if (!m) { if (sh > 0) { action = 'new'; note = sh + ' shares' + (avg ? ' at ' + px(avg, cur) + ' avg' : ''); } else { action = 'skip'; note = 'Share count not visible'; } }
    else if (sh == null) { action = num(h.last_price) ? 'price' : 'skip'; note = 'Share count not visible'; }
    else { diff = sh - m.shares;
      if (Math.abs(diff) < 1e-6) { action = (avg && m.avg && Math.abs(avg - m.avg) / m.avg > 0.01) ? 'cost' : 'same'; if (action === 'cost') note = 'Broker ' + px(avg, cur) + ' vs ledger ' + px(m.avg, cur) + '; left as is'; }
      else { action = diff > 0 ? 'buy' : 'sell'; note = (diff > 0 ? '+' : '') + fmtN(diff, Math.abs(diff) % 1 ? 2 : 0) + ' shares'; } }
    rows.push({t, h, m, cur, action, diff, note, checked: !['same','skip','cost'].includes(action) && h.confidence !== 'low'});
  });
  C.pos.filter(m => !seen.has(m.t)).forEach(m => rows.push({t:m.t, m, cur:m.cur, action:'missing', note:'Tick to record a sale of all ' + fmtN(m.shares, 0) + ' shares', checked:false}));
  return rows;
}
async function readShot(){
  if (!shot.files.length) throw new Error('Add at least one screenshot.');
  const images = []; for (const f of shot.files) images.push(await fileToJpeg(f.file));
  const r = await callFn('read-portfolio', {images}).catch(e => { throw new Error(aiErr(e.code)); });
  shot.result = r.portfolio; shot.rows = reconcile(shot.result);
  $('.mbody').innerHTML = shotBody();
  $('#mform button[type=submit]').textContent = 'Apply ' + shot.rows.filter(r => r.checked).length + ' changes';
}
async function applyShot(){
  const today = todayISO(), now = Date.now(), fx = G.fx; let n = 0;
  document.querySelectorAll('.shot-ck').forEach(el => { shot.rows[+el.dataset.i].checked = el.checked; });
  for (const r of shot.rows.filter(x => x.checked)) {
    const h = r.h || {}, lastPx = num(h.last_price), sh = num(h.shares), avg = num(h.avg_cost);
    const doc = Object.assign({ticker:r.t}, S.positions[r.t] || {});
    if (!doc.company && h.name) doc.company = h.name;
    if (!doc.currency) doc.currency = r.cur;
    if (lastPx) Object.assign(doc, {price:lastPx, priceAt:today, priceTs:now});
    const tx = (side, shares, price, kind) => store.add('transactions', {ticker:r.t, side, date:today, shares, price, fees:0, currency:r.cur, note:'Imported from portfolio screenshot', reason: side === 'SELL' ? 'Other' : ''})
      .then(() => store.add('history', {ticker:r.t, at:today, ts:Date.now(), kind, title:(side === 'SELL' ? 'Sold ' : kind === 'BUY' ? 'Bought ' : 'Added ') + fmtN(shares, shares % 1 ? 2 : 0) + ' at ' + px(price, r.cur) + ' (from screenshot)', note:'Reconciled with ' + (shot.result.broker || 'broker') + ' screenshot', snapshot: S.positions[r.t] ? snapOf(S.positions[r.t]) : {}, price}));
    if (r.action === 'new') await tx('BUY', sh, avg || lastPx, 'BUY');
    else if (r.action === 'buy') { let p = avg ? (sh * avg - r.m.L.cost) / r.diff : null; if (!(p > 0)) p = lastPx || r.m.price; await tx('BUY', r.diff, p, 'ADD'); }
    else if (r.action === 'sell') await tx('SELL', -r.diff, lastPx || r.m.price, 'SELL');
    else if (r.action === 'missing') await tx('SELL', r.m.shares, r.m.price, 'SELL');
    await store.set('positions', r.t, doc); n++;
  }
  const cashEl = $('#shot-cash');
  if (cashEl && cashEl.checked && shot.result.cash != null) { const s = Object.assign({}, S.settings.main || {}); s.cash = shot.result.base_currency === 'USD' ? shot.result.cash * fx : shot.result.cash; delete s.example; await store.set('settings', 'main', s); }
  shot.files.forEach(f => URL.revokeObjectURL(f.url));
  toast(n + ' holdings updated from your screenshot');
  setTimeout(() => pollQuotes(true), 500);
}

/* ------------------------------------------------------------------ */
/* Boot                                                                */
/* ------------------------------------------------------------------ */
function renderAuth(){
  const codeStep = auth.step === 'code';
  return '<div class="auth"><div class="auth-card">' +
    '<div class="brand" style="font-size:22px"><span class="mark" aria-hidden="true"></span>Thesis Ledger</div>' +
    '<p class="auth-lead">Remember why you bought every position, what would make you sell, and whether that reason is still true.</p>' +
    '<ul class="auth-points"><li>Every buy tied to a written thesis and exit plan</li><li>Alerts only when a thesis, price or size condition is met</li><li>A price target reached is reviewed, never sold automatically</li></ul>' +
    '<form id="authform" class="auth-form" novalidate>' +
      (codeStep
        ? '<label class="fld"><span>Enter the 6-digit code sent to ' + esc(auth.email) + '</span><input id="auth-code" inputmode="numeric" autocomplete="one-time-code" maxlength="10" placeholder="123456"></label>' +
          '<button class="btn pri" type="submit"' + (auth.busy ? ' disabled' : '') + '>' + (auth.busy ? 'Checking…' : 'Sign in') + '</button>' +
          '<button class="btn ghost" type="button" data-act="auth-back">Use a different email</button>' +
          '<p class="fhint">You can also open the link in the email on this device.</p>'
        : '<label class="fld"><span>Email</span><input id="auth-email" type="email" autocomplete="email" placeholder="you@example.com" value="' + esc(auth.email) + '"></label>' +
          '<button class="btn pri" type="submit"' + (auth.busy ? ' disabled' : '') + '>' + (auth.busy ? 'Sending…' : 'Email me a sign-in code') + '</button>' +
          '<p class="fhint">No password. New here? The same code creates your account. Your portfolio is private to you.</p>') +
      '<p class="err" role="alert">' + esc(auth.msg) + '</p>' +
    '</form>' +
    '<div class="auth-alt"><button class="btn ghost" data-act="guest">Try it without an account</button><span class="fhint">Data stays on this device only.</span></div>' +
  '</div></div>';
}
async function submitAuth(){
  auth.msg = '';
  if (auth.step === 'email') {
    const email = ($('#auth-email').value || '').trim();
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) { auth.msg = 'Enter a valid email address.'; render(); return; }
    auth.email = email; auth.busy = true; render();
    const {error} = await sb.auth.signInWithOtp({email, options:{emailRedirectTo: location.origin + location.pathname}});
    auth.busy = false;
    if (error) auth.msg = error.message; else auth.step = 'code';
    render(); const c = $('#auth-code'); if (c) c.focus();
  } else {
    const token = ($('#auth-code').value || '').replace(/\s/g, '');
    if (!token) { auth.msg = 'Enter the code from the email.'; render(); return; }
    auth.busy = true; render();
    const {data, error} = await sb.auth.verifyOtp({email:auth.email, token, type:'email'});
    auth.busy = false;
    if (error) { auth.msg = error.message.includes('expired') ? 'That code has expired or is wrong. Request a new one.' : error.message; render(); return; }
    auth.step = 'email'; startCloud(data.user);
  }
}
async function loadExamples(){
  try {
    const d = await (await fetch('examples.json', {cache:'no-cache'})).json();
    for (const c of COLS) for (const [id, doc] of Object.entries(d[c] || {})) if (!S[c][id]) await store.set(c, id, doc);
    toast('Example portfolio loaded. Remove it any time from the banner.');
  } catch (e) { toast('Could not load the examples.'); }
}
function exportData(){
  const blob = new Blob([JSON.stringify({app:'thesis-ledger', exportedAt:new Date().toISOString(), data:S}, null, 1)], {type:'application/json'});
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'thesis-ledger-' + todayISO() + '.json';
  document.body.appendChild(a); a.click(); setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 500);
}
async function importData(file){
  try {
    const j = JSON.parse(await file.text()), d = j.data || j; let n = 0;
    for (const c of COLS) for (const [id, doc] of Object.entries(d[c] || {})) { if (doc && typeof doc === 'object') { await store.set(c, id, doc); n++; } }
    closeModal(); toast(n + ' records imported');
  } catch (e) { toast('That file is not a Thesis Ledger backup.'); }
}
async function deleteEverything(){
  if (mode === 'cloud') {
    try { await callFn('delete-account', {}); }
    catch (e) {
      const {error} = await sb.from('docs').delete().eq('user_id', user.id);
      if (error) { toast('Could not delete: ' + error.message); return; }
    }
    outbox = []; saveOutbox(); toast('Your data was deleted.'); await signOut(); return;
  }
  try { localStorage.removeItem(GUEST_KEY); } catch(e){}
  clearState(); closeModal(); render(); toast('Data on this device was deleted.');
}
async function refreshQuotes(btn){
  const syms = [...new Set([...G.pos.map(m => m.t), ...G.watch.map(m => m.t)])];
  if (!syms.length) return;
  if (btn) { btn.disabled = true; btn.textContent = 'Refreshing…'; }
  try {
    const r = await callFn('quotes', {symbols:syms});
    let n = 0;
    for (const [t, q] of Object.entries(r.quotes || {})) {
      if (!q || !(q.price > 0)) continue;
      const col = G.pos.some(m => m.t === t) ? 'positions' : 'watchlist';
      const old = S[col][t] || {ticker:t};
      if (old.price === q.price && old.prevClose === q.prevClose) continue;
      await store.set(col, t, Object.assign({}, old, {price:q.price, prevClose:q.prevClose ?? old.prevClose ?? null, priceAt:todayISO()})); n++;
    }
    const miss = syms.filter(s => !(r.quotes || {})[s]);
    toast(n + ' prices refreshed' + (miss.length ? '. Update manually: ' + miss.join(', ') : ''));
  } catch (e) { toast(e.code === 'not_configured' ? 'Live prices are not set up on this server. Use Update prices.' : e.code === 'offline' ? 'You are offline.' : 'Could not refresh prices.'); }
  if (btn) { btn.disabled = false; btn.textContent = 'Refresh prices'; }
}

/* ------------------------------------------------------------------ */
/* Boot                                                                */
/* ------------------------------------------------------------------ */
async function init(){
  window.addEventListener('online', flush);
  setInterval(flush, 30000);
  setInterval(() => { pollQuotes(false); schedule(); }, 60000);
  document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'visible' && mode === 'cloud') { pull().then(flush).catch(() => {}); pollQuotes(false); } });
  window.addEventListener('beforeinstallprompt', e => { e.preventDefault(); installEvt = e; schedule(); });
  window.addEventListener('appinstalled', () => { installEvt = null; toast('Thesis Ledger installed'); schedule(); });
  if ('serviceWorker' in navigator && location.protocol !== 'file:') {
    navigator.serviceWorker.register('sw.js').then(reg => {
      reg.addEventListener('updatefound', () => { const w = reg.installing; if (w) w.addEventListener('statechange', () => { if (w.state === 'installed' && navigator.serviceWorker.controller) { ui.updateReady = true; schedule(); } }); });
    }).catch(() => {});
  }
  if (!cloudReady) { startLocal(); return; }
  sb = window.supabase.createClient(CFG.supabaseUrl, CFG.supabaseAnonKey, {auth:{persistSession:true, autoRefreshToken:true, detectSessionInUrl:true}});
  sb.auth.onAuthStateChange((ev, session) => {
    if (session && session.user && (ev === 'SIGNED_IN' || ev === 'INITIAL_SESSION')) startCloud(session.user);
    if (ev === 'SIGNED_OUT' && mode === 'cloud') { user = null; clearState(); mode = 'auth'; render(); }
  });
  const {data} = await sb.auth.getSession();
  if (data.session) startCloud(data.session.user);
  else if (mode === 'boot') { if (readJSON('tl-guest', false)) startLocal(); else { mode = 'auth'; render(); } }
}
init();
})();
