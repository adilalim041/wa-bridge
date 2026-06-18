import fs from 'fs';
import path from 'path';
import { createClient } from '@supabase/supabase-js';
import { getPendingDialogs as getPendingDialogsFromDb } from '../src/lib/dailyRun.js';
import { resolveTag } from '../src/ai/tagConstants.js';
import { normalizeManagerIssues } from '../src/ai/managerIssueConstants.js';

const ROOT = process.cwd();
const BRIDGE_URL = 'https://wa-bridge-production-7cd0.up.railway.app';
const SINCE_HOURS = Number(process.env.DAILY_ANALYSIS_SINCE_HOURS || 240);
const PENDING_SOURCE = process.env.DAILY_ANALYSIS_PENDING_SOURCE || 'bridge';
const SAVE = process.argv.includes('--save');
const DIRECT_SAVE = process.argv.includes('--direct');
const MARK_AS_READ = process.argv.includes('--mark-read');
const SESSIONS = [
  'astana-renat-rabochiy-reklama',
  'astana-aytzhan',
  'astana-nursultan',
  'almaty-rabochiy-reklama',
  'almaty-armada',
];
const KNOWN_MANAGER_PHONES = new Set([
  '77077832888',
  '77015368899',
  '77010688828',
  '77076206888',
  '77074507999',
]);

function loadEnv(file) {
  const out = {};
  if (!fs.existsSync(file)) return out;
  for (const raw of fs.readFileSync(file, 'utf8').split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const idx = line.indexOf('=');
    if (idx < 0) continue;
    let value = line.slice(idx + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    out[line.slice(0, idx).trim()] = value;
  }
  return out;
}

function readBridgeKey() {
  if (process.env.BRIDGE_API_KEY) return process.env.BRIDGE_API_KEY;
  if (process.env.API_KEY) return process.env.API_KEY;

  const skillPath = 'C:/Users/User/.claude/scheduled-tasks/daily-wa-analysis/SKILL.md';
  if (fs.existsSync(skillPath)) {
    const skill = fs.readFileSync(skillPath, 'utf8');
    const match = skill.match(/X-Api-Key \(header\):\s*([^\s]+)/);
    if (match) return match[1];
  }
  throw new Error('Bridge API key not found in env or daily-wa-analysis skill');
}

const env = { ...loadEnv(path.join(ROOT, '.env')), ...process.env };
const SUPABASE_URL = (env.SUPABASE_URL || '').replace(/\/$/, '');
const SUPABASE_KEY = env.SUPABASE_SERVICE_ROLE_KEY;
const BRIDGE_KEY = readBridgeKey();
if (!SUPABASE_URL || !SUPABASE_KEY) throw new Error('Supabase env missing');

const sbHeaders = { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` };
const bridgeHeaders = { 'x-api-key': BRIDGE_KEY, 'content-type': 'application/json' };
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function getJson(url, opts = {}, attempt = 1) {
  try {
    const res = await fetch(url, opts);
    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch { json = { raw: text }; }
    if (!res.ok) throw new Error(`${res.status} ${url}: ${JSON.stringify(json).slice(0, 300)}`);
    return json;
  } catch (err) {
    if (attempt >= 4) throw err;
    const waitMs = 1000 * attempt + Math.floor(Math.random() * 500);
    console.warn(`request failed, retry ${attempt}/3 in ${waitMs}ms: ${err.message}`);
    await sleep(waitMs);
    return getJson(url, opts, attempt + 1);
  }
}

async function bridge(pathname, opts = {}) {
  return getJson(`${BRIDGE_URL}${pathname}`, {
    ...opts,
    headers: { ...bridgeHeaders, ...(opts.headers || {}) },
  });
}

async function supa(pathname) {
  return getJson(`${SUPABASE_URL}/rest/v1/${pathname}`, { headers: sbHeaders });
}

function chunks(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function inList(values) {
  return `(${values.map((v) => `"${String(v).replaceAll('"', '\\"')}"`).join(',')})`;
}

function textOf(messages, fromMe = null) {
  return messages
    .filter((m) => fromMe === null || Boolean(m.from_me) === fromMe)
    .map((m) => String(m.body || '').trim())
    .filter(Boolean)
    .join('\n')
    .toLowerCase();
}

function cleanSnippet(s, n = 140) {
  return String(s || '').replace(/\s+/g, ' ').trim().slice(0, n);
}

function phoneFromJid(jid) {
  return String(jid || '').split('@')[0].replace(/\D/g, '');
}

function isGroup(jid) {
  return String(jid || '').includes('120363') || String(jid || '').includes('@g.us');
}

function minutesBetween(a, b) {
  return Math.max(0, Math.round((new Date(b).getTime() - new Date(a).getTime()) / 60000));
}

function hoursBetween(a, b) {
  return Math.max(0, (new Date(b).getTime() - new Date(a).getTime()) / 3600000);
}

function wordCount(s) {
  return String(s || '').trim().split(/\s+/).filter(Boolean).length;
}

function isClosingAck(body) {
  const s = String(body || '').trim().toLowerCase();
  if (!s || /[?？]/.test(s) || wordCount(s) > 6) return false;
  return /^(спасибо|рахмет|ок|okay|хорошо|понял|поняла|да|нет|благодарю|спс|👍|🙏|👌)[.!…🏻🏼🏽🏾🏿\s]*$/i.test(s)
    || /(спасибо|рахмет|все хорошо|всё хорошо|благодарю|👍|🙏|👌)/i.test(s);
}

function isOpenClientRequest(message) {
  const body = String(message?.body || '').trim().toLowerCase();
  const type = String(message?.message_type || '').toLowerCase();
  if (isClosingAck(body)) return false;
  if (['audio', 'image', 'video', 'document', 'contact'].includes(type) && (!body || /^\[(audio|image|video|document|contact)/.test(body))) return true;
  return /[?？]|подскаж|можно|сколько|цена|стоим|когда|где|адрес|фото|видео|каталог|прайс|кп|счет|счёт|оплат|достав|отправ|налич|размер|цвет|модель|какой|какая|какие|нужн|интерес/.test(body);
}

function hasConcreteNextStep(outText) {
  return /кп|коммерческ|прайс|каталог|счет|счёт|оплат|достав|отправ|привез|поступ|в наличии|забер|заед|подъед|подьед|шоурум|адрес|2gis|каспи|перевести|чек|накладн|сегодня|сег\b|точно будет|завтра|в среду|в четверг|в пятницу|в течени|жд[её]м|ожидаем|извин|задерж|границ|очеред|(?:\+7|8)\s*[\d\s().-]{9,}/.test(outText);
}

function isLateStageOrService(allText) {
  return /оплат|счет|счёт|чек|накладн|достав|отправ|получател|адрес доставки|курьер|yandex\.ru\/route|забер|самовывоз|поступ|пришел|пришёл|остат|осталось|в понедельник|все хорошо|всё хорошо|гарант|сервис|замен|кранбукс|картридж|протека|дефект|ремонт|бонус|бухгалтер|оферт|платформ|supplier|pintrillion|эквайринг|комисси/.test(allText);
}

function isShortReplyToManagerQuestion(messages) {
  const last = messages[messages.length - 1];
  if (!last || last.from_me) return false;

  const body = String(last.body || '').trim().toLowerCase();
  if (!/^(да|нет|можно|ок|okay|хорошо|спасибо|рахмет)[.!?\s]*$/i.test(body)) return false;

  const previous = [...messages.slice(0, -1)].reverse().find((m) => m.from_me);
  return Boolean(previous && /[?؟]|можно|подскаж|бонус|каспи|оплат/.test(String(previous.body || '').toLowerCase()));
}

function isSupplierOrAdminFlow(allText) {
  return /pintrillion|supplier|платформ|оферт|налогов|документн|эквайринг|комисси|ответ на платформе|дизайнер.*ждет.*ответ|дизайнер.*ждёт.*ответ/.test(allText);
}

function asksForVisual(body) {
  const text = String(body || '').toLowerCase();
  if (/(\bя\b|сейчас|щас|сами|сам|сама|наш[ауе]?|мо[йяёе])[^.!?\n]{0,40}(покажу|скину|отправлю|пришлю|сниму)/.test(text)) return false;
  if (/вы\s+просили[^.!?\n]{0,40}(фото|видео)|сейчас[^.!?\n]{0,40}(пришлю|скину|отправлю)[^.!?\n]{0,40}(фото|видео)|пришлю[^.!?\n]{0,40}(фото|видео)/.test(text)) return false;
  return /фото|видео|покажите|покажешь|покажете|как выглядит|можно.*увидеть|снимите|скиньте.*вид/.test(text);
}

function managerAnsweredWithVisual(message) {
  const body = String(message?.body || '').toLowerCase();
  const type = String(message?.message_type || '').toLowerCase();
  return ['image', 'video', 'document'].includes(type)
    || /\[image\]|\[video\]|\[document|фото|видео|каталог|прайс|кп|коммерческ/.test(body);
}

function hasTypedMessage(messages, { fromMe = null, types = [] } = {}) {
  const wanted = new Set(types.map((type) => String(type).toLowerCase()));
  return messages.some((message) => {
    if (fromMe !== null && Boolean(message.from_me) !== fromMe) return false;
    return wanted.has(String(message.message_type || '').toLowerCase());
  });
}

function hasOutgoingMediaOrProposal(messages, outText) {
  return hasTypedMessage(messages, { fromMe: true, types: ['image', 'video', 'document', 'contact'] })
    || /\[image\]|\[video\]|\[document|\[contact|коммерческое предложение|кп|прайс/i.test(outText);
}

function hasContactMediaHandoff(messages) {
  return hasTypedMessage(messages, { fromMe: false, types: ['contact'] })
    && hasTypedMessage(messages, { fromMe: true, types: ['image', 'video', 'document', 'contact'] });
}

function hasPendingVisualRequest(messages) {
  for (const msg of messages) {
    if (msg.from_me || !asksForVisual(msg.body)) continue;
    const answered = messages.some((candidate) =>
      candidate.from_me
      && new Date(candidate.timestamp) > new Date(msg.timestamp)
      && managerAnsweredWithVisual(candidate)
    );
    if (!answered && businessMinutesBetween(msg.timestamp, new Date()) > 60) return true;
  }
  return false;
}

function shouldFlagNoShowroomInvite({ messages, product, customerType, source, allText, outText }) {
  const inText = textOf(messages, false);
  if (!['sink', 'faucet', 'grinder'].includes(product.detail)) return false;
  if (!messages.some((m) => !m.from_me) || !messages.some((m) => m.from_me)) return false;
  if (isLateStageOrService(allText)) return false;
  if (source.lead_source === 'existing_customer') return false;
  if (['colleague', 'spam', 'unknown'].includes(customerType)) return false;

  const nonKitchenRequest = /туалет|тумб|ванн|санузел|душ|унитаз/.test(allText);
  const productIdentification = /что это за мойк|что за модель|омск написан|оригинал|подделк|идентифиц/.test(inText);
  const alreadyInvited = /шоурум|приезж|адрес|армад|салон|подъехать|подьехать|2gis|акмешит|жибек жолы|выставоч|визит|посетили|приходили/.test(outText);
  const customerWillVisit = /зайду|заеду|приеду|приду|посещу|буду в городе/.test(allText);
  const transferred = /передам.*(контакт|менеджер|коллег)|с вами.{0,60}свяж|нашему менеджеру|наш менеджер|менеджер из|по вашему региону/.test(outText);
  const remoteCity = /караганда|шымкент|павлодар|костанай|актау|атырау|уральск|семей|тараз|туркестан|кызылорда|кызыл-орда|усть[-\s]?каменогорск|экибастуз|петропавл|кокшетау|актобе|талдыкорган/.test(allText);
  const partnerFlow = /дизайнер(?!ск)|дизайн[-\s]?студ|студия дизайна|архитектор|interior|партнер|партнёр|дилер|клиент.*(просит|хочет|выбрал|заказал)|для клиента/.test(inText);
  const contactMediaHandoff = hasContactMediaHandoff(messages);
  const realSalesAnswer = /стоим|цена|от\s*\d|₸|тг|тенге|кп|коммерческ|ком\.?пр|прайс|каталог|модель|вариант|мощност|диаметр|евростандарт|подбер|подобрать|акцион|налич|установ|монтаж|\[document|\[image\]|\[video\]/.test(outText);

  return realSalesAnswer && !nonKitchenRequest && !productIdentification && !alreadyInvited && !customerWillVisit && !transferred && !remoteCity && !partnerFlow && !contactMediaHandoff;
}

function shouldNeedFollowup(messages, allText) {
  const last = messages[messages.length - 1];
  if (!last?.from_me || hoursBetween(last.timestamp, new Date()) < 24) return false;
  if (isLateStageOrService(allText)) return false;
  const customerText = textOf(messages, false);
  return /подума|посмотр|ознаком|решим|посовет|позже|напишу|дам знать|сравн|дорог|скидк|жду/.test(customerText);
}

function isManagerHandoff(outText) {
  return /передам.{0,80}(контакт|менеджер|коллег)|с вами.{0,80}свяж|свяжется.{0,80}менеджер|менеджер.{0,80}свяж|по вашему региону/i.test(outText);
}

function isManagerOnlyQualifyingQuestion(text) {
  const s = String(text || '').trim().toLowerCase();
  if (!s || !/[?？]/.test(s)) return false;
  if (/(цена|стоим|от\s*\d|₸|тг|тенге|кп|коммерческ|прайс|каталог|\[document|\[image\]|\[video\])/i.test(s)) return false;
  return /как.*обращаться|как.*зовут|вы с астаны|вы с алматы|с какого города|какой город|подскажите.*город/i.test(s);
}

function hasSalesMaterial(text) {
  return /цена|стоим|от\s*\d|₸|тг|тенге|кп|коммерческ|прайс|каталог|модель|вариант|мощност|налич|шоурум|адрес|2gis|\[document|\[image\]|\[video\]/i.test(text);
}

function hasManagerFollowupAttempt(text) {
  return /что выбрал|вопросы остал|как решение|подскажите.*решил|удалось.*посет|посетить.*шоурум|напомин|возвращаюсь|пишите\/звоните|пишите или звоните|в любое удобное/i.test(text);
}

function hasCustomerPassiveSignal(messages) {
  return messages.some((m) => {
    if (m.from_me) return false;
    const body = String(m.body || '').toLowerCase();
    if (/хочу сантехнику.*скидк|интересует.*скидк|по скидк/i.test(body)) return false;
    return /подума|посмотр|ознаком|решим|посовет|позже|напишу|дам знать|сравн|дорог|жду|вернусь|свяжусь/i.test(body);
  });
}

function shouldNeedFollowupStrict(messages, allText) {
  const last = messages[messages.length - 1];
  if (!last?.from_me || hoursBetween(last.timestamp, new Date()) < 24) return false;
  if (isLateStageOrService(allText)) return false;

  const outgoing = messages.filter((m) => m.from_me);
  const lastManagerText = String(last.body || '').toLowerCase();
  const outText = textOf(messages, true);
  if (isManagerHandoff(outText)) return false;
  if (hasManagerFollowupAttempt(outText)) return false;
  if (isManagerOnlyQualifyingQuestion(lastManagerText)) return false;

  const gaveSalesMaterial = outgoing.some((m) => hasSalesMaterial(String(m.body || '').toLowerCase()));
  return gaveSalesMaterial || hasCustomerPassiveSignal(messages);
}

shouldNeedFollowup = shouldNeedFollowupStrict;

const ALMATY_UTC_OFFSET_MIN = 5 * 60;
const WORK_START_HOUR = 10;
const WORK_END_HOUR = 20;

function localParts(date) {
  const shifted = new Date(date.getTime() + ALMATY_UTC_OFFSET_MIN * 60000);
  return {
    y: shifted.getUTCFullYear(),
    m: shifted.getUTCMonth(),
    d: shifted.getUTCDate(),
    h: shifted.getUTCHours(),
    min: shifted.getUTCMinutes(),
    sec: shifted.getUTCSeconds(),
  };
}

function utcFromLocal(y, m, d, h, min = 0, sec = 0) {
  return new Date(Date.UTC(y, m, d, h, min, sec) - ALMATY_UTC_OFFSET_MIN * 60000);
}

function localYmd(value) {
  const date = value ? new Date(value) : new Date();
  return new Date(date.getTime() + ALMATY_UTC_OFFSET_MIN * 60000).toISOString().slice(0, 10);
}

function businessMinutesBetween(a, b) {
  let cursor = new Date(a);
  const end = new Date(b);
  if (!Number.isFinite(cursor.getTime()) || !Number.isFinite(end.getTime()) || end <= cursor) return 0;

  let total = 0;
  while (cursor < end && total < 60 * 24 * 30) {
    const p = localParts(cursor);
    const dayStart = utcFromLocal(p.y, p.m, p.d, WORK_START_HOUR);
    const dayEnd = utcFromLocal(p.y, p.m, p.d, WORK_END_HOUR);

    if (cursor < dayStart) cursor = dayStart;
    if (cursor >= dayEnd) {
      cursor = utcFromLocal(p.y, p.m, p.d + 1, WORK_START_HOUR);
      continue;
    }

    const segmentEnd = end < dayEnd ? end : dayEnd;
    total += Math.max(0, Math.round((segmentEnd - cursor) / 60000));
    cursor = segmentEnd >= dayEnd ? utcFromLocal(p.y, p.m, p.d + 1, WORK_START_HOUR) : segmentEnd;
  }
  return total;
}

function detectProduct(allText) {
  const text = String(allText || '').replace(/омо[ий]кири|omoikiri/gi, '');
  if (/измельч|диспоуз|disposer|диспоз/.test(text)) return { topic: 'grinder_sale', detail: 'grinder' };
  if (/смесител|кран|faucet/.test(text)) return { topic: 'faucet_sale', detail: 'faucet' };
  if (/мойк|раковин|sink|подстоль|столешниц|чаш/.test(text)) return { topic: 'sink_sale', detail: 'sink' };
  if (/сушк|дозатор|аксессуар/.test(text)) return { topic: 'accessory_sale', detail: 'accessory' };
  return { topic: 'other', detail: 'general' };
}

function detectLeadSource(inText, allText, product) {
  const adStrong = /реклам|инст|instagram|insta|скидк|акци|увидел|увидела|объявлен|хочу приобрести|omoikiri|омойкири|омоик|официальн/.test(inText);
  const adTemplate = /здравствуйте.*(хочу|интересует|сколько|цена|стоимость)/s.test(inText);
  const niche = ['grinder', 'sink', 'faucet'].includes(product.detail);
  if (adStrong || (adTemplate && niche)) {
    const suffix = product.detail === 'general' ? 'general' : product.detail;
    const via = /инст|instagram|insta/.test(inText) ? '_via_instagram' : '';
    const discount = /скидк|акци/.test(inText) ? '_discount' : '';
    return { lead_source: 'omoikiri_ad', lead_source_detail: `omoikiri_ad_${suffix}${discount}${via}` };
  }
  if (/покупал|покупала|брали у вас|уже покуп/.test(allText)) {
    return { lead_source: 'existing_customer', lead_source_detail: 'returning_customer' };
  }
  if (/дизайн|студия|архитектор|клиент.*хочет|проект/.test(inText)) {
    return { lead_source: 'referral', lead_source_detail: 'designer_or_partner' };
  }
  return { lead_source: 'organic', lead_source_detail: null };
}

function analyzeDialog(dialog, messages, chatRow) {
  const incoming = messages.filter((m) => !m.from_me);
  const outgoing = messages.filter((m) => m.from_me);
  const inText = textOf(messages, false);
  const outText = textOf(messages, true);
  const allText = textOf(messages, null);
  const clientProduct = detectProduct(inText);
  const product = clientProduct.detail === 'general' ? detectProduct(allText) : clientProduct;
  const source = detectLeadSource(inText, allText, product);
  const managerIssues = new Set();
  const riskFlags = new Set();

  let customerType = 'end_client';
  const inboundPushNames = incoming.map((m) => String(m.push_name || '').toLowerCase()).join(' ');
  const remotePhone = phoneFromJid(dialog.remote_jid);
  const looksInternalText = /коллег|накладн|счет\s*№|счёт\s*№|команда|склад.*отправ|отправ.*склад|контакт передаю клиенту|заявленн.*дефект|гаранти|протекает|созвонюсь|заменить кран|картридж|букс/.test(allText);
  const looksInternalName = /omoikiri|омойкири|омоик|менеджер|склад|админ/.test(inboundPushNames);
  if (isGroup(dialog.remote_jid) || KNOWN_MANAGER_PHONES.has(remotePhone) || looksInternalName || looksInternalText) {
    customerType = 'colleague';
  } else if (/дизайнер(?!ск)|дизайн[-\s]?студ|студия дизайна|архитектор|interior|партнер|партнёр/.test(inText)) {
    customerType = 'partner';
  }
  if (/спам|казино|ставк|crypto|крипт|заработ/.test(allText)) customerType = 'spam';

  let firstResponseMin = null;
  if (incoming.length > 0 && outgoing.length > 0) {
    const firstIn = incoming[0];
    const firstOutAfter = outgoing.find((m) => new Date(m.timestamp) > new Date(firstIn.timestamp));
    if (firstOutAfter) {
      firstResponseMin = businessMinutesBetween(firstIn.timestamp, firstOutAfter.timestamp);
      if (firstResponseMin > 30) managerIssues.add('slow_response');
    }
  }

  for (const msg of incoming) {
    const nextOut = outgoing.find((m) => new Date(m.timestamp) > new Date(msg.timestamp));
    if (nextOut && businessMinutesBetween(msg.timestamp, nextOut.timestamp) > 90) managerIssues.add('slow_response');
  }

  const last = messages[messages.length - 1];
  const clientWaiting = last
    && !last.from_me
    && customerType !== 'colleague'
    && customerType !== 'spam'
    && isOpenClientRequest(last)
    && !isShortReplyToManagerQuestion(messages)
    && !isSupplierOrAdminFlow(allText)
    && businessMinutesBetween(last.timestamp, new Date()) > 60;
  if (clientWaiting) {
    managerIssues.add('no_response');
  }

  if (/дорого|цена не устраивает|не устраивает|конкурент|blanco|бланко|купил|купила|передумал|не надо|отмена/.test(inText)) {
    riskFlags.add('lost_lead');
  }
  if (/жалоб|брак|сломал|не работает|претенз|вернуть|возврат|гаранти|почему.*нет ответа|не отвеч/.test(inText)) {
    riskFlags.add('client_unhappy');
  }
  if (shouldNeedFollowup(messages, allText) && !/что выбрал|вопросы остал|как решение|подскажите.*решил|напомнить|возвращаюсь/.test(outText)) {
    managerIssues.add('no_followup');
  }

  const managerWords = outText.split(/\s+/).filter(Boolean).length;
  const hasQuestions = /\?/.test(outText) || /размер|бюджет|цвет|монтаж|город|когда|какую|какой|нужн|подскаж/.test(outText);
  const hasMediaOrProposal = hasOutgoingMediaOrProposal(messages, outText);
  if (incoming.length > 0 && outgoing.length > 0 && managerWords < 25 && !hasQuestions && !hasMediaOrProposal && !hasConcreteNextStep(outText) && !isSupplierOrAdminFlow(allText)) {
    managerIssues.add('short_template_only');
  }
  if (shouldFlagNoShowroomInvite({ messages, product, customerType, source, allText, outText })) {
    managerIssues.add('no_showroom_invite');
  }
  if (hasPendingVisualRequest(messages)) {
    managerIssues.add('no_photos');
  }

  let intent = 'consultation';
  if (/цена|стоимость|сколько|прайс|кп|коммерческое/.test(allText)) intent = 'price_inquiry';
  if (riskFlags.has('client_unhappy')) intent = 'complaint';
  if (customerType === 'partner') intent = 'collaboration';
  if (customerType === 'colleague') intent = 'other';

  let dealStage = 'consultation';
  if (outgoing.length === 0) dealStage = 'first_contact';
  if (/кп|коммерческое предложение|\[document/.test(outText)) dealStage = 'proposal_sent';
  if (/оплат|счет|счёт|перевод|наличк|достав/.test(allText)) dealStage = 'payment';
  if (riskFlags.has('lost_lead')) dealStage = 'refused';
  if (customerType === 'colleague' || customerType === 'spam') dealStage = 'needs_review';

  let leadTemperature = 'warm';
  if (customerType === 'colleague' || customerType === 'spam') leadTemperature = 'dead';
  else if (riskFlags.has('lost_lead')) leadTemperature = 'cold';
  else if (/купить|заказ|оплат|счет|счёт|достав|налич|сроч|сегодня|подъехать|подьехать/.test(inText)) leadTemperature = 'hot';
  else if (source.lead_source === 'omoikiri_ad' || intent === 'price_inquiry') leadTemperature = 'warm';
  else if (outgoing.length > 0 && incoming.length === 0) leadTemperature = 'cold';

  let sentiment = 'neutral';
  if (riskFlags.has('client_unhappy')) sentiment = 'negative';
  if (/спасибо|благодар|ок|понял|хорошо/.test(inText)) sentiment = 'positive';

  const customerName = incoming.find((m) => m.push_name)?.push_name || chatRow?.display_name || phoneFromJid(dialog.remote_jid);
  const firstCustomerText = cleanSnippet(incoming[0]?.body || '');
  const lastCustomerText = cleanSnippet([...incoming].reverse()[0]?.body || '');
  const responsePart = firstResponseMin === null ? 'первый ответ не найден' : `первый ответ примерно через ${firstResponseMin} мин`;
  const sourcePart = source.lead_source === 'omoikiri_ad' ? 'Лид с рекламы Omoikiri' : source.lead_source === 'existing_customer' ? 'Повторный клиент' : customerType === 'partner' ? 'Партнер/дизайнер' : 'Клиентский диалог';
  const summary = customerType === 'colleague'
    ? `Внутренний/служебный диалог, не клиентская продажа. ${cleanSnippet(last?.body || firstCustomerText, 120)}`
    : `${sourcePart}: ${customerName || 'клиент'} интересуется ${product.detail === 'general' ? 'товаром/консультацией' : product.detail}. ${responsePart}. Последний запрос клиента: "${lastCustomerText || firstCustomerText}".`;

  if (customerType === 'colleague' || customerType === 'spam') {
    managerIssues.clear();
    riskFlags.clear();
    intent = 'other';
    dealStage = 'needs_review';
    leadTemperature = 'dead';
    sentiment = 'neutral';
  }

  let actionRequired = customerType !== 'colleague' && customerType !== 'spam' && (clientWaiting || managerIssues.size > 0 || leadTemperature === 'hot');
  let action = null;
  if (customerType === 'colleague' || customerType === 'spam') {
    actionRequired = false;
  } else if (clientWaiting) {
    action = 'Ответить клиенту, закрыть открытый вопрос и предложить следующий шаг: КП, шоурум, оплату или доставку.';
  } else if (managerIssues.has('no_followup')) {
    action = 'Сделать повторный контакт: уточнить решение, снять возражения и предложить конкретный вариант.';
  } else if (leadTemperature === 'hot') {
    action = 'Дожать горячий лид: подтвердить модель/цену, наличие, адрес и способ оплаты.';
  } else {
    action = 'Контролировать диалог и не оставлять клиента без следующего шага.';
  }

  const history = dialog.customer_history || {};
  if (history.is_existing) {
    source.lead_source = 'existing_customer';
    source.lead_source_detail = 'known_customer';
  }

  return {
    dialog_session_id: dialog.id,
    session_id: dialog.session_id,
    remote_jid: dialog.remote_jid,
    intent,
    lead_temperature: leadTemperature,
    lead_source: source.lead_source,
    lead_source_detail: source.lead_source_detail,
    customer_type: customerType,
    dialog_topic: product.topic,
    deal_stage: dealStage,
    sentiment,
    risk_flags: [...riskFlags],
    summary_ru: summary,
    action_required: actionRequired,
    action_suggestion: action,
    confidence: customerType === 'colleague' ? 0.7 : 0.74,
    analyzed_at: new Date().toISOString(),
    analysis_date: localYmd(dialog.last_message_at || new Date().toISOString()),
    message_count_analyzed: messages.length,
    consultation_score: customerType === 'end_client' || customerType === 'partner'
      ? Math.max(1, Math.min(10, 7 - (managerIssues.size > 0 ? 2 : 0) - (riskFlags.size > 0 ? 2 : 0) + (hasQuestions ? 1 : 0)))
      : null,
    consultation_details: {
      first_response_minutes: firstResponseMin,
      product: product.detail,
      ad_lead: source.lead_source === 'omoikiri_ad',
      heuristic: true,
    },
    followup_status: managerIssues.has('no_followup') || clientWaiting ? 'needed' : 'not_needed',
    manager_issues: normalizeManagerIssues([...managerIssues]),
    is_existing_customer: Boolean(history.is_existing),
    previous_orders_count: history.orders_count || 0,
    previous_orders_amount: history.total_amount || 0,
    last_purchase_date: history.last_purchase_date || null,
  };
}

async function getPendingDialogs() {
  const all = [];
  for (const sessionId of SESSIONS) {
    const r = PENDING_SOURCE === 'direct'
      ? await getPendingDialogsFromDb({ sinceHours: SINCE_HOURS, limit: 500, sessionId })
      : await bridge(`/admin/daily-run/pending-dialogs?since_hours=${SINCE_HOURS}&limit=500&sessionId=${encodeURIComponent(sessionId)}`);
    all.push(...(r.dialogs || []));
    console.log(`${sessionId}: ${r.count} pending (${r.new_count} new, ${r.re_analyze_count} re-analyze)`);
  }
  const seen = new Set();
  return all.filter((d) => {
    if (seen.has(d.id)) return false;
    seen.add(d.id);
    return true;
  });
}

async function getMessagesByDialog(dialogIds) {
  const map = new Map();
  for (const part of chunks(dialogIds, 40)) {
    const rows = await supa(
      `messages?select=id,dialog_session_id,session_id,remote_jid,from_me,timestamp,push_name,body,message_type,media_url&dialog_session_id=in.${encodeURIComponent(inList(part))}&order=timestamp.asc&limit=10000`
    );
    for (const row of rows) {
      if (!map.has(row.dialog_session_id)) map.set(row.dialog_session_id, []);
      map.get(row.dialog_session_id).push(row);
    }
  }
  for (const list of map.values()) {
    list.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  }
  return map;
}

async function getChats(dialogs) {
  const map = new Map();
  for (const sessionId of SESSIONS) {
    const jids = [...new Set(dialogs.filter((d) => d.session_id === sessionId).map((d) => d.remote_jid))];
    for (const part of chunks(jids, 80)) {
      const rows = await supa(
        `chats?select=session_id,remote_jid,display_name,tags,tag_confirmed,phone_number&session_id=eq.${encodeURIComponent(sessionId)}&remote_jid=in.${encodeURIComponent(inList(part))}`
      );
      for (const row of rows) map.set(`${row.session_id}:::${row.remote_jid}`, row);
    }
  }
  return map;
}

function groupCount(rows, fn) {
  const out = {};
  for (const row of rows) {
    const key = fn(row) || 'unknown';
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function reportAdLeads(records, dialogsById, messagesByDialog) {
  const ad = records.filter((r) => r.lead_source === 'omoikiri_ad');
  const lines = [
    '# Advertising Leads Analysis',
    '',
    `Generated: ${new Date().toISOString()}`,
    `Window: last ${SINCE_HOURS} hours`,
    '',
    `Total ad leads: ${ad.length}`,
    '',
    '## By Account',
    '',
  ];
  for (const [sid, count] of Object.entries(groupCount(ad, (r) => r.session_id)).sort((a, b) => b[1] - a[1])) {
    lines.push(`- ${sid}: ${count}`);
  }
  lines.push('', '## By Product', '');
  for (const [product, count] of Object.entries(groupCount(ad, (r) => r.consultation_details?.product)).sort((a, b) => b[1] - a[1])) {
    lines.push(`- ${product}: ${count}`);
  }
  lines.push('', '## Red Flags', '');
  const bad = ad.filter((r) => (r.manager_issues || []).length || (r.risk_flags || []).length || r.action_required);
  for (const r of bad.slice(0, 60)) {
    const dialog = dialogsById.get(r.dialog_session_id);
    const msgs = messagesByDialog.get(r.dialog_session_id) || [];
    const firstIn = msgs.find((m) => !m.from_me);
    const lastMsg = msgs[msgs.length - 1];
    lines.push(`### ${r.session_id} / +${phoneFromJid(r.remote_jid)} / ${r.analysis_date}`);
    lines.push(`- Status: ${r.lead_temperature}, ${r.deal_stage}`);
    lines.push(`- Issues: ${(r.manager_issues || []).join(', ') || 'none'}`);
    lines.push(`- Risks: ${(r.risk_flags || []).join(', ') || 'none'}`);
    lines.push(`- First client message: ${cleanSnippet(firstIn?.body || '', 220)}`);
    lines.push(`- Last message: ${lastMsg?.from_me ? 'manager' : 'client'} — ${cleanSnippet(lastMsg?.body || '', 220)}`);
    lines.push(`- Action: ${r.action_suggestion || 'none'}`);
    if (dialog?.customer_history?.is_existing) {
      lines.push(`- Existing customer: ${dialog.customer_history.orders_count} orders, ${dialog.customer_history.total_amount} KZT`);
    }
    lines.push('');
  }
  return lines.join('\n');
}

async function saveRecords(records) {
  const totals = { saved: 0, updated: 0, tagged: 0, marked_read: 0, batches: 0 };
  for (const part of chunks(records, 20)) {
    const r = await bridge('/admin/daily-run/save-analysis', {
      method: 'POST',
      body: JSON.stringify({ records: part, mark_as_read: MARK_AS_READ }),
    });
    totals.saved += r.saved || 0;
    totals.updated += r.updated || 0;
    totals.tagged += r.tagged || 0;
    totals.marked_read += r.marked_read || 0;
    totals.batches += 1;
    console.log(`saved batch ${totals.batches}: +${r.saved || 0}, updated ${r.updated || 0}, tagged ${r.tagged || 0}`);
    await sleep(500);
  }
  return totals;
}

async function saveRecordsDirect(records) {
  const totals = { saved: 0, updated: 0, tagged: 0, chat_ai_batches: 0, tag_batches: 0 };
  const uniqueRecords = [
    ...new Map(records.map((r) => [`${r.dialog_session_id}::${r.analysis_date}`, r])).values(),
  ];

  const existingByDialog = new Map();
  for (const part of chunks(uniqueRecords.map((r) => r.dialog_session_id), 100)) {
    const { data, error } = await supabase
      .from('chat_ai')
      .select('id, dialog_session_id, analysis_date')
      .in('dialog_session_id', part);
    if (error) throw new Error(`direct chat_ai existing read failed: ${error.message}`);
    for (const row of data || []) {
      existingByDialog.set(`${row.dialog_session_id}::${row.analysis_date}`, row.id);
    }
  }

  const toInsert = uniqueRecords.filter((r) => !existingByDialog.has(`${r.dialog_session_id}::${r.analysis_date}`));
  const toUpdate = uniqueRecords.filter((r) => existingByDialog.has(`${r.dialog_session_id}::${r.analysis_date}`));

  for (const part of chunks(toInsert, 100)) {
    const { data, error } = await supabase
      .from('chat_ai')
      .insert(part)
      .select('id');
    if (error) throw new Error(`direct chat_ai insert failed: ${error.message}`);
    totals.saved += data?.length || part.length;
    totals.chat_ai_batches += 1;
    console.log(`direct insert chat_ai batch ${totals.chat_ai_batches}: ${data?.length || part.length}`);
    await sleep(250);
  }

  for (const record of toUpdate) {
    const { error } = await supabase
      .from('chat_ai')
      .update(record)
      .eq('dialog_session_id', record.dialog_session_id)
      .eq('analysis_date', record.analysis_date);
    if (error) throw new Error(`direct chat_ai update failed: ${error.message}`);
    totals.updated += 1;
  }

  const uniqueJids = [...new Set(uniqueRecords.map((r) => r.remote_jid).filter(Boolean))];
  const existingTags = new Map();
  for (const part of chunks(uniqueJids, 100)) {
    const { data, error } = await supabase
      .from('chat_tags')
      .select('remote_jid, tag_confirmed')
      .in('remote_jid', part);
    if (error) throw new Error(`direct tag read failed: ${error.message}`);
    for (const row of data || []) existingTags.set(row.remote_jid, row);
  }

  const tagRows = [];
  const chatRows = [];
  const seenTagJids = new Set();
  const seenChatPairs = new Set();
  for (const record of uniqueRecords) {
    const tag = resolveTag(record.customer_type);
    if (!tag) continue;
    const existing = existingTags.get(record.remote_jid);
    if (!existing?.tag_confirmed && !seenTagJids.has(record.remote_jid)) {
      tagRows.push({
        remote_jid: record.remote_jid,
        tags: [tag],
        tag_confirmed: false,
        updated_at: new Date().toISOString(),
      });
      seenTagJids.add(record.remote_jid);
    }
    const pair = `${record.session_id}:::${record.remote_jid}`;
    if (!seenChatPairs.has(pair)) {
      chatRows.push({
        session_id: record.session_id,
        remote_jid: record.remote_jid,
        ai_tag: tag,
        updated_at: new Date().toISOString(),
      });
      seenChatPairs.add(pair);
    }
  }

  for (const part of chunks(tagRows, 100)) {
    const { error } = await supabase
      .from('chat_tags')
      .upsert(part, { onConflict: 'remote_jid' });
    if (error) throw new Error(`direct tag upsert failed: ${error.message}`);
    totals.tagged += part.length;
    totals.tag_batches += 1;
    console.log(`direct upsert tags batch ${totals.tag_batches}: ${part.length}`);
    await sleep(250);
  }

  for (const part of chunks(chatRows, 100)) {
    const { error } = await supabase
      .from('chats')
      .upsert(part, { onConflict: 'session_id,remote_jid' });
    if (error) throw new Error(`direct chats ai_tag upsert failed: ${error.message}`);
    await sleep(250);
  }

  return totals;
}

async function main() {
  console.log(`Daily analysis runner: since_hours=${SINCE_HOURS}, pending_source=${PENDING_SOURCE}, save=${SAVE}, direct=${DIRECT_SAVE}, mark_read=${MARK_AS_READ}`);
  if (!DIRECT_SAVE && PENDING_SOURCE !== 'direct') {
    await bridge('/admin/daily-run/auto-dismiss', { method: 'POST', body: '{}' }).catch((e) => {
      console.warn(`auto-dismiss skipped: ${e.message}`);
    });
  }

  const dialogs = await getPendingDialogs();
  const dialogsById = new Map(dialogs.map((d) => [d.id, d]));
  console.log(`Total unique pending dialogs: ${dialogs.length}`);

  const messagesByDialog = await getMessagesByDialog(dialogs.map((d) => d.id));
  const chatsByKey = await getChats(dialogs);
  const records = [];
  const skipped = [];
  for (const d of dialogs) {
    const messages = messagesByDialog.get(d.id) || [];
    if (messages.length === 0) {
      skipped.push(d.id);
      continue;
    }
    records.push(analyzeDialog(d, messages, chatsByKey.get(`${d.session_id}:::${d.remote_jid}`)));
  }

  const summary = {
    generated_at: new Date().toISOString(),
    since_hours: SINCE_HOURS,
    pending_dialogs: dialogs.length,
    records: records.length,
    skipped_without_messages: skipped.length,
    by_session: groupCount(records, (r) => r.session_id),
    by_customer_type: groupCount(records, (r) => r.customer_type),
    by_lead_source: groupCount(records, (r) => r.lead_source),
    by_temperature: groupCount(records, (r) => r.lead_temperature),
    manager_issues: groupCount(records.flatMap((r) => (r.manager_issues || []).map((issue) => ({ issue }))), (r) => r.issue),
    risk_flags: groupCount(records.flatMap((r) => (r.risk_flags || []).map((flag) => ({ flag }))), (r) => r.flag),
  };

  fs.mkdirSync(path.join(ROOT, 'reports'), { recursive: true });
  const stamp = new Date().toISOString().slice(0, 10);
  const summaryPath = path.join(ROOT, 'reports', `daily-analysis-summary-${stamp}.json`);
  const adPath = path.join(ROOT, 'reports', `ad-leads-analysis-${stamp}.md`);
  fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2), 'utf8');
  fs.writeFileSync(adPath, reportAdLeads(records, dialogsById, messagesByDialog), 'utf8');
  console.log(JSON.stringify(summary, null, 2));
  console.log(`Report written: ${adPath}`);

  if (SAVE) {
    const totals = DIRECT_SAVE ? await saveRecordsDirect(records) : await saveRecords(records);
    console.log(JSON.stringify({ save_totals: totals }, null, 2));
    if (!DIRECT_SAVE) {
      const digest = await bridge('/admin/daily-run/digest?send_telegram=false', { method: 'POST', body: '{}' });
      console.log(JSON.stringify({ digest: { date: digest.date, new_analyses: digest.new_analyses, critical: digest.critical?.count, hot_leads: digest.hot_leads?.count, manager_issues: digest.manager_issues?.count, stuck_deals: digest.stuck_deals?.count } }, null, 2));
    }
  } else {
    console.log('Dry-run only. Re-run with --save to write chat_ai/tags.');
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
