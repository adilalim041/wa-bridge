import { supabase } from '../storage/supabase.js';
import { normalizeManagerIssues } from '../ai/managerIssueConstants.js';

const APPLY = process.argv.includes('--apply');
const DAYS = Number(process.env.CALIBRATION_DAYS || 30);
const ALMATY_UTC_OFFSET_MIN = 5 * 60;
const WORK_START_HOUR = 10;
const WORK_END_HOUR = 20;

function clean(s, n = 140) {
  return String(s || '').replace(/\s+/g, ' ').trim().slice(0, n);
}

function textOf(messages, fromMe = null) {
  return messages
    .filter((m) => fromMe === null || Boolean(m.from_me) === fromMe)
    .map((m) => String(m.body || ''))
    .join('\n')
    .toLowerCase();
}

function localParts(date) {
  const shifted = new Date(date.getTime() + ALMATY_UTC_OFFSET_MIN * 60000);
  return { y: shifted.getUTCFullYear(), m: shifted.getUTCMonth(), d: shifted.getUTCDate() };
}

function utcFromLocal(y, m, d, h) {
  return new Date(Date.UTC(y, m, d, h, 0, 0) - ALMATY_UTC_OFFSET_MIN * 60000);
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

function wordCount(s) {
  return String(s || '').trim().split(/\s+/).filter(Boolean).length;
}

function isClosingAck(body) {
  const s = String(body || '').trim().toLowerCase();
  if (!s || /[?؟]/.test(s) || wordCount(s) > 8) return false;
  return /^(спасибо|рахмет|ок|okay|хорошо|понял|поняла|да|нет|благодарю|спс|👍|🙏|👌)/i.test(s)
    || /(спасибо|рахмет|все хорошо|всё хорошо|благодарю)/i.test(s);
}

function isPassiveStatusUpdate(body) {
  const s = String(body || '').trim().toLowerCase();
  if (!s || /[?]/.test(s)) return false;
  return /^(\u043f\u043e\u043a\u0430\s+\u043d\u0435\u0442\s+\u043e\u043f\u043b\u0430\u0442|\u043d\u0435\u0442\s+\u043e\u043f\u043b\u0430\u0442|\u043e\u0442\u0434\u0430\u043b\u0430?\s+\u043d\u0430\s+\u043e\u043f\u043b\u0430\u0442|\u043e\u043f\u043b\u0430\u0442\u0443\s+\u043e\u0442\u0434\u0430\u043b\u0430?|\u043f\u0435\u0440\u0435\u0434\u0430\u043b\u0430?\s+\u043d\u0430\s+\u043e\u043f\u043b\u0430\u0442|\u0441\u0430\u043c\u0430?\s+\u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044e|\u0442\u043e\u0436\u0435\s+\u0441\u0430\u043c\u0430?\s+\u043e\u0442\u043f\u0440\u0430\u0432\u043b\u044e)/i.test(s);
}

function isOpenClientRequest(message) {
  const body = String(message?.body || '').trim().toLowerCase();
  const type = String(message?.message_type || '').toLowerCase();
  if (isClosingAck(body)) return false;
  if (isPassiveStatusUpdate(body)) return false;
  if (['audio', 'image', 'video', 'document', 'contact'].includes(type) && (!body || /^\[(audio|image|video|document|contact)/.test(body))) return true;
  return /[?؟]|подскаж|можно|сколько|цена|стоим|что по|когда|где|адрес|фото|видео|каталог|прайс|кп|счет|счёт|оплат|достав|отправ|налич|размер|цвет|модель|какой|какая|какие|нужн|интерес|узнай|сейчас что делать|пришлите|скиньте/.test(body);
}

function isLateStageOrService(allText) {
  return /оплат|счет|счёт|чек|накладн|достав|отправ|получател|адрес доставки|курьер|забер|самовывоз|поступ|пришел|пришёл|остат|гарант|сервис|замен|кранбукс|картридж|протека|дефект|ремонт|бонус|бухгалтер|оферт|платформ|supplier|pintrillion|эквайринг|комисси/.test(allText);
}

function hasConcreteNextStep(outText) {
  return /кп|коммерческ|прайс|каталог|счет|счёт|оплат|достав|отправ|привез|поступ|в наличии|забер|заед|подъед|шоурум|адрес|2gis|каспи|перевести|чек|накладн|сегодня|сег\b|точно будет|завтра|в течение|цена|стоим|₸|тг|жд[её]м|ожидаем|извин|задерж|границ|очеред|(?:\+7|8)\s*[\d\s().-]{9,}/.test(outText);
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

function isPassiveFollowupSignal(messages) {
  const last = messages[messages.length - 1];
  if (!last || last.from_me) return false;
  if (isOpenClientRequest(last)) return false;
  const customerText = textOf(messages, false);
  return /подума|посмотр|ознаком|решим|посовет|позже|напишу|дам знать|сравн|дорог|скидк|жду|отправлю заказчик|вернусь|свяжусь/.test(customerText);
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
  if (!last?.from_me) return false;
  if (businessMinutesBetween(last.timestamp, new Date()) < 60 * 24) return false;
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

function hasPendingVisualRequest(messages) {
  for (const msg of messages) {
    if (msg.from_me || !asksForVisual(msg.body)) continue;
    const answered = messages.some((candidate) =>
      candidate.from_me
      && new Date(candidate.timestamp) > new Date(msg.timestamp)
      && managerAnsweredWithVisual(candidate)
    );
    if (!answered) return true;
  }
  return false;
}

function detectProduct(allText) {
  const text = String(allText || '').replace(/омо[ий]кири|omoikiri/gi, '');
  if (/измельч|диспоуз|disposer|диспоз/.test(text)) return 'grinder';
  if (/смесител|кран|faucet/.test(text)) return 'faucet';
  if (/мойк|раковин|sink|подстоль|столешниц|чаш/.test(text)) return 'sink';
  if (/дозатор|сушка|аксессуар/.test(text)) return 'accessory';
  return 'other';
}

function lastManagerAskedForCityOrName(messages) {
  const last = messages[messages.length - 1];
  if (!last?.from_me) return false;
  const body = String(last.body || '').toLowerCase();
  return /вы с астаны|вы с алматы|с какого города|как могу к вам обращаться|как я могу к вам обращаться|как вас зовут/.test(body);
}

function shouldRemoveNoShowroom(messages, customerType, leadSource) {
  const allText = textOf(messages);
  const outText = textOf(messages, true);
  const inText = textOf(messages, false);
  const clientProduct = detectProduct(inText);
  const product = clientProduct === 'other' ? detectProduct(allText) : clientProduct;
  if (!['sink', 'faucet', 'grinder'].includes(product)) return true;
  if (leadSource === 'existing_customer') return true;
  if (customerType === 'partner') return true;
  if (isLateStageOrService(allText)) return true;
  if (/что это за мойк|что за модель|омск написан|оригинал|подделк|идентифиц/.test(inText)) return true;
  if (lastManagerAskedForCityOrName(messages)) return true;
  if (/передам.*(контакт|менеджер|коллег)|с вами.{0,60}свяж|нашему менеджеру|наш менеджер|менеджер из|по вашему региону/.test(outText)) return true;
  if (/шоурум|приезж|адрес|салон|подъехать|подьехать|2gis|акмешит|жибек жолы|выставоч|визит|посетили|приходили/.test(outText)) return true;
  if (/зайду|заеду|приеду|приду|посещу|буду в городе/.test(allText)) return true;
  if (/дизайнер(?!ск)|дизайн[-\s]?студ|студия дизайна|архитектор|interior|партнер|партнёр|дилер|для клиента/.test(inText)) return true;
  if (/туалет|тумб|ванн|санузел|душ|унитаз/.test(allText)) return true;
  return false;
}

function nextAction(issues) {
  if (issues.includes('no_response')) {
    return 'Ответить клиенту и закрыть открытый вопрос: цена, наличие, КП, шоурум, оплата или доставка.';
  }
  if (issues.includes('no_followup')) {
    return 'Сделать повторный контакт: уточнить решение, снять возражения и предложить конкретный следующий шаг.';
  }
  if (issues.length > 0) {
    return 'Проверить диалог и закрыть найденную проблему менеджера.';
  }
  return null;
}

async function fetchRows() {
  const since = new Date(Date.now() - (DAYS - 1) * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const { data, error } = await supabase
    .from('chat_ai')
    .select('id, dialog_session_id, session_id, remote_jid, analysis_date, customer_type, lead_source, manager_issues, followup_status, action_required, action_suggestion, summary_ru')
    .gte('analysis_date', since)
    .in('customer_type', ['end_client', 'partner'])
    .not('manager_issues', 'eq', '{}')
    .limit(2000);
  if (error) throw error;
  return data || [];
}

async function fetchMessages(dialogIds) {
  const map = new Map();
  for (let i = 0; i < dialogIds.length; i += 80) {
    const { data, error } = await supabase
      .from('messages')
      .select('id, dialog_session_id, from_me, timestamp, push_name, body, message_type')
      .in('dialog_session_id', dialogIds.slice(i, i + 80))
      .order('timestamp', { ascending: true })
      .limit(10000);
    if (error) throw error;
    for (const row of data || []) {
      const key = String(row.dialog_session_id);
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(row);
    }
  }
  return map;
}

function calibrate(row, messages) {
  const issues = new Set(normalizeManagerIssues(row.manager_issues || []));
  const original = [...issues];
  const reasons = [];
  const last = messages[messages.length - 1];
  const allText = textOf(messages);
  const outText = textOf(messages, true);
  const clientWaiting = last
    && !last.from_me
    && isOpenClientRequest(last)
    && !isShortReplyToManagerQuestion(messages)
    && !isSupplierOrAdminFlow(allText)
    && businessMinutesBetween(last.timestamp, new Date()) > 60;

  if (clientWaiting && issues.has('no_followup')) {
    issues.delete('no_followup');
    issues.add('no_response');
    reasons.push('open client request belongs to no_response, not no_followup');
  }

  if (issues.has('no_response') && !clientWaiting) {
    issues.delete('no_response');
    reasons.push('no open unanswered client request');
  }

  if (issues.has('no_followup')) {
    if (!shouldNeedFollowupStrict(messages, allText)) {
      issues.delete('no_followup');
      reasons.push('follow-up need not proven by strict rule');
    }
  }

  if (issues.has('short_template_only') && (!last?.from_me || hasConcreteNextStep(outText) || /\[image\]|\[document|\[video\]/.test(outText) || isLateStageOrService(allText) || isSupplierOrAdminFlow(allText))) {
    issues.delete('short_template_only');
    reasons.push('manager gave concrete next step/media/logistics');
  }

  if (issues.has('no_photos') && !hasPendingVisualRequest(messages)) {
    issues.delete('no_photos');
    reasons.push('no unresolved visual request');
  }

  if (issues.has('no_showroom_invite') && shouldRemoveNoShowroom(messages, row.customer_type, row.lead_source)) {
    issues.delete('no_showroom_invite');
    reasons.push('showroom invite exclusion applies');
  }

  const nextIssues = normalizeManagerIssues([...issues]);
  const changed = JSON.stringify(original) !== JSON.stringify(nextIssues);
  if (!changed) return null;

  return {
    id: row.id,
    dialog_session_id: row.dialog_session_id,
    before: original,
    after: nextIssues,
    reasons,
    patch: {
      manager_issues: nextIssues,
      followup_status: nextIssues.some((x) => x === 'no_response' || x === 'no_followup') ? 'needed' : 'not_needed',
      action_required: nextIssues.length > 0,
      action_suggestion: nextAction(nextIssues),
    },
    sample: {
      session_id: row.session_id,
      date: row.analysis_date,
      name: messages.find((m) => !m.from_me)?.push_name || null,
      first: clean(messages.find((m) => !m.from_me)?.body),
      last: clean(last?.body),
      summary: clean(row.summary_ru, 200),
    },
  };
}

async function main() {
  const rows = await fetchRows();
  const messagesByDialog = await fetchMessages(rows.map((r) => r.dialog_session_id).filter(Boolean));
  const changes = [];

  for (const row of rows) {
    const messages = messagesByDialog.get(String(row.dialog_session_id)) || [];
    if (!messages.length) continue;
    const change = calibrate(row, messages);
    if (change) changes.push(change);
  }

  const summary = {
    mode: APPLY ? 'apply' : 'dry-run',
    days: DAYS,
    scanned_rows: rows.length,
    changed_rows: changes.length,
    issue_changes: {},
    samples: changes.slice(0, 25),
  };

  for (const change of changes) {
    for (const issue of change.before) {
      if (!change.after.includes(issue)) {
        summary.issue_changes[`-${issue}`] = (summary.issue_changes[`-${issue}`] || 0) + 1;
      }
    }
    for (const issue of change.after) {
      if (!change.before.includes(issue)) {
        summary.issue_changes[`+${issue}`] = (summary.issue_changes[`+${issue}`] || 0) + 1;
      }
    }
  }

  if (APPLY) {
    for (const change of changes) {
      const { error } = await supabase
        .from('chat_ai')
        .update(change.patch)
        .eq('id', change.id);
      if (error) throw new Error(`update failed for ${change.id}: ${error.message}`);
    }
  }

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
