/**
 * Lead source enum — single source of truth для chat_ai.lead_source.
 *
 * Audit fix H-5 (2026-05-04): раньше enum определялся в 3 местах:
 *   - src/ai/schemas.js (Zod enum)
 *   - src/ai/aiWorker.js (prompt instruction text)
 *   - SKILL.md (manual analysis vocabulary)
 * Расхождение приводило к тому что Claude (через Claude Max в SKILL.md)
 * возвращал 'omoikiri_ad' / 'organic' / 'walkin' — но Zod не имел этих
 * значений в enum → .catch('unknown') → данные терялись.
 *
 * Этот файл — единственный источник truth. schemas.js + aiWorker.js + (когда-то)
 * frontend filters все импортируют его.
 *
 * Adil's bizdev: реклама Omoikiri крутится на 2 сессии (astana-renat-rabochiy-reklama
 * и almaty-rabochiy-reklama). Если первое incoming на этих сессиях → omoikiri_ad
 * автоматически (детерминистический сигнал, см. SKILL.md S0).
 */

// Канонические значения которые Claude должен возвращать в анализе.
// Группированы по приоритету детекции (см. SKILL.md decision tree).
export const LEAD_SOURCE_CANONICAL = [
  // Самые частые первичные источники
  'omoikiri_ad',         // платная реклама Omoikiri (IG / FB / Google / TikTok)
  'instagram',           // органический IG (хэштег, профиль, не из paid ad)
  'website',             // «заявка с сайта»
  'referral',            // «по совету / посоветовал X»
  'existing_customer',   // повторный клиент (customer_history.is_existing=true)
  'walkin',              // «был в шоуруме / приходил вчера»
  'manager_internal',    // лид от внутреннего менеджера (Айтжан / Нурсултан / Сан и т.д.)
  'organic',             // органический поиск, без явного источника
  'unknown',             // непонятно (rare; используем organic если есть хоть какой-то текст)
];

// Legacy values — раньше использовались в schemas.js до H-5 unification 2026-05-04.
// Уже записанные в БД chat_ai rows могут содержать эти значения. Zod должен их
// принимать чтобы не ломать historical data. aiWorker НЕ возвращает их в prompt.
export const LEAD_SOURCE_LEGACY = [
  'instagram_ad',     // → omoikiri_ad (paid)
  'google_ad',        // → omoikiri_ad
  'word_of_mouth',    // → referral
  'repeat_client',    // → existing_customer
  'designer_partner', // → referral_designer
  'showroom_visit',   // → walkin
  'incoming_call',    // → manager_internal
  'ad',               // → omoikiri_ad
];

// Полный enum для Zod validation — canonical + legacy backward compat.
export const LEAD_SOURCE_ENUM = [...LEAD_SOURCE_CANONICAL, ...LEAD_SOURCE_LEGACY];

// Маппинг legacy → canonical (для миграции и аналитики).
// Можно использовать в analytics aggregations чтобы группировать legacy под
// современные ключи: «instagram_ad» rows покажутся в omoikiri_ad bucket.
export const LEAD_SOURCE_LEGACY_MAP = {
  instagram_ad: 'omoikiri_ad',
  google_ad: 'omoikiri_ad',
  ad: 'omoikiri_ad',
  word_of_mouth: 'referral',
  repeat_client: 'existing_customer',
  designer_partner: 'referral',
  showroom_visit: 'walkin',
  incoming_call: 'manager_internal',
};

// Helper: нормализовать legacy → canonical (для analytics aggregations).
export function canonicalLeadSource(rawValue) {
  if (!rawValue) return 'unknown';
  return LEAD_SOURCE_LEGACY_MAP[rawValue] || rawValue;
}

// AI prompt-friendly text — для inline instruction в aiWorker.js / SKILL.md.
// Перечисляем ТОЛЬКО canonical (legacy не возвращается LLM, только в БД для backcompat).
export const LEAD_SOURCE_PROMPT_LIST = LEAD_SOURCE_CANONICAL.join(', ');
