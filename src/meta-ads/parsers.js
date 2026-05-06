/**
 * Meta Ads — pure parsers for structured data fields.
 *
 * Конвертируют raw Meta API jsonb-поля (targeting, placements, object_story_spec)
 * в human-readable структуры. Все функции чистые — без side effects, без I/O.
 *
 * Использование:
 *   import { parseObjectStorySpec, parseTargeting, parsePlacements } from './parsers.js';
 *
 * Зачем:
 *   - targeting / placements хранятся в мета_ad_sets.targeting / .placements как raw jsonb.
 *   - object_story_spec получается lazy per-creative через getCreative().
 *   - UI audit-tree нуждается в человекочитаемых строках и структурированных данных.
 *
 * Тесты: примеры в конце файла (input/output comments). Unit тесты не используем
 * в этом проекте — примеры служат спецификацией и reference для будущих изменений.
 */

// ---------------------------------------------------------------------------
// parseObjectStorySpec
// ---------------------------------------------------------------------------

/**
 * Извлекает destination info из object_story_spec креатива.
 *
 * CTW (Click To WhatsApp) объявления имеют в link_data.link URL вида:
 *   https://wa.me/77001234567?text=Здравствуйте
 *   https://api.whatsapp.com/send?phone=77001234567&text=Привет
 *
 * Для обычных объявлений link_data.link — обычный URL сайта.
 * Также проверяем child_attachments (carousel) — берём первый элемент.
 *
 * @param {object|null|undefined} spec — creative.object_story_spec (jsonb from Meta)
 * @returns {{ landingUrl: string|null, whatsappPhone: string|null, whatsappMessageTemplate: string|null }}
 */
export function parseObjectStorySpec(spec) {
  const empty = { landingUrl: null, whatsappPhone: null, whatsappMessageTemplate: null };

  if (!spec || typeof spec !== 'object') return empty;

  // Извлекаем link из link_data или первого child_attachment (carousel)
  let rawLink = null;

  if (spec.link_data?.link) {
    rawLink = spec.link_data.link;
  } else if (Array.isArray(spec.link_data?.child_attachments) && spec.link_data.child_attachments.length > 0) {
    rawLink = spec.link_data.child_attachments[0]?.link ?? null;
  } else if (spec.video_data?.cta?.value?.link) {
    // Video ads with CTA button
    rawLink = spec.video_data.cta.value.link;
  }

  if (!rawLink || typeof rawLink !== 'string') return empty;

  // Нормализация URL
  let url;
  try {
    url = new URL(rawLink);
  } catch {
    // Невалидный URL — возвращаем как есть без парсинга WA-параметров
    return { landingUrl: rawLink, whatsappPhone: null, whatsappMessageTemplate: null };
  }

  const host = url.hostname.toLowerCase();
  const isWhatsAppUrl = host === 'wa.me' || host === 'api.whatsapp.com';

  if (!isWhatsAppUrl) {
    return { landingUrl: rawLink, whatsappPhone: null, whatsappMessageTemplate: null };
  }

  // --- CTW (Click To WhatsApp) URL ---
  // wa.me format:       https://wa.me/77001234567?text=...
  // api.whatsapp.com:   https://api.whatsapp.com/send?phone=77001234567&text=...

  let phoneRaw = null;

  if (host === 'wa.me') {
    // Телефон в пути: /77001234567
    phoneRaw = url.pathname.replace(/^\//, '').trim();
  } else if (host === 'api.whatsapp.com') {
    // Телефон в query param: ?phone=77001234567
    phoneRaw = url.searchParams.get('phone') ?? null;
  }

  const whatsappPhone = normalizeWhatsAppPhone(phoneRaw);

  // Текст из ?text= (URL-decoded автоматически через URLSearchParams)
  const textParam = url.searchParams.get('text');
  const whatsappMessageTemplate = textParam ? decodeURIComponent(textParam) : null;

  return {
    landingUrl: rawLink,
    whatsappPhone,
    whatsappMessageTemplate,
  };
}

/**
 * Нормализовать номер телефона WhatsApp.
 * Гарантирует формат: "+77001234567" (только цифры с +, без пробелов/скобок).
 *
 * @param {string|null|undefined} phone
 * @returns {string|null}
 */
function normalizeWhatsAppPhone(phone) {
  if (!phone || typeof phone !== 'string') return null;

  // Удаляем всё кроме цифр и + в начале
  const digits = phone.replace(/[^\d+]/g, '');
  if (!digits) return null;

  // Убеждаемся что начинается с +
  const normalized = digits.startsWith('+') ? digits : `+${digits}`;

  // Минимальный phone: +XXXXXXXXXXX (минимум 7 цифр после +)
  if (normalized.length < 8) return null;

  return normalized;
}

// ---------------------------------------------------------------------------
// parseTargeting
// ---------------------------------------------------------------------------

/**
 * Конвертирует raw targeting jsonb в человекочитаемую структуру.
 *
 * Meta targeting имеет сложную иерархию:
 *   - geo_locations: { cities, regions, countries, zips }
 *   - age_min / age_max
 *   - genders: [1] = male, [2] = female, [] или null = all
 *   - flexible_spec: [{ interests: [{id,name},...], behaviors: [...] }]
 *   - excluded_custom_audiences, excluded_geo_locations
 *   - detailed_targeting_advantage_audience: 0|1 — Advantage+ audience
 *
 * @param {object|null|undefined} targeting — raw Meta targeting jsonb
 * @returns {{
 *   summary: string,
 *   geo: { cities: string[], countries: string[], regions: string[], radius: number|null },
 *   age: { min: number|null, max: number|null },
 *   genders: 'female'|'male'|'all',
 *   interestsCount: number,
 *   interestsSample: string[],
 *   isAdvantageAudience: boolean,
 *   raw: object
 * }}
 */
export function parseTargeting(targeting) {
  const fallback = {
    summary: 'Custom targeting',
    geo: { cities: [], countries: [], regions: [], radius: null },
    age: { min: null, max: null },
    genders: 'all',
    interestsCount: 0,
    interestsSample: [],
    isAdvantageAudience: false,
    raw: targeting ?? null,
  };

  if (!targeting || typeof targeting !== 'object') return fallback;

  try {
    // --- Geo ---
    const geoLoc = targeting.geo_locations ?? {};
    const cities = extractNamedArray(geoLoc.cities);
    const regions = extractNamedArray(geoLoc.regions);
    const countries = Array.isArray(geoLoc.countries) ? geoLoc.countries.map(String) : [];
    const radius = geoLoc.custom_locations?.[0]?.radius ?? null;

    // --- Age ---
    const ageMin = targeting.age_min != null ? Number(targeting.age_min) : null;
    const ageMax = targeting.age_max != null ? Number(targeting.age_max) : null;

    // --- Gender ---
    // genders: [1] = male, [2] = female, undefined/null/[1,2] = all
    let genders = 'all';
    if (Array.isArray(targeting.genders) && targeting.genders.length === 1) {
      if (targeting.genders[0] === 1) genders = 'male';
      else if (targeting.genders[0] === 2) genders = 'female';
    }

    // --- Interests from flexible_spec ---
    const allInterests = [];
    if (Array.isArray(targeting.flexible_spec)) {
      for (const group of targeting.flexible_spec) {
        if (Array.isArray(group.interests)) {
          for (const interest of group.interests) {
            if (interest?.name) allInterests.push(interest.name);
          }
        }
        if (Array.isArray(group.behaviors)) {
          for (const b of group.behaviors) {
            if (b?.name) allInterests.push(b.name);
          }
        }
      }
    }

    const isAdvantageAudience =
      targeting.detailed_targeting_advantage_audience === 1 ||
      targeting.detailed_targeting_advantage_audience === true;

    // --- Summary string ---
    const summaryParts = [];

    // Geo summary
    if (cities.length > 0) {
      const radiusPart = radius ? ` (${radius}км)` : '';
      summaryParts.push(`${cities.slice(0, 2).join(', ')}${radiusPart}`);
    } else if (regions.length > 0) {
      summaryParts.push(regions.slice(0, 2).join(', '));
    } else if (countries.length > 0) {
      summaryParts.push(countries.slice(0, 2).join(', '));
    }

    // Gender + age summary
    const genderAgeStr = buildGenderAgeStr(genders, ageMin, ageMax);
    if (genderAgeStr) summaryParts.push(genderAgeStr);

    // Interests summary
    if (allInterests.length > 0) {
      summaryParts.push(`${allInterests.length} интересов`);
    }

    const summary = summaryParts.length > 0 ? summaryParts.join('; ') : 'Targeting configured';

    return {
      summary,
      geo: { cities, countries, regions, radius },
      age: { min: ageMin, max: ageMax },
      genders,
      interestsCount: allInterests.length,
      interestsSample: allInterests.slice(0, 3),
      isAdvantageAudience,
      raw: targeting,
    };
  } catch {
    return { ...fallback, raw: targeting };
  }
}

/**
 * Извлечь массив name-строк из Meta geo array.
 * Meta возвращает: [{key: '...', name: 'Алматы', ...}, ...]
 *
 * @param {Array|undefined} arr
 * @returns {string[]}
 */
function extractNamedArray(arr) {
  if (!Array.isArray(arr)) return [];
  return arr.map((item) => item?.name ?? item?.key ?? '').filter(Boolean);
}

/**
 * Собрать строку "ж 25-45" / "м 18+" / "25-45" и т.п.
 *
 * @param {'female'|'male'|'all'} genders
 * @param {number|null} ageMin
 * @param {number|null} ageMax
 * @returns {string}
 */
function buildGenderAgeStr(genders, ageMin, ageMax) {
  const parts = [];
  if (genders === 'female') parts.push('ж');
  else if (genders === 'male') parts.push('м');

  if (ageMin != null && ageMax != null) parts.push(`${ageMin}-${ageMax}`);
  else if (ageMin != null) parts.push(`${ageMin}+`);
  else if (ageMax != null) parts.push(`до ${ageMax}`);

  return parts.join(' ');
}

// ---------------------------------------------------------------------------
// parsePlacements
// ---------------------------------------------------------------------------

/**
 * Конвертирует raw placements jsonb в человекочитаемую структуру.
 *
 * Meta хранит placements в targeting.publisher_platforms / .facebook_positions /
 * .instagram_positions / .device_platforms (это поле из meta_ad_sets.placements
 * которое sync.js записывает как { publisher_platforms: [...] } или полный
 * targeting объект).
 *
 * Также принимает полный targeting object напрямую — сам ищет нужные поля.
 *
 * @param {object|null|undefined} placements — meta_ad_sets.placements (jsonb)
 * @returns {{
 *   summary: string,
 *   platforms: string[],
 *   positions: Record<string, string[]>,
 *   devices: string[],
 *   raw: object
 * }}
 */
export function parsePlacements(placements) {
  const fallback = {
    summary: 'Advantage+ placements',
    platforms: [],
    positions: {},
    devices: [],
    raw: placements ?? null,
  };

  if (!placements || typeof placements !== 'object') return fallback;

  try {
    // Поддерживаем два формата:
    // 1. { publisher_platforms: [...] } — что sync.js записывает
    // 2. Полный targeting объект (на случай если передали targeting напрямую)
    const src = placements;

    const publisherPlatforms = extractStringArray(src.publisher_platforms);
    const facebookPositions = extractStringArray(src.facebook_positions);
    const instagramPositions = extractStringArray(src.instagram_positions);
    const audienceNetworkPositions = extractStringArray(src.audience_network_positions);
    const messengerPositions = extractStringArray(src.messenger_positions);
    const devicePlatforms = extractStringArray(src.device_platforms);

    // Если нет ничего — Advantage+ Placements (Meta выбирает автоматически)
    if (publisherPlatforms.length === 0) {
      return fallback;
    }

    // Собираем positions map
    const positions = {};
    if (facebookPositions.length > 0) positions.facebook = facebookPositions;
    if (instagramPositions.length > 0) positions.instagram = instagramPositions;
    if (audienceNetworkPositions.length > 0) positions.audience_network = audienceNetworkPositions;
    if (messengerPositions.length > 0) positions.messenger = messengerPositions;

    // Devices
    const devices = devicePlatforms.length > 0
      ? devicePlatforms
      : ['mobile', 'desktop']; // default если не указано

    // --- Summary string ---
    const summaryParts = buildPlacementSummary(publisherPlatforms, positions);

    return {
      summary: summaryParts.length > 0 ? summaryParts.join(', ') : 'All placements',
      platforms: publisherPlatforms,
      positions,
      devices,
      raw: placements,
    };
  } catch {
    return fallback;
  }
}

/**
 * Извлечь массив строк из поля (может быть string[] или просто string).
 *
 * @param {any} val
 * @returns {string[]}
 */
function extractStringArray(val) {
  if (!val) return [];
  if (Array.isArray(val)) return val.map(String).filter(Boolean);
  if (typeof val === 'string') return [val];
  return [];
}

/**
 * Построить список "Feed FB+IG, Reels FB+IG, Stories IG".
 *
 * @param {string[]} platforms
 * @param {Record<string, string[]>} positions
 * @returns {string[]}
 */
function buildPlacementSummary(platforms, positions) {
  // Маппинг позиций Meta → человекочитаемые ярлыки
  const positionLabels = {
    feed: 'Feed',
    reels: 'Reels',
    story: 'Stories',
    right_hand_column: 'RHS',
    instant_article: 'Instant Articles',
    marketplace: 'Marketplace',
    video_feeds: 'Video Feed',
    search: 'Search',
    instream_video: 'In-stream Video',
    profile_feed: 'Profile Feed',
  };

  // Инвертируем: position → платформы где есть
  const positionToPlatforms = {};
  for (const [platform, posArr] of Object.entries(positions)) {
    const platformLabel = platform === 'facebook' ? 'FB'
      : platform === 'instagram' ? 'IG'
      : platform === 'audience_network' ? 'AN'
      : platform === 'messenger' ? 'MSG'
      : platform.toUpperCase();

    for (const pos of posArr) {
      if (!positionToPlatforms[pos]) positionToPlatforms[pos] = [];
      positionToPlatforms[pos].push(platformLabel);
    }
  }

  // Приоритетные позиции для summary
  const priorityOrder = ['feed', 'reels', 'story', 'video_feeds', 'instream_video', 'search'];
  const summaryParts = [];

  for (const pos of priorityOrder) {
    if (positionToPlatforms[pos]) {
      const label = positionLabels[pos] ?? pos;
      const platformsStr = positionToPlatforms[pos].join('+');
      summaryParts.push(`${label} ${platformsStr}`);
    }
  }

  // Остальные позиции которые не в priority list
  for (const [pos, pls] of Object.entries(positionToPlatforms)) {
    if (!priorityOrder.includes(pos)) {
      const label = positionLabels[pos] ?? pos;
      summaryParts.push(`${label} ${pls.join('+')}`);
    }
  }

  // Если нет позиций, просто перечислить платформы
  if (summaryParts.length === 0 && platforms.length > 0) {
    return [platforms.map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join('+')];
  }

  return summaryParts;
}

// ===========================================================================
// ПРИМЕРЫ (используются как reference / inline tests)
// ===========================================================================

/*
─────────────────────────────────────────────────────────────────────────────
parseObjectStorySpec — примеры
─────────────────────────────────────────────────────────────────────────────

ПРИМЕР 1: Типичный CTW (wa.me) ad
INPUT:
  {
    link_data: {
      link: "https://wa.me/77001234567?text=%D0%97%D0%B4%D1%80%D0%B0%D0%B2%D1%81%D1%82%D0%B2%D1%83%D0%B9%D1%82%D0%B5",
      message: "Кухонные мойки от Omoikiri"
    }
  }
OUTPUT:
  {
    landingUrl: "https://wa.me/77001234567?text=%D0%97...",
    whatsappPhone: "+77001234567",
    whatsappMessageTemplate: "Здравствуйте"
  }

ПРИМЕР 2: Обычный трафик на сайт (не CTW)
INPUT:
  {
    link_data: {
      link: "https://omoikiri.kz/sinks",
      message: "Японские мойки"
    }
  }
OUTPUT:
  {
    landingUrl: "https://omoikiri.kz/sinks",
    whatsappPhone: null,
    whatsappMessageTemplate: null
  }

ПРИМЕР 3: Edge case — api.whatsapp.com format
INPUT:
  {
    link_data: {
      link: "https://api.whatsapp.com/send?phone=77771234567&text=Привет"
    }
  }
OUTPUT:
  {
    landingUrl: "https://api.whatsapp.com/send?phone=77771234567&text=Привет",
    whatsappPhone: "+77771234567",
    whatsappMessageTemplate: "Привет"
  }

ПРИМЕР 4: Edge case — null input
INPUT: null
OUTPUT: { landingUrl: null, whatsappPhone: null, whatsappMessageTemplate: null }

ПРИМЕР 5: Edge case — malformed link URL
INPUT: { link_data: { link: "not-a-url" } }
OUTPUT: { landingUrl: "not-a-url", whatsappPhone: null, whatsappMessageTemplate: null }

ПРИМЕР 6: wa.me без text param
INPUT: { link_data: { link: "https://wa.me/77001234567" } }
OUTPUT: { landingUrl: "https://wa.me/77001234567", whatsappPhone: "+77001234567", whatsappMessageTemplate: null }


─────────────────────────────────────────────────────────────────────────────
parseTargeting — примеры
─────────────────────────────────────────────────────────────────────────────

ПРИМЕР 1: Типичный таргетинг (Алматы, женщины 25-45, интересы)
INPUT:
  {
    geo_locations: {
      cities: [
        { key: "3000", name: "Алматы", country: "KZ" }
      ]
    },
    age_min: 25,
    age_max: 45,
    genders: [2],
    flexible_spec: [
      {
        interests: [
          { id: "6003020", name: "Кухня" },
          { id: "6003021", name: "Дом" },
          { id: "6003022", name: "Ремонт" }
        ]
      }
    ],
    detailed_targeting_advantage_audience: 0
  }
OUTPUT:
  {
    summary: "Алматы; ж 25-45; 3 интересов",
    geo: { cities: ["Алматы"], countries: [], regions: [], radius: null },
    age: { min: 25, max: 45 },
    genders: "female",
    interestsCount: 3,
    interestsSample: ["Кухня", "Дом", "Ремонт"],
    isAdvantageAudience: false,
    raw: { ... }
  }

ПРИМЕР 2: Широкий таргетинг (страна, все, Advantage+)
INPUT:
  {
    geo_locations: { countries: ["KZ"] },
    age_min: 18,
    detailed_targeting_advantage_audience: 1
  }
OUTPUT:
  {
    summary: "KZ; 18+",
    geo: { cities: [], countries: ["KZ"], regions: [], radius: null },
    age: { min: 18, max: null },
    genders: "all",
    interestsCount: 0,
    interestsSample: [],
    isAdvantageAudience: true,
    raw: { ... }
  }

ПРИМЕР 3: Edge case — null input
INPUT: null
OUTPUT:
  {
    summary: "Custom targeting",
    geo: { cities: [], countries: [], regions: [], radius: null },
    age: { min: null, max: null },
    genders: "all",
    interestsCount: 0,
    interestsSample: [],
    isAdvantageAudience: false,
    raw: null
  }

ПРИМЕР 4: Edge case — malformed/unexpected structure
INPUT: { weird_field: 123, another: [] }
OUTPUT: fallback с summary: "Custom targeting"


─────────────────────────────────────────────────────────────────────────────
parsePlacements — примеры
─────────────────────────────────────────────────────────────────────────────

ПРИМЕР 1: Типичный Facebook + Instagram с позициями
INPUT:
  {
    publisher_platforms: ["facebook", "instagram"],
    facebook_positions: ["feed", "reels"],
    instagram_positions: ["feed", "reels", "story"],
    device_platforms: ["mobile"]
  }
OUTPUT:
  {
    summary: "Feed FB+IG, Reels FB+IG, Stories IG",
    platforms: ["facebook", "instagram"],
    positions: {
      facebook: ["feed", "reels"],
      instagram: ["feed", "reels", "story"]
    },
    devices: ["mobile"],
    raw: { ... }
  }

ПРИМЕР 2: Только Instagram Stories
INPUT:
  {
    publisher_platforms: ["instagram"],
    instagram_positions: ["story"]
  }
OUTPUT:
  {
    summary: "Stories IG",
    platforms: ["instagram"],
    positions: { instagram: ["story"] },
    devices: ["mobile", "desktop"],
    raw: { ... }
  }

ПРИМЕР 3: Edge case — Advantage+ Placements (нет publisher_platforms)
INPUT: {} или null
OUTPUT:
  {
    summary: "Advantage+ placements",
    platforms: [],
    positions: {},
    devices: [],
    raw: {} / null
  }

ПРИМЕР 4: Edge case — malformed positions (строка вместо массива)
INPUT:
  {
    publisher_platforms: "facebook",
    facebook_positions: "feed"
  }
OUTPUT:
  {
    summary: "Feed FB",
    platforms: ["facebook"],
    positions: { facebook: ["feed"] },
    devices: ["mobile", "desktop"],
    raw: { ... }
  }
  (extractStringArray корректно обработает строку)
*/
