# OMOIKIRI.AI — WhatsApp Hub + CRM + AI Analytics

## КТО Я
Adil — маркетолог и предприниматель из Астаны, Казахстан. Управляет официальной дистрибуцией Omoikiri (японская кухонная сантехника: мойки, смесители). Шоурумы в Астане и Алматы. ~80% клиентов приходят через дизайнеров интерьера.

## ЧТО МЫ СТРОИМ
**Omoikiri.AI** — система контроля WhatsApp-переписок менеджеров + CRM + AI-аналитика. Это НЕ для менеджеров — это инструмент руководителя (Adil) для контроля качества обслуживания, анализа продаж и автоматизации.

## АРХИТЕКТУРА

### 1. WA Bridge (Node.js + Baileys) — Railway
- **URL:** `wa-bridge-production-7cd0.up.railway.app`
- **GitHub:** `adilalim041/wa-bridge`
- Мульти-сессия WhatsApp (N номеров одновременно)
- REST API: `/sessions`, `/chats`, `/messages`, `/send`, `/read-all`, `/mute`, `/hide`, `/tags`, `/contacts-crm`, `/ai/chat`
- WebSocket `/ws` для real-time сообщений
- Anti-ban: delays, rate-limiting, browser fingerprint
- Auth state в Supabase (переживает редеплой)
- Медиа: Baileys → Cloudinary (`do0zl6hbd`)
- AI Pipeline: dialog sessions → queue → Claude Sonnet worker (каждые 30 сек)
- Device name: "Omoikiri CRM"

### 2. WA Dashboard (React 19 + Vite) — Vercel
- **URL:** `wa-dashboard-blond.vercel.app`
- **GitHub:** `adilalim041/wa-dashboard`
- Тёмная тема Omoikiri (чёрный фон + teal ambient glow + glassmorphism)
- Цвета бренда: красный #C8102E, синий #0077C8, фиолетовый #981D97
- Баблы: красный градиент = входящие, синий = исходящие
- Шрифты: Inter + JetBrains Mono
- Все CSS в `src/main.jsx` template literal (НЕ отдельные файлы)
- Авторизация через Supabase Auth
- Навигация: табы "Чаты" / "Контакты" в сайдбаре
- AI-чат на главной странице (HomeScreen = AIChat)

### 3. Supabase — проект WPAdil
- **ID:** `gehiqhnzbumtbvhncblj`
- **URL:** `gehiqhnzbumtbvhncblj.supabase.co`
- **ВАЖНО:** У Adil два проекта Supabase. Этот проект использует ТОЛЬКО WPAdil (`gehiqhnzbumtbvhncblj`), НЕ `advluvxpllxxzjrxeskm`

**Таблицы:**
- `messages` — все сообщения (session_id, remote_jid, body, from_me, timestamp, ai_processed, dialog_session_id)
- `chats` — чаты (session_id, remote_jid, tags[], is_muted, is_hidden, display_name)
- `contacts_crm` — CRM-контакты (first_name, last_name, role, company, city, responsible_manager, avatar_url, notes)
- `session_config` — конфиг сессий WhatsApp
- `dialog_sessions` — группировка сообщений в диалоги (gap > 4 часов = новая сессия)
- `chat_ai` — AI-анализ диалогов (intent, lead_temperature, lead_source, dialog_topic, deal_stage, sentiment, risk_flags, summary_ru, action_required, action_suggestion)
- `ai_queue` — очередь на AI-обработку
- `manager_analytics` — время ответа менеджеров (customer_message_at, manager_response_at, response_time_seconds)
- `auth_state`, `session_lock`, `manager_sessions` — сервисные таблицы

### 4. Cloudinary
- **Cloud name:** `do0zl6hbd`
- Папки: `ai_news` (AdilFlow), `omoikiri_crm/avatars` (аватарки контактов)

### 5. AI Stack
- **Фоновый анализ:** Claude Sonnet через Anthropic API, worker в Bridge каждые 30 сек
- **AI-чат:** Claude Sonnet с function calling (9 tools: get_chats, get_messages, get_ai_analysis, get_manager_analytics, get_contacts, find_problems, update_deal_stage, update_tags, create_task)
- **ANTHROPIC_API_KEY** в Railway env vars

## АКТИВНЫЕ СЕССИИ WHATSAPP

Не хардкодим в этом файле — реальный список меняется при подключении новых номеров. Источник правды: таблица `session_config` в Supabase (`is_active=true`). На 2026-04-29 активны 6 сессий: `almaty-rabochiy-reklama`, `astana-nursultan`, `almaty-armada`, `astana-aytzhan`, `almaty-nurbolat`, `astana-renat-rabochiy-reklama`.

## БЭКЛОГ И ПЛАН РАБОТЫ

**Источник правды для приоритетов и активных задач:** [`ObsidianVault/projects/omoikiri/backlog.md`](../ObsidianVault/projects/omoikiri/backlog.md). Там же ссылки на свежие аудиты, security-задачи (Q-1..Q-6, hands-off readiness) и forward-looking risks.

Старые "волны" (2A-4B → 4C → 5A-B → 6) использовались до 2026-04-22 как кодовые названия фаз. Большинство реализовано (V1 templatization, Phase 2/3 RLS, Knowledge Base, AI Дашборд, CRM воронка частично). Сейчас задачи трекаются в backlog как `Task X.Y` или `Q-N`.

**Архив описания закрытых волн (для исторического контекста)** — см. [`ObsidianVault/projects/omoikiri/decisions.md`](../ObsidianVault/projects/omoikiri/decisions.md) и audits.

## ПРАВИЛА РАБОТЫ

1. **PowerShell не поддерживает `&&`** — все команды терминала давать отдельно
2. **CSS живёт в `src/main.jsx`** — НЕ создавать отдельные CSS-файлы
3. **НЕ МЕНЯТЬ высоту `.dashboard-topbar__logo-img`** — Adil настроил размер вручную
4. **Supabase = WPAdil** (`gehiqhnzbumtbvhncblj`), НЕ старый проект
5. **GitHub:** `adilalim041` — оба репо (wa-bridge, wa-dashboard)
6. **Деплой:** wa-dashboard → Vercel (auto), wa-bridge → Railway (auto), оба по push в main
7. **Логотипы в `wa-dashboard/src/assets/`:** white_text.png, color_logo.png, black_logo.png, black_text.png, black_omoikiri.png, omoikiri_ai.PNG
8. **AI модель:** Claude Sonnet (`claude-sonnet-4-20250514`) через Anthropic API
9. **Язык интерфейса:** русский
10. **Adil предпочитает** большие промпты которые делают много за раз, а не маленькие итеративные изменения

## НЕЗАВИСИМЫЙ АУДИТ — senior-reviewer субагент

В команде есть независимый аудитор уровня Staff Engineer: `~/.claude/agents/senior-reviewer.md`. Он **не пишет код**, он ищет проблемы, которые implementer пропустил, и выдаёт ranked findings в `ObsidianVault/projects/omoikiri/audits/`.

### Когда ОБЯЗАТЕЛЬНО вызывать senior-reviewer

1. **После крупной волны** (4C, 5A, 6, ...) — перед тем как сказать "волна закрыта".
2. **Перед мержем в main** если диф >500 строк ИЛИ трогает `src/api/`, `src/ai/`, `src/storage/supabase.js`, auth, миграции.
3. **Перед деплоем нового клиента шаблона** — чтобы не разослать уязвимость на N инстансов.
4. **Раз в неделю автоматически** — через scheduled task `weekly-senior-audit` (воскресенье 22:00 или первое открытие после 6 дней).
5. **По запросу Adil'а** ("проведи аудит", "проверь безопасность").

### Как вызывать

```
Agent(
  subagent_type: "senior-reviewer",
  description: "Audit after wave X",
  prompt: "Проект Omoikiri.AI, стадия: {MVP/Production/SaaS}.
           Триггер: {after-wave / pre-merge / pre-client-deploy / weekly}.
           Конкретный scope: {вся система / diff волны X / src/ai/ + src/api/auth.js}.
           Следуй инструкциям в ~/.claude/agents/senior-reviewer.md полностью.
           Отчёт в ObsidianVault/projects/omoikiri/audits/{today}-audit.md."
)
```

### После отчёта

- Прочитай executive summary отчёта. Если CRITICAL — приоритезируй починку до новых фич.
- CRITICAL/HIGH финдинги перенеси в `ObsidianVault/projects/omoikiri/backlog.md` (если ещё нет).
- **Не игнорируй "Still open" пункты** — если CRITICAL повторяется 3 аудита подряд, остановись и починти, прежде чем делать следующую фичу.

### Чего senior-reviewer НЕ делает

- Не пишет код (фиксы делают `backend-dev` / `frontend-dev` / `integrations-dev`).
- Не правит decisions.md (осознанные trade-off'ы не критикует).
- Не блокирует push / main.

### История аудитов

См. `ObsidianVault/projects/omoikiri/audits/_index.md` — там список с датами и score. Baseline: 2026-04-16, 6.5/10.
