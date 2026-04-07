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
- **AI-чат:** Claude Sonnet с function calling (6 tools: get_chats, get_messages, get_ai_analysis, get_manager_analytics, get_contacts, find_problems)
- **ANTHROPIC_API_KEY** в Railway env vars

## ТЕКУЩАЯ СЕССИЯ WHATSAPP
- session_id: `omoikiri-main`
- display_name: "Астана Основной"
- phone: 77014135151

## ЧТО СДЕЛАНО (Волны 2A → 4B)

### Волна 2A — UI + UX
- Тёмная тема Omoikiri с glassmorphism
- Логотипы Omoikiri в topbar и LoginPage (файлы в `src/assets/`)
- Медиа thumbnails + lightbox при клике
- Mute чатов (persistent — сохраняется в Supabase)

### Волна 2B — Поиск + Мульти-сессия
- Fuzzy search через Fuse.js (Мади → Madi)
- Фикс бага 78 непрочитанных (endpoint /read-all)
- Device name "Omoikiri CRM"
- UI для "Все номера" (multi-session selector)

### Волна 2C — Теги + Скрытие
- Теги на чаты (клиент, дизайнер, VIP, спам, партнёр, новый)
- Градиентные tag pills с цветами
- Кастомный dropdown фильтра тегов
- Hidden chats (скрыть чат + удалить переписку из дашборда, Bridge не сохраняет новые сообщения)
- Persistent mute (читается из Supabase при загрузке)

### Волна 3A — CRM Контакты
- Таблица `contacts_crm` в Supabase
- 4 CRUD эндпоинта в Bridge
- Компонент ContactCard с формой (имя, фамилия, роль, компания, город, менеджер, заметки)
- Кнопка "Контакт" в хедере чата

### Волна 3B — Полировка контактов
- Загрузка аватарки через Cloudinary
- Город "Другой" — кастомное поле ввода
- CRM-индикатор в списке чатов (✓ бейдж, CRM-имя вместо pushName, аватарка)
- HomeScreen (заменил EmptyState)
- Esc закрывает чат

### Волна 3C — Страница контактов
- Табы "Чаты" / "Контакты" в сайдбаре
- ContactsList с Fuse.js поиском и фильтром по ролям
- Клик на контакт → ContactCard справа
- "Открыть чат" из карточки контакта → переход на вкладку Чаты

### Волна 4A — AI Фундамент
- Dialog sessions (группировка сообщений, gap > 4 часов = новый диалог)
- AI Worker (Claude Sonnet, каждые 30 сек, обработка очереди)
- Анализ: intent, lead_temperature, lead_source, dialog_topic, deal_stage, sentiment, risk_flags, summary_ru
- Manager response time tracking
- ai_queue с дедупликацией

### Волна 4B — AI Чат
- Endpoint POST /ai/chat с 6 tools (function calling)
- AIChat компонент на главной странице
- Лого omoikiri_ai.PNG как аватарка бота
- Suggestion chips (горячие лиды, без ответа, аналитика, проблемы)
- Typing indicator, markdown formatting, error handling

## ПЛАН ДАЛЬНЕЙШИХ ВОЛН

### Волна 4C — Knowledge Base + Реальные номера
- Добавить в системный промпт AI стандарты Omoikiri (как правильно консультировать, скрипты продаж)
- Подключить реальные номера менеджеров (Алматы)

### Волна 5A — AI Дашборд
- Панель аналитики с графиками (время ответа, конверсия, горячие лиды)

### Волна 5B — Менеджеры
- Таблица менеджеров, привязка к сессиям, рейтинг

### Волна 6 — CRM Воронка
- Этапы сделки, напоминания, повторные продажи

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
