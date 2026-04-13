import { logger } from '../config.js';
import { supabase } from '../storage/supabase.js';
import { KNOWLEDGE_BASE } from './knowledgeBase.js';

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const AI_MODEL = 'claude-sonnet-4-20250514';

function getSystemPrompt() {
  return `Ты — Omoikiri.AI, интеллектуальный ассистент компании Omoikiri Kazakhstan (японская кухонная сантехника: мойки, смесители, аксессуары).
Шоурумы в Астане и Алматы. Ты помогаешь руководителю контролировать менеджеров и анализировать продажи.

Текущая дата и время: ${new Date().toLocaleString('ru-RU', { timeZone: 'Asia/Almaty', dateStyle: 'full', timeStyle: 'short' })}
Часовой пояс: Алматы (UTC+5)

Твои возможности:
- Просматривать все WhatsApp-переписки менеджеров с клиентами
- Анализировать качество обслуживания
- Находить горячих лидов и проблемных клиентов
- Показывать аналитику по времени ответа менеджеров
- Давать рекомендации по улучшению продаж

Правила:
- Отвечай на русском языке
- Будь конкретным: называй имена, номера, даты
- Если данных нет, честно скажи
- Используй инструменты чтобы получить актуальные данные, не придумывай
- Форматируй ответ красиво: используй **жирный** для важного, списки где уместно
- Будь проактивным: если видишь проблему, сообщи даже если не спрашивали
- При оценке работы менеджеров сверяй с нашими стандартами продаж из базы знаний ниже

Когда пользователь просит создать задачу или напоминание:
1. Сначала найди контакт через get_chats с name_search если указано имя
2. Определи remote_jid контакта из результатов
3. Вычисли дату и время (учитывай что пользователь в Алматы, UTC+5)
4. Определи тип задачи: call_back (перезвонить), send_quote (КП), send_catalog (каталог), visit_showroom (шоурум), follow_up (follow-up)
5. Используй create_task с правильными параметрами

${KNOWLEDGE_BASE}`;
}

const TOOLS = [
  {
    name: 'get_chats',
    description: 'Получить список всех чатов WhatsApp с последним сообщением, тегами, статусом мута. Можно фильтровать по session_id.',
    input_schema: {
      type: 'object',
      properties: {
        session_id: {
          type: 'string',
          description: 'ID сессии WhatsApp (например omoikiri-main). Если не указан, берутся все.',
        },
        limit: {
          type: 'number',
          description: 'Количество чатов (по умолчанию 20)',
        },
        name_search: {
          type: 'string',
          description: 'Search chats by contact name or phone (partial match, case-insensitive). Also searches CRM contact names.',
        },
      },
      required: [],
    },
  },
  {
    name: 'get_messages',
    description: 'Получить сообщения конкретного чата. Возвращает текст, отправителя, время.',
    input_schema: {
      type: 'object',
      properties: {
        session_id: {
          type: 'string',
          description: 'ID сессии WhatsApp',
        },
        remote_jid: {
          type: 'string',
          description: 'Номер телефона/ID чата',
        },
        limit: {
          type: 'number',
          description: 'Количество сообщений (по умолчанию 30)',
        },
      },
      required: ['remote_jid'],
    },
  },
  {
    name: 'get_ai_analysis',
    description: 'Получить AI-анализ диалогов: намерение, температура лида, тема, этап сделки, sentiment, risk flags, резюме.',
    input_schema: {
      type: 'object',
      properties: {
        session_id: {
          type: 'string',
          description: 'ID сессии WhatsApp. Если не указан — все сессии.',
        },
        remote_jid: {
          type: 'string',
          description: 'Конкретный чат. Если не указан — все чаты.',
        },
        lead_temperature: {
          type: 'string',
          description: 'Фильтр: hot, warm, cold, dead',
        },
        sentiment: {
          type: 'string',
          description: 'Фильтр: positive, neutral, negative, aggressive',
        },
        limit: {
          type: 'number',
          description: 'Количество (по умолчанию 20)',
        },
      },
      required: [],
    },
  },
  {
    name: 'get_manager_analytics',
    description: 'Получить аналитику по времени ответа менеджеров: среднее время, самые медленные ответы, количество.',
    input_schema: {
      type: 'object',
      properties: {
        session_id: {
          type: 'string',
          description: 'ID сессии (номера WhatsApp)',
        },
        days: {
          type: 'number',
          description: 'За последние N дней (по умолчанию 7)',
        },
      },
      required: [],
    },
  },
  {
    name: 'get_contacts',
    description: 'Получить CRM-контакты: имя, роль, компания, город, заметки.',
    input_schema: {
      type: 'object',
      properties: {
        session_id: {
          type: 'string',
          description: 'ID сессии',
        },
        role: {
          type: 'string',
          description: 'Фильтр по роли: клиент, дизайнер, партнёр и т.д.',
        },
      },
      required: [],
    },
  },
  {
    name: 'find_problems',
    description: 'Найти проблемные чаты: клиенты без ответа, негативный sentiment, risk flags, медленные ответы.',
    input_schema: {
      type: 'object',
      properties: {
        session_id: {
          type: 'string',
          description: 'ID сессии',
        },
        hours_no_response: {
          type: 'number',
          description: 'Порог часов без ответа (по умолчанию 2)',
        },
      },
      required: [],
    },
  },
  {
    name: 'update_deal_stage',
    description: 'Update the deal stage for a contact in the CRM funnel. Use when the user asks to move a contact to a different stage.',
    input_schema: {
      type: 'object',
      properties: {
        session_id: {
          type: 'string',
          description: 'WhatsApp session ID',
        },
        remote_jid: {
          type: 'string',
          description: 'Phone number / chat JID (e.g. 77001234567@s.whatsapp.net)',
        },
        deal_stage: {
          type: 'string',
          enum: ['first_contact', 'consultation', 'model_selection', 'price_negotiation', 'payment', 'delivery', 'completed', 'refused', 'needs_review'],
          description: 'New deal stage',
        },
      },
      required: ['remote_jid', 'deal_stage'],
    },
  },
  {
    name: 'update_tags',
    description: 'Update tags for a chat. Replaces existing tags with the new set.',
    input_schema: {
      type: 'object',
      properties: {
        session_id: {
          type: 'string',
          description: 'WhatsApp session ID',
        },
        remote_jid: {
          type: 'string',
          description: 'Phone number / chat JID',
        },
        tags: {
          type: 'array',
          items: { type: 'string' },
          description: 'New set of tags',
        },
      },
      required: ['remote_jid', 'tags'],
    },
  },
  {
    name: 'create_task',
    description: 'Create a task/reminder in CRM. Use when user asks to schedule follow-ups, callbacks, send quotes, etc. You MUST first use get_chats or get_contacts to find the remote_jid for the contact if user mentions a name.',
    input_schema: {
      type: 'object',
      properties: {
        session_id: { type: 'string', description: 'WhatsApp session ID (default: use current session)' },
        remote_jid: { type: 'string', description: 'Contact phone JID (e.g. 77001234567@s.whatsapp.net). Use get_chats to find it by name.' },
        title: { type: 'string', description: 'Task title, max 200 chars' },
        due_date: { type: 'string', description: 'Due date as ISO 8601 string (e.g. 2026-03-31T12:00:00+05:00)' },
        description: { type: 'string', description: 'Optional details, max 2000 chars' },
        task_type: { type: 'string', enum: ['follow_up', 'call_back', 'send_quote', 'send_catalog', 'visit_showroom', 'custom'], description: 'Type of task' },
        priority: { type: 'string', enum: ['low', 'medium', 'high', 'urgent'], description: 'Priority level' },
        deal_value: { type: 'number', description: 'Expected deal value' },
        notes: { type: 'string', description: 'Additional notes' },
      },
      required: ['title', 'due_date'],
    },
  },
];

function normalizeLimit(value, fallback, max = 100) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }

  return Math.min(parsed, max);
}

async function executeTool(name, input = {}) {
  try {
    switch (name) {
      case 'get_chats': {
        // If searching by name, fetch more rows to allow post-filter
        const fetchLimit = input.name_search ? 200 : normalizeLimit(input.limit, 20);
        let query = supabase
          .from('chats')
          .select(
            'session_id, remote_jid, display_name, chat_type, tags, is_muted, last_message_at, phone_number, push_name'
          )
          .neq('is_hidden', true)
          .order('last_message_at', { ascending: false })
          .limit(fetchLimit);

        if (input.session_id) {
          query = query.eq('session_id', input.session_id);
        }

        const { data, error } = await query;
        if (error) {
          return JSON.stringify({ error: error.message });
        }

        let filtered = data || [];

        // Enrich with CRM contact names so AI can find contacts by CRM name
        const jids = filtered.map((c) => c.remote_jid).filter(Boolean);
        if (jids.length > 0) {
          const { data: crmContacts } = await supabase
            .from('contacts_crm')
            .select('remote_jid, first_name, last_name')
            .in('remote_jid', jids);

          if (crmContacts && crmContacts.length > 0) {
            const crmMap = new Map(crmContacts.map((c) => [c.remote_jid, c]));
            filtered = filtered.map((chat) => {
              const crm = crmMap.get(chat.remote_jid);
              if (crm) {
                const crmName = [crm.first_name, crm.last_name].filter(Boolean).join(' ').trim();
                return { ...chat, crm_name: crmName || undefined };
              }
              return chat;
            });
          }
        }

        // Filter by name_search if provided
        if (input.name_search && typeof input.name_search === 'string') {
          const q = input.name_search.toLowerCase();
          filtered = filtered.filter((c) =>
            (c.display_name || '').toLowerCase().includes(q) ||
            (c.remote_jid || '').includes(q) ||
            (c.push_name || '').toLowerCase().includes(q) ||
            (c.crm_name || '').toLowerCase().includes(q)
          );
        }

        // Apply final limit after filtering
        const finalLimit = normalizeLimit(input.limit, 20);
        filtered = filtered.slice(0, finalLimit);

        return JSON.stringify(filtered);
      }

      case 'get_messages': {
        const sessionId = typeof input.session_id === 'string' && input.session_id.trim()
          ? input.session_id.trim()
          : 'omoikiri-main';
        const remoteJid = typeof input.remote_jid === 'string' ? input.remote_jid.trim() : '';

        if (!remoteJid) {
          return JSON.stringify({ error: 'remote_jid is required' });
        }

        const { data, error } = await supabase
          .from('messages')
          .select('body, from_me, timestamp, message_type, push_name, sender')
          .eq('session_id', sessionId)
          .eq('remote_jid', remoteJid)
          .order('timestamp', { ascending: false })
          .limit(normalizeLimit(input.limit, 30, 200));

        if (error) {
          return JSON.stringify({ error: error.message });
        }

        return JSON.stringify((data || []).reverse());
      }

      case 'get_ai_analysis': {
        let query = supabase
          .from('chat_ai')
          .select(
            'session_id, remote_jid, intent, lead_temperature, lead_source, dialog_topic, deal_stage, sentiment, risk_flags, summary_ru, action_required, action_suggestion, confidence, analyzed_at, message_count_analyzed'
          )
          .order('analyzed_at', { ascending: false })
          .limit(normalizeLimit(input.limit, 20));

        if (input.session_id) {
          query = query.eq('session_id', input.session_id);
        }

        if (input.remote_jid) {
          query = query.eq('remote_jid', input.remote_jid);
        }

        if (input.lead_temperature) {
          query = query.eq('lead_temperature', input.lead_temperature);
        }

        if (input.sentiment) {
          query = query.eq('sentiment', input.sentiment);
        }

        const { data, error } = await query;
        if (error) {
          return JSON.stringify({ error: error.message });
        }

        return JSON.stringify(data || []);
      }

      case 'get_manager_analytics': {
        const days = normalizeLimit(input.days, 7, 365);
        const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
        let query = supabase
          .from('manager_analytics')
          .select(
            'session_id, remote_jid, customer_message_at, manager_response_at, response_time_seconds'
          )
          .gte('customer_message_at', since)
          .not('response_time_seconds', 'is', null)
          .order('customer_message_at', { ascending: false })
          .limit(100);

        if (input.session_id) {
          query = query.eq('session_id', input.session_id);
        }

        const { data, error } = await query;
        if (error) {
          return JSON.stringify({ error: error.message });
        }

        const times = (data || [])
          .map((row) => Number(row.response_time_seconds))
          .filter((value) => Number.isFinite(value) && value >= 0);

        const total = times.reduce((sum, value) => sum + value, 0);
        const stats = {
          total_responses: times.length,
          avg_seconds: times.length ? Math.round(total / times.length) : 0,
          avg_minutes: times.length ? Math.round(total / times.length / 60) : 0,
          fastest_seconds: times.length ? Math.min(...times) : 0,
          slowest_seconds: times.length ? Math.max(...times) : 0,
          slow_over_2h: times.filter((value) => value > 7200).length,
          fast_under_5min: times.filter((value) => value <= 300).length,
          recent_responses: (data || []).slice(0, 10),
        };

        return JSON.stringify(stats);
      }

      case 'get_contacts': {
        let query = supabase
          .from('contacts_crm')
          .select('*')
          .order('updated_at', { ascending: false })
          .limit(100);

        if (input.session_id) {
          query = query.eq('session_id', input.session_id);
        }

        if (input.role) {
          query = query.eq('role', input.role);
        }

        const { data, error } = await query;
        if (error) {
          return JSON.stringify({ error: error.message });
        }

        return JSON.stringify(data || []);
      }

      case 'find_problems': {
        const hoursThreshold = normalizeLimit(input.hours_no_response, 2, 720);
        const cutoff = new Date(Date.now() - hoursThreshold * 60 * 60 * 1000).toISOString();

        let unansweredQuery = supabase
          .from('manager_analytics')
          .select('session_id, remote_jid, customer_message_at')
          .is('manager_response_at', null)
          .lt('customer_message_at', cutoff)
          .order('customer_message_at', { ascending: true })
          .limit(20);

        let negativeQuery = supabase
          .from('chat_ai')
          .select('session_id, remote_jid, sentiment, risk_flags, summary_ru, action_suggestion')
          .in('sentiment', ['negative', 'aggressive'])
          .order('analyzed_at', { ascending: false })
          .limit(10);

        let riskyQuery = supabase
          .from('chat_ai')
          .select('session_id, remote_jid, risk_flags, summary_ru, lead_temperature')
          .not('risk_flags', 'eq', '{}')
          .order('analyzed_at', { ascending: false })
          .limit(10);

        if (input.session_id) {
          unansweredQuery = unansweredQuery.eq('session_id', input.session_id);
          negativeQuery = negativeQuery.eq('session_id', input.session_id);
          riskyQuery = riskyQuery.eq('session_id', input.session_id);
        }

        const [
          { data: unanswered, error: err1 },
          { data: negative, error: err2 },
          { data: risky, error: err3 },
        ] = await Promise.all([unansweredQuery, negativeQuery, riskyQuery]);

        if (err1 || err2 || err3) {
          return JSON.stringify({
            error: err1?.message || err2?.message || err3?.message || 'Failed to find problems',
          });
        }

        return JSON.stringify({
          unanswered_chats: unanswered || [],
          negative_sentiment: negative || [],
          risk_flag_chats: risky || [],
        });
      }

      case 'update_deal_stage': {
        const VALID_STAGES = ['needs_review', 'first_contact', 'consultation', 'model_selection', 'price_negotiation', 'payment', 'delivery', 'completed', 'refused'];
        const sessionId = typeof input.session_id === 'string' && input.session_id.trim()
          ? input.session_id.trim()
          : 'omoikiri-main';
        const remoteJid = typeof input.remote_jid === 'string' ? input.remote_jid.trim() : '';
        const dealStage = input.deal_stage;

        if (!remoteJid) {
          return JSON.stringify({ error: 'remote_jid is required' });
        }

        if (!dealStage || !VALID_STAGES.includes(dealStage)) {
          return JSON.stringify({ error: `Invalid deal_stage. Must be one of: ${VALID_STAGES.join(', ')}` });
        }

        // Find the latest chat_ai record
        const { data: latest, error: findErr } = await supabase
          .from('chat_ai')
          .select('id')
          .eq('session_id', sessionId)
          .eq('remote_jid', remoteJid)
          .order('analysis_date', { ascending: false })
          .limit(1)
          .maybeSingle();

        if (findErr) {
          return JSON.stringify({ error: findErr.message });
        }

        if (latest) {
          const { error: updErr } = await supabase
            .from('chat_ai')
            .update({ deal_stage: dealStage })
            .eq('id', latest.id);

          if (updErr) {
            return JSON.stringify({ error: updErr.message });
          }
        } else {
          // Create minimal chat_ai record
          const { error: insErr } = await supabase.from('chat_ai').insert({
            session_id: sessionId,
            remote_jid: remoteJid,
            deal_stage: dealStage,
            analysis_date: new Date().toISOString().slice(0, 10),
          });

          if (insErr) {
            return JSON.stringify({ error: insErr.message });
          }
        }

        return JSON.stringify({ success: true, dealStage });
      }

      case 'update_tags': {
        const sessionId = typeof input.session_id === 'string' && input.session_id.trim()
          ? input.session_id.trim()
          : 'omoikiri-main';
        const remoteJid = typeof input.remote_jid === 'string' ? input.remote_jid.trim() : '';
        const tags = input.tags;

        if (!remoteJid) {
          return JSON.stringify({ error: 'remote_jid is required' });
        }

        if (!Array.isArray(tags)) {
          return JSON.stringify({ error: 'tags must be an array of strings' });
        }

        // Validate: max 10 tags, each max 50 chars
        const cleanTags = tags
          .filter((t) => typeof t === 'string' && t.trim().length > 0)
          .map((t) => t.trim().slice(0, 50))
          .slice(0, 10);

        const { error: tagErr } = await supabase
          .from('chats')
          .update({ tags: cleanTags, tag_confirmed: true, updated_at: new Date().toISOString() })
          .eq('session_id', sessionId)
          .eq('remote_jid', remoteJid);

        if (tagErr) {
          return JSON.stringify({ error: tagErr.message });
        }

        return JSON.stringify({ success: true, tags: cleanTags });
      }

      case 'create_task': {
        const sessionId = typeof input.session_id === 'string' && input.session_id.trim()
          ? input.session_id.trim()
          : 'omoikiri-main';
        const title = (input.title || '').trim();
        const dueDate = (input.due_date || '').trim();
        if (!title) return JSON.stringify({ error: 'title is required' });
        if (!dueDate) return JSON.stringify({ error: 'due_date is required' });
        if (title.length > 200) return JSON.stringify({ error: 'Title too long (max 200)' });

        // Validate due_date is a valid date
        const parsed = new Date(dueDate);
        if (isNaN(parsed.getTime())) {
          return JSON.stringify({ error: 'Error: invalid due_date format' });
        }

        // Validate remote_jid format if provided
        if (input.remote_jid && !input.remote_jid.includes('@')) {
          return JSON.stringify({ error: 'Error: invalid remote_jid format' });
        }

        const VALID_TYPES = ['follow_up', 'call_back', 'send_quote', 'send_catalog', 'visit_showroom', 'custom'];
        const VALID_PRIORITIES = ['low', 'medium', 'high', 'urgent'];

        const { data, error } = await supabase
          .from('tasks')
          .insert({
            session_id: sessionId,
            remote_jid: input.remote_jid || null,
            title,
            description: input.description || null,
            task_type: VALID_TYPES.includes(input.task_type) ? input.task_type : 'follow_up',
            priority: VALID_PRIORITIES.includes(input.priority) ? input.priority : 'medium',
            status: 'pending',
            due_date: dueDate,
            deal_value: input.deal_value || null,
            notes: input.notes || null,
            created_by: 'ai_chat',
          })
          .select()
          .single();

        if (error) return JSON.stringify({ error: error.message });
        return JSON.stringify({ success: true, taskId: data.id, title: data.title, dueDate: data.due_date });
      }

      default:
        return JSON.stringify({ error: `Unknown tool: ${name}` });
    }
  } catch (error) {
    logger.error({ err: error, tool: name, input }, 'Tool execution error');
    return JSON.stringify({ error: error.message });
  }
}

export async function handleAIChat(conversationHistory) {
  if (!ANTHROPIC_API_KEY) {
    return { error: 'AI не настроен: отсутствует ANTHROPIC_API_KEY' };
  }

  try {
    const messages = Array.isArray(conversationHistory) ? [...conversationHistory] : [];
    let maxIterations = 5;

    while (maxIterations > 0) {
      maxIterations -= 1;

      const response = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
          model: AI_MODEL,
          max_tokens: 4096,
          system: getSystemPrompt(),
          tools: TOOLS,
          messages,
        }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        logger.error({ status: response.status, body: errorText }, 'Claude chat API error');
        return { error: `Claude API error: ${response.status}`, details: errorText.slice(0, 500) };
      }

      const data = await response.json();

      if (data.stop_reason === 'tool_use') {
        messages.push({ role: 'assistant', content: data.content });

        const toolResults = [];
        for (const block of data.content || []) {
          if (block.type !== 'tool_use') {
            continue;
          }

          logger.info({ tool: block.name, input: block.input }, 'AI calling tool');
          const result = await executeTool(block.name, block.input);
          toolResults.push({
            type: 'tool_result',
            tool_use_id: block.id,
            content: result,
          });
        }

        if (toolResults.length === 0) {
          return { error: 'AI запросил инструменты, но не передал ни одного вызова' };
        }

        messages.push({ role: 'user', content: toolResults });
        continue;
      }

      const textContent = (data.content || [])
        .filter((block) => block.type === 'text')
        .map((block) => block.text)
        .join('\n')
        .trim();

      return { response: textContent, usage: data.usage };
    }

    return { error: 'AI превысил лимит вызовов инструментов' };
  } catch (error) {
    logger.error({ err: error }, 'Unexpected error in AI chat');
    return { error: error.message };
  }
}
