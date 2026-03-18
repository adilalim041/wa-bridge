import { logger } from '../config.js';
import { supabase } from '../storage/supabase.js';

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const AI_MODEL = 'claude-sonnet-4-20250514';

const SYSTEM_PROMPT = `Ты — Omoikiri.AI, интеллектуальный ассистент компании Omoikiri Kazakhstan (японская кухонная сантехника: мойки, смесители, аксессуары).
Шоурумы в Астане и Алматы. Ты помогаешь руководителю контролировать менеджеров и анализировать продажи.

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
- Будь проактивным: если видишь проблему, сообщи даже если не спрашивали`;

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
        let query = supabase
          .from('chats')
          .select(
            'session_id, remote_jid, display_name, chat_type, tags, is_muted, last_message_at, phone_number'
          )
          .neq('is_hidden', true)
          .order('last_message_at', { ascending: false })
          .limit(normalizeLimit(input.limit, 20));

        if (input.session_id) {
          query = query.eq('session_id', input.session_id);
        }

        const { data, error } = await query;
        if (error) {
          return JSON.stringify({ error: error.message });
        }

        return JSON.stringify(data || []);
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
          system: SYSTEM_PROMPT,
          tools: TOOLS,
          messages,
        }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        logger.error({ status: response.status, body: errorText }, 'Claude chat API error');
        return { error: `Claude API error: ${response.status}` };
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
