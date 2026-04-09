-- ============================================================
-- Cleanup: Remove all data for session 'omoikiri-main'
-- Generated: 2026-04-09
--
-- PURPOSE: Session has been removed from the system.
--          Delete all personal/conversation data associated with it.
--
-- HOW TO RUN: Execute in Supabase SQL Editor (uses service_role implicitly)
-- ============================================================

-- Step 1: Preview — count rows per table BEFORE deletion
-- (Run this block first to verify scope)

DO $$
DECLARE
    v_messages       BIGINT;
    v_chats          BIGINT;
    v_contacts_crm   BIGINT;
    v_dialog_sessions BIGINT;
    v_chat_ai        BIGINT;
    v_ai_queue       BIGINT;
    v_manager_analytics BIGINT;
    v_auth_state     BIGINT;
    v_session_lock   BIGINT;
    v_session_config BIGINT;
    v_manager_sessions BIGINT;
    v_audit_log      BIGINT;
    v_tasks          BIGINT;
BEGIN
    SELECT count(*) INTO v_messages FROM messages WHERE session_id = 'omoikiri-main';
    SELECT count(*) INTO v_chats FROM chats WHERE session_id = 'omoikiri-main';
    SELECT count(*) INTO v_contacts_crm FROM contacts_crm WHERE session_id = 'omoikiri-main';
    SELECT count(*) INTO v_dialog_sessions FROM dialog_sessions WHERE session_id = 'omoikiri-main';
    SELECT count(*) INTO v_chat_ai FROM chat_ai WHERE session_id = 'omoikiri-main';
    SELECT count(*) INTO v_ai_queue FROM ai_queue WHERE session_id = 'omoikiri-main';
    SELECT count(*) INTO v_manager_analytics FROM manager_analytics WHERE session_id = 'omoikiri-main';
    SELECT count(*) INTO v_auth_state FROM auth_state WHERE session_id = 'omoikiri-main';
    SELECT count(*) INTO v_session_lock FROM session_lock WHERE session_id = 'omoikiri-main';
    SELECT count(*) INTO v_session_config FROM session_config WHERE session_id = 'omoikiri-main';
    SELECT count(*) INTO v_manager_sessions FROM manager_sessions WHERE session_id = 'omoikiri-main';
    SELECT count(*) INTO v_audit_log FROM audit_log WHERE session_id = 'omoikiri-main';
    SELECT count(*) INTO v_tasks FROM tasks WHERE session_id = 'omoikiri-main';

    RAISE NOTICE '=== ROWS TO DELETE FOR omoikiri-main ===';
    RAISE NOTICE 'messages:           %', v_messages;
    RAISE NOTICE 'chats:              %', v_chats;
    RAISE NOTICE 'contacts_crm:       %', v_contacts_crm;
    RAISE NOTICE 'dialog_sessions:    %', v_dialog_sessions;
    RAISE NOTICE 'chat_ai:            %', v_chat_ai;
    RAISE NOTICE 'ai_queue:           %', v_ai_queue;
    RAISE NOTICE 'manager_analytics:  %', v_manager_analytics;
    RAISE NOTICE 'auth_state:         %', v_auth_state;
    RAISE NOTICE 'session_lock:       %', v_session_lock;
    RAISE NOTICE 'session_config:     %', v_session_config;
    RAISE NOTICE 'manager_sessions:   %', v_manager_sessions;
    RAISE NOTICE 'audit_log:          %', v_audit_log;
    RAISE NOTICE 'tasks:              %', v_tasks;
    RAISE NOTICE '=======================================';
    RAISE NOTICE 'TOTAL:              %',
        v_messages + v_chats + v_contacts_crm + v_dialog_sessions +
        v_chat_ai + v_ai_queue + v_manager_analytics + v_auth_state +
        v_session_lock + v_session_config + v_manager_sessions +
        v_audit_log + v_tasks;
END $$;


-- ============================================================
-- Step 2: DELETE all data (in a transaction)
--
-- Order matters for foreign keys:
--   manager_sessions → references session_config(session_id)
--   messages.dialog_session_id → references dialog_sessions(id) (logical, no FK constraint)
--   chat_ai.dialog_session_id → references dialog_sessions(id) (logical, no FK constraint)
--
-- Safe order: leaf tables first, then parent tables
-- ============================================================

BEGIN;

-- 1. AI layer (depends on dialog_sessions logically)
DELETE FROM chat_ai WHERE session_id = 'omoikiri-main';
DELETE FROM ai_queue WHERE session_id = 'omoikiri-main';
DELETE FROM manager_analytics WHERE session_id = 'omoikiri-main';

-- 2. Messages (references dialog_session_id logically)
DELETE FROM messages WHERE session_id = 'omoikiri-main';

-- 3. Dialog sessions (parent of AI analysis and messages logically)
DELETE FROM dialog_sessions WHERE session_id = 'omoikiri-main';

-- 4. Chat and CRM data
DELETE FROM chats WHERE session_id = 'omoikiri-main';
DELETE FROM contacts_crm WHERE session_id = 'omoikiri-main';
DELETE FROM tasks WHERE session_id = 'omoikiri-main';

-- 5. Auth and connection state
DELETE FROM auth_state WHERE session_id = 'omoikiri-main';
DELETE FROM session_lock WHERE session_id = 'omoikiri-main';

-- 6. Audit log
DELETE FROM audit_log WHERE session_id = 'omoikiri-main';

-- 7. FK-dependent: manager_sessions BEFORE session_config
DELETE FROM manager_sessions WHERE session_id = 'omoikiri-main';

-- 8. Parent table last (manager_sessions has FK to this)
DELETE FROM session_config WHERE session_id = 'omoikiri-main';

COMMIT;


-- ============================================================
-- Step 3: Verify — all counts should be 0
-- ============================================================

SELECT 'messages' AS "table", count(*) AS remaining FROM messages WHERE session_id = 'omoikiri-main'
UNION ALL
SELECT 'chats', count(*) FROM chats WHERE session_id = 'omoikiri-main'
UNION ALL
SELECT 'contacts_crm', count(*) FROM contacts_crm WHERE session_id = 'omoikiri-main'
UNION ALL
SELECT 'dialog_sessions', count(*) FROM dialog_sessions WHERE session_id = 'omoikiri-main'
UNION ALL
SELECT 'chat_ai', count(*) FROM chat_ai WHERE session_id = 'omoikiri-main'
UNION ALL
SELECT 'ai_queue', count(*) FROM ai_queue WHERE session_id = 'omoikiri-main'
UNION ALL
SELECT 'manager_analytics', count(*) FROM manager_analytics WHERE session_id = 'omoikiri-main'
UNION ALL
SELECT 'auth_state', count(*) FROM auth_state WHERE session_id = 'omoikiri-main'
UNION ALL
SELECT 'session_lock', count(*) FROM session_lock WHERE session_id = 'omoikiri-main'
UNION ALL
SELECT 'session_config', count(*) FROM session_config WHERE session_id = 'omoikiri-main'
UNION ALL
SELECT 'manager_sessions', count(*) FROM manager_sessions WHERE session_id = 'omoikiri-main'
UNION ALL
SELECT 'audit_log', count(*) FROM audit_log WHERE session_id = 'omoikiri-main'
UNION ALL
SELECT 'tasks', count(*) FROM tasks WHERE session_id = 'omoikiri-main';
