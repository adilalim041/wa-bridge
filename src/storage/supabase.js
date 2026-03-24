import { createClient } from '@supabase/supabase-js';
import { config } from '../config.js';

// Backend uses service_role key to bypass RLS
export const supabase = createClient(config.supabaseUrl, config.supabaseServiceRoleKey);
