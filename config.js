// Fill these in after creating your Supabase project (Project Settings → API).
// The anon key is designed to be public; row-level security keeps each user's data private.
// Leave them empty to run in device-only mode.
window.TL_CONFIG = {
  supabaseUrl: "https://idlbzrsmtpvfpvwnzaee.supabase.co",
  supabaseAnonKey: "sb_publishable_0r1kcGB8IDk1MvSViKpMHQ_Sf8I8F0v",   // publishable key: safe to be public
  ai: true,              // analyst + screenshot import (need ANTHROPIC_API_KEY on the server)
  quotes: true,          // live prices (needs the "quotes" function + FINNHUB_API_KEY)
  refreshMinutes: 5      // how often prices refresh while the app is open
};
