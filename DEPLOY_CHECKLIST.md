# twickell.com B2B Launch — Deploy Checklist

This checklist lives in the repo because the deploy has a few non-obvious steps. Run through it from top to bottom the first time. Most steps you only do once.

---

## 0. Before pushing anything

**Verify what's about to be deployed:**
```bash
cd /home/frank/twickell_deploy
git status         # see all changed files
git diff app.py    # spot-check your own changes
```

If you see any file you don't recognize, ask before pushing.

---

## 1. Configure secrets on Hugging Face Space

You don't touch GitHub for this. Go directly to the Space settings on huggingface.co.

URL: **https://huggingface.co/spaces/Artie1379/twickell/settings**

Under **Variables and secrets → New secret**, add these (the values come from your existing .env files — do NOT paste them here in plain text):

| Name | Where to get the value |
|------|------------------------|
| `ANTHROPIC_API_KEY` | (already set — verify it's still there) |
| `GROQ_API_KEY` | (already set — verify) |
| `HF_TOKEN` | (already set — verify) |
| `STRIPE_SECRET_KEY` | Stripe dashboard → Developers → API keys → Secret key (`sk_live_...` for prod, `sk_test_...` for testing) |
| `STRIPE_WEBHOOK_SECRET` | (Consumer Orby) — existing webhook signing secret, if you already have a webhook at /stripe_webhook |
| `STRIPE_WC_WEBHOOK_SECRET` | (B2B Website Controller) — created in step 3 below |
| `ORBI_EMAIL` | `orbiAisolutions@gmail.com` |
| `ORBI_EMAIL_PASSWORD` | The Gmail app password from `/home/frank/projects/Orbi_Brain/.env` (`ORBI_EMAIL_PASSWORD=...`) |
| `ORBI_DATA_DIR` | `/data` (only if you've enabled persistent storage — see step 2). Skip otherwise; defaults to local. |

Variables (non-secret, can be public):

| Name | Value |
|------|-------|
| `ORBI_WIDGET_URL` | `https://twickell.com/widget` |
| `ORBI_DASHBOARD_URL` | `https://twickell.com` |
| `ORBI_BRAIN_URL` | `https://twickell.com` |
| `ORBI_EMAIL_FROM_NAME` | `Orby AI` |

---

## 2. (Optional but recommended) Enable persistent storage on HF Space

By default, the HF Space filesystem is wiped on every restart. Customer data (business profiles, learned answers, API keys, leads) lives in `data/customers/<id>/`. If the Space restarts (which it does occasionally — upgrades, idle sleep), that data is gone unless you've enabled persistent storage.

Steps:
1. Go to https://huggingface.co/spaces/Artie1379/twickell/settings
2. Find **Persistent storage**
3. Pick the smallest tier (usually $5/mo for ~20GB). For first 50 customers this is plenty.
4. Set `ORBI_DATA_DIR=/data` in env vars (see step 1).

If you skip this, the system still works — but customer data is ephemeral. **Don't onboard a paying customer until persistent storage is on.**

---

## 3. Configure the Stripe webhooks (TWO endpoints, separate secrets)

twickell.com handles TWO products via Stripe:
- Consumer Orby → `/stripe_webhook` (already exists)
- B2B Website Controller → `/api/wc/webhook` (NEW)

Each needs its own endpoint in Stripe with its own signing secret.

### Existing consumer webhook (likely already set up)
- URL: `https://twickell.com/stripe_webhook`
- Signing secret env var: `STRIPE_WEBHOOK_SECRET`
- Don't touch this if it's working.

### NEW B2B webhook
1. Go to https://dashboard.stripe.com/webhooks
2. Click **Add endpoint**
3. Endpoint URL: `https://twickell.com/api/wc/webhook`
4. Description: `Orbi AI Website Controller (B2B)`
5. Events to listen for:
   - `checkout.session.completed`
   - `invoice.payment_failed`
6. After creating, click the new endpoint → **Signing secret** → **Reveal**
7. Copy that secret and paste it into HF Space env var **`STRIPE_WC_WEBHOOK_SECRET`** (note: DIFFERENT variable name from the consumer secret)

Use TEST keys (`sk_test_...`, `whsec_test_...`) first. Once you've tested with a real card in Stripe test mode, switch to LIVE keys.

---

## 4. Push the code (this is the deploy)

You do NOT need to log into GitHub to do this. Your terminal already has the credentials cached. From `/home/frank/twickell_deploy`:

```bash
cd /home/frank/twickell_deploy

git add app.py requirements.txt bridge_routes.py \
        website/business.html \
        website/dashboard.html \
        website/widget/aurora-widget.js \
        website/b2b_checkout_prep.html \
        website/b2b_prep_invalid.html \
        website/privacy.html website/terms.html website/refund.html \
        industry_packs/ \
        DEPLOY_CHECKLIST.md CLAUDE_HANDOFF_FOR_DEPLOY.md

git commit -m "Add AI Website Controller B2B product: chat-driven buy flow, owner dashboard, Stripe checkout, embed code generator"

git push origin main
```

HuggingFace Spaces auto-syncs from GitHub and rebuilds within 1-3 minutes. Watch the build log at:
https://huggingface.co/spaces/Artie1379/twickell?logs=build

The build will:
1. Pull the new code
2. Install dependencies (now including `flask-cors`)
3. Restart gunicorn
4. Serve at twickell.com (your custom domain)

**Don't push anything else until you see the build succeed.**

---

## 5. Verify the deploy

After build completes, open in a browser:

| URL | What you should see |
|-----|---------------------|
| `https://twickell.com/` | New B2B front page (gold/dark-blue, "Meet your AI staff" hero, chat widget) |
| `https://twickell.com/personal` | OLD consumer Orby page (still intact) |
| `https://twickell.com/business_demo_chat` (POST) | 405 if you GET it — that's expected. POST is the real endpoint. |
| `https://twickell.com/widget/aurora-widget.js` | JavaScript content (200 OK) |
| `https://twickell.com/privacy#section_20` | (link in business.html — section 20 exists in terms.html) |
| `https://twickell.com/terms#section_20` | Section 20 B2B addendum visible |

If the new front page loads but the chat is silent or errors, check:
- HF Space build logs for Python errors
- HF Space env vars are all set (especially LLM keys)
- Browser dev tools → network tab → `/business_demo_chat` request → response payload

---

## 6. End-to-end test with Stripe TEST keys

Run this BEFORE telling any real customer to buy.

1. On `https://twickell.com/`, talk to Orby in the chat.
2. Say: "I want to buy the Starter tier."
3. Walk through her qualification: business name, type, website, email.
4. Wait for the scrape to complete (she'll say "I see you're a [industry]...").
5. Confirm.
6. She'll say "ready for legal review?" → say yes.
7. You should be redirected to `/b2b-checkout-prep?token=...`
8. Check all the boxes on the legal review page.
9. Click "Continue to payment".
10. Stripe checkout opens (TEST mode).
11. Use Stripe test card: `4242 4242 4242 4242`, any future expiry, any CVC.
12. Pay.
13. You should redirect to `/wc/success` (welcome page).
14. Check your email at `orbiAisolutions@gmail.com` outbox for the welcome email. Note: it sends FROM that address, so to test you need to put your own email in step 3, not orbiAisolutions@.
15. Click the dashboard link in the email → you should see the empty owner dashboard for the test customer.
16. The embed code is in the dashboard's "Embed Code" tab. Copy it. Paste it into a test HTML file. Open it. Chat with Orby. She should respond.

If any step fails, check `HF Space → Logs → Container` for the Python error.

---

## 7. Cut over to LIVE Stripe keys

Once test-mode E2E works:

1. In Stripe dashboard, switch the toggle from "Test mode" to "Live mode".
2. Get the live `sk_live_...` secret key.
3. Create a new webhook endpoint pointing at `https://twickell.com/api/wc/webhook` in LIVE mode and copy its signing secret (`whsec_live_...`).
4. Update HF Space env vars `STRIPE_SECRET_KEY` and `STRIPE_WEBHOOK_SECRET` with the LIVE values.
5. HF Space will restart automatically. Wait ~30 seconds.
6. Now real money will flow on real card purchases.

---

## 8. Things that are NOT deployed and don't need to be (yet)

These run locally only and aren't part of the twickell.com deploy:

- The standalone Bridge service on `127.0.0.1:5080` (it's been replaced by `bridge_routes.py` inside twickell)
- The Orbi_Brain on port 5060 (this was the original brain prototype — its `/chat` logic is now ported into twickell as `/chat`)
- The Ultimate Bridge Brain on port 8091 (separate exploration project)

You can leave them running on your laptop — they don't conflict with the deployed twickell.

---

## 9. Quick reference — files that matter

| File | Why |
|------|-----|
| `app.py` | Main Flask app — has consumer Orby routes + B2B chat + customer `/chat` |
| `bridge_routes.py` | All Bridge endpoints as a Blueprint |
| `requirements.txt` | Added `flask-cors` |
| `website/business.html` | NEW B2B front page (served at `/`) |
| `website/index.html` | OLD consumer Orby page (now served at `/personal`) |
| `website/b2b_checkout_prep.html` | Legal review page |
| `website/dashboard.html` | Owner dashboard |
| `website/widget/aurora-widget.js` | Customer-side chat widget |
| `website/privacy.html` | Privacy policy (+ Section 14 B2B addendum) |
| `website/terms.html` | Terms (+ Section 20 B2B addendum) |
| `data/customers/<id>/` | Per-customer state — created at runtime |

---

## 10. Rollback

If the deploy goes sideways, revert is just:

```bash
cd /home/frank/twickell_deploy
git log --oneline -5    # find the previous good commit hash
git revert <bad commit hash>
git push origin main
```

HF Space rebuilds with the prior version in 1-3 minutes. You don't lose the new code — it's still in git history; the revert just reverses the diff.
