# Claude Handoff — Walking Frank Through the twickell.com B2B Deploy

> **You are the helper Claude.** Frank is non-technical. He is going to share his screen / paste output to you while you walk him through the steps below. The other Claude (the one who built this) wrote all this code earlier today and is still working in parallel on other things — you don't need to coordinate. Just walk Frank through deployment.
>
> If you hit something you're unsure about, stop and tell Frank to bring you the technical reference: `/home/frank/twickell_deploy/DEPLOY_CHECKLIST.md`. Don't guess.

---

## What Frank built today (so you have context)

He's launching the AI Website Controller — a B2B SaaS product. Today's session shipped:

1. A new front page on twickell.com (`business.html`) — chat-first, B2B-focused
2. Orby on that page is now a sales agent: she walks visitors through qualification, scrapes their site, talks them through pricing, and sends them to a legal-review page before payment
3. A legal-review page that requires all checkboxes before Stripe checkout
4. Stripe checkout for the AI Website Controller ($99 / $199 / $349/mo + $299 setup)
5. An auto-provisioning system: when payment succeeds, the system creates the customer's folder, generates their API key + embed code + owner dashboard link, and emails it to them
6. An owner dashboard at `/dashboard?token=<owner_token>` where customers answer pending questions, see leads, and edit their business profile
7. A `/chat` endpoint that the customer's embed code on their own website actually calls
8. Updates to privacy + terms with B2B-specific addenda (Section 14 in privacy, Section 20 in terms)

All this code is **on Frank's laptop in `/home/frank/twickell_deploy/`**. None of it is on the internet yet. The current live twickell.com is the OLD consumer Orby site.

**Your job:** walk Frank through getting it on the internet safely.

---

## The deployment path in plain English

twickell.com is hosted on a free service called **Hugging Face Spaces**. The way deploys work:

1. Frank's code lives on GitHub (in a repo called `franklstreet-svg/twickell`).
2. Hugging Face watches that GitHub repo.
3. When Frank pushes new code to GitHub, Hugging Face automatically rebuilds and redeploys twickell.com.
4. Total time from push to live site: about 2-5 minutes.

For the deploy to work, **Frank also needs to set some environment variables** on the Hugging Face Space settings page (these tell the app his Stripe keys, his email password, etc.). And he needs to set up a **Stripe webhook** so Stripe can tell Frank's app when someone pays.

That's the whole picture. Now walk Frank through it.

---

# Walkthrough — Do These Steps With Frank

## STEP 1 — Open a terminal and check what's about to be deployed

Tell Frank:
> "Open your terminal. Type `cd /home/frank/twickell_deploy` and press Enter. Then type `git status` and press Enter."

Look at his output. You should see things like:
- A list of modified files (M) including `app.py`, `requirements.txt`, `website/privacy.html`, `website/terms.html`
- A list of "untracked" files (??) including `bridge_routes.py`, `website/business.html`, `website/dashboard.html`, several `website/b2b_*.html`, `website/widget/aurora-widget.js`, `DEPLOY_CHECKLIST.md`, and `CLAUDE_HANDOFF_FOR_DEPLOY.md` (this file)

If the output looks roughly like that → ✅ proceed to step 2.

If the output is wildly different (e.g., shows a different repo, or no changes at all) → STOP. Tell Frank: *"This doesn't look right — let me get the other Claude to check."*

---

## STEP 2 — Set environment variables on Hugging Face Space

Tell Frank:
> "Open your web browser. Go to this URL: https://huggingface.co/spaces/Artie1379/twickell/settings"

He'll need to be logged in as the `Artie1379` account.

On that page, scroll to **Variables and secrets**. There are TWO kinds:
- **Variables** — public, anyone can see them
- **Secrets** — private, only the app sees them

### Secrets to add (these are PRIVATE — passwords and API keys)

Click **"New secret"** for each. The name goes in the "Name" field, the value goes in the "Value" field.

| Secret name | Where Frank gets the value |
|-------------|---------------------------|
| `ANTHROPIC_API_KEY` | Should ALREADY be set — verify it's there. If not, get from `/home/frank/projects/Orbi_Brain/.env` |
| `GROQ_API_KEY` | Should ALREADY be set — verify |
| `HF_TOKEN` | Should ALREADY be set — verify |
| `STRIPE_SECRET_KEY` | We'll come back for this in Step 4 — skip for now |
| `STRIPE_WEBHOOK_SECRET` | We'll come back for this in Step 4 — skip for now |
| `ORBI_EMAIL` | Type exactly: `orbiAisolutions@gmail.com` |
| `ORBI_EMAIL_PASSWORD` | Frank can find this in his file `/home/frank/projects/Orbi_Brain/.env` on the line that starts with `ORBI_EMAIL_PASSWORD=`. Have him copy the value AFTER the equals sign. |

### Variables to add (these are PUBLIC — URLs, names)

Click **"New variable"** for each.

| Variable name | Value to paste |
|---------------|---------------|
| `ORBI_WIDGET_URL` | `https://twickell.com/widget` |
| `ORBI_DASHBOARD_URL` | `https://twickell.com` |
| `ORBI_BRAIN_URL` | `https://twickell.com` |
| `ORBI_EMAIL_FROM_NAME` | `Orby AI` |

When done, the page should show all 11 entries. Have Frank screenshot it for you so you can verify.

---

## STEP 3 — Enable persistent storage on the Hugging Face Space

This is important. Without it, customer data disappears every time the Space restarts (which it does — for updates, when idle too long, etc.).

Tell Frank:
> "On the same Hugging Face settings page, scroll to find a section called **Persistent storage**. There should be a button like 'Upgrade' or 'Enable storage'. Click it."

Hugging Face will offer storage tiers. The smallest tier (around $5/month for ~20GB) is plenty for the first hundreds of customers.

After he enables it, go back to Step 2 and add one more secret:

| Secret name | Value |
|-------------|-------|
| `ORBI_DATA_DIR` | `/data` |

**If Frank doesn't want to pay for persistent storage yet:** that's OK for early testing, but **tell him explicitly:** *"Don't onboard a paying customer until persistent storage is on, because their data will be lost on the next Space restart."*

---

## STEP 4 — Set up the Stripe webhooks (note: TWO of them)

Frank's twickell.com has TWO Stripe-driven products:
- Consumer "My Orby" (existing — handled by `/stripe_webhook`)
- B2B "AI Website Controller" (new — handled by `/api/wc/webhook`)

Each needs its OWN webhook endpoint in Stripe with its OWN signing secret. They cannot share.

Tell Frank:
> "Open a new tab. Go to https://dashboard.stripe.com/webhooks — log in to your Stripe account."

Make sure the top-right toggle is in **TEST MODE** for now.

### 4a — Verify the consumer webhook (already exists, probably)

Check whether there's already a webhook endpoint pointing to `https://twickell.com/stripe_webhook`. If yes:
- Click into it
- Reveal the signing secret
- Make sure `STRIPE_WEBHOOK_SECRET` env var on HuggingFace matches this value (this is the CONSUMER one)
- Leave it alone otherwise — don't change events or URL

If no consumer webhook exists, skip this — Frank may have never set up one.

### 4b — Add a NEW webhook for the B2B Website Controller

1. Click **"+ Add endpoint"**
2. For "Endpoint URL", type exactly: `https://twickell.com/api/wc/webhook`
3. For "Description", type: `Orbi AI Website Controller (B2B)`
4. For "Listen to → Select events", click **"+ Select events"** and check ONLY these two:
   - `checkout.session.completed`
   - `invoice.payment_failed`
5. Click **"Add endpoint"**

You'll land on the new endpoint's page. Look for **"Signing secret"** with a "Reveal" or "Click to reveal" button. Click it.

A string starting with `whsec_...` appears. Have Frank COPY that whole string.

Now go back to the HuggingFace Space settings tab and add this — note the variable name is DIFFERENT from the consumer one:

| Secret name | Value |
|-------------|-------|
| `STRIPE_WC_WEBHOOK_SECRET` | The `whsec_...` string from the B2B endpoint Frank just created |
| `STRIPE_WEBHOOK_SECRET` | (Already there if 4a applied; leave alone) |

Then add the Stripe secret key. In the Stripe dashboard, click **Developers → API keys** in the left sidebar. Find the **Secret key** (starts with `sk_test_...` in test mode, or `sk_live_...` in live mode). Click "Reveal". Copy it.

Back to HF Space settings:

| Secret name | Value |
|-------------|-------|
| `STRIPE_SECRET_KEY` | The `sk_test_...` (or `sk_live_...`) value |

⚠️ **STRONGLY RECOMMEND starting in TEST MODE.** Frank should do the first end-to-end test with test cards before switching to live. Test mode means real card data isn't processed, but the whole flow runs.

---

## STEP 5 — Push the code to GitHub (the actual deploy)

This is where the new B2B site goes live.

Tell Frank:
> "Back in your terminal — make sure you're still in the right folder. Type `pwd` and press Enter. It should say `/home/frank/twickell_deploy`."

If it doesn't, have him type `cd /home/frank/twickell_deploy` first.

Then have him paste this WHOLE block:

```bash
git add app.py requirements.txt bridge_routes.py \
        website/business.html \
        website/dashboard.html \
        website/widget/aurora-widget.js \
        website/b2b_checkout_prep.html \
        website/b2b_prep_invalid.html \
        website/privacy.html website/terms.html website/refund.html \
        industry_packs/ \
        DEPLOY_CHECKLIST.md CLAUDE_HANDOFF_FOR_DEPLOY.md
```

Then:

```bash
git commit -m "Add AI Website Controller B2B product: chat-driven buy flow, owner dashboard, Stripe checkout, embed code generator"
```

Then:

```bash
git push origin main
```

The push will take 10-30 seconds. He should see lines like:
```
Enumerating objects: ...
Counting objects: ...
Writing objects: ...
remote: ...
To https://github.com/franklstreet-svg/twickell.git
   abc1234..def5678  main -> main
```

If he gets a permission error or authentication prompt, that's an OAuth/token issue. Ask the other Claude.

If the push succeeds, IMMEDIATELY have him open this URL to watch the build:
> https://huggingface.co/spaces/Artie1379/twickell?logs=build

He should see the Space rebuilding. It'll say "Building" → eventually "Running" — that means it's live.

If it shows "Failed" or "Error" — have Frank paste the last 50 lines of the build log to you. Common issues: missing env var, Python import error, etc.

---

## STEP 6 — Verify the deploy

Once the HF Space says "Running", have Frank visit these URLs in his browser:

1. **`https://twickell.com/`** — Should show the new B2B front page: gold/dark-blue/black theme, "Meet your AI staff" hero, chat widget visible
2. **`https://twickell.com/personal`** — Should show the OLD consumer Orby (this is preserved on purpose)
3. **`https://twickell.com/privacy`** — Scroll to Section 14 — should see the B2B addendum
4. **`https://twickell.com/terms`** — Scroll to Section 20 — should see the B2B addendum

If everything looks right, proceed to Step 7.

If anything is broken (the front page is the old one, or the page is blank), check:
- HF Space build log (did the deploy actually finish?)
- Browser cache (have Frank do a hard refresh: Ctrl+Shift+R on Linux/Windows, Cmd+Shift+R on Mac)

---

## STEP 7 — Full end-to-end test with a fake purchase

Now Frank pretends to be a customer. Use STRIPE TEST MODE.

1. Open `https://twickell.com/` in a private/incognito browser window
2. Scroll to the chat (it's right in the middle of the page)
3. Type to Orby: **"I'd like to buy the Starter tier"**
4. She'll ask for business name. Say: **"Acme Plumbing"**
5. She'll ask what kind of business. Say: **"plumbing contractor"**
6. She'll ask for website. Say: **`https://example.com`** (or any real URL)
7. She'll ask for email. Use Frank's actual email so he gets the welcome email.
8. After he gives the website, Orby pauses, scrapes it, and reports what she found.
9. She'll ask about traffic — say: **"under 500"**
10. She'll summarize the order and ask "ready for legal review?" — say **"yes"**.
11. The browser should redirect to `/b2b-checkout-prep?token=...`
12. Check every box on the legal review page.
13. Click **"I accept all — Continue to payment →"**
14. Stripe checkout opens.
15. Use the test card: **`4242 4242 4242 4242`**, any future expiry (e.g., `12/30`), any CVC (e.g., `123`), any zip (e.g., `89501`).
16. Click "Subscribe" / "Pay".
17. The browser redirects to `/wc/success` — Frank sees a welcome page.
18. Check Frank's email inbox — within ~1 minute he should get a welcome email from `orbiAisolutions@gmail.com` with his dashboard link and his embed code.

**If the email never arrives:**
- Check the HF Space logs for the line "WC PROVISIONED" — if missing, the webhook didn't fire (Stripe webhook misconfigured)
- Check Frank's spam folder
- The email send needs `ORBI_EMAIL_PASSWORD` set correctly on the HF Space

**If everything worked:** the embed code is in the email. Frank can paste it into any test HTML page to see his Orby running.

---

## STEP 8 — Switch from TEST to LIVE Stripe

Once Step 7 works with test cards, repeat Step 4 with LIVE Stripe keys:

1. In Stripe dashboard, top-right corner: toggle from "Test mode" to "Live mode"
2. Repeat the webhook creation in LIVE mode (URL: `https://twickell.com/api/wc/webhook`)
3. Get the LIVE webhook signing secret
4. Get the LIVE secret API key (`sk_live_...`)
5. Update the two HF Space secrets:
   - `STRIPE_SECRET_KEY` → `sk_live_...`
   - `STRIPE_WEBHOOK_SECRET` → `whsec_live_...`
6. HF Space auto-restarts. Wait 30-60 seconds.
7. **Now real money flows on real card purchases.**

---

## If something breaks

Most failures fall into a few buckets:

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| HF Space build fails with "ModuleNotFoundError" | Dependency missing | Check `requirements.txt` has `flask-cors` |
| Front page is still the old consumer page | Build didn't deploy | Check HF Space logs |
| Chat returns "I'm having a hiccup" | LLM keys missing/wrong | Check Anthropic/Groq/HF keys |
| Checkout returns "Stripe not configured" | Stripe keys missing | Check `STRIPE_SECRET_KEY` env var |
| Stripe payment goes through but no email | Webhook signing wrong, or email password wrong | Check HF logs for "WC PROVISIONED"; check `STRIPE_WEBHOOK_SECRET` and `ORBI_EMAIL_PASSWORD` |
| Customer's embed code doesn't work | API key validation issue | Check HF logs when posting to `/chat` |

When in doubt, have Frank paste the HF Space container logs to you. The logs are at:
https://huggingface.co/spaces/Artie1379/twickell?logs=container

That's where Python errors will show up.

---

## When you're done

When the deploy is live AND the end-to-end test works AND Frank's switched to live Stripe keys, the launch is essentially open. The first 50 customers get founding-member pricing locked for life.

Tell Frank to come back to the other Claude with: *"Deployment complete, ready for first customer."* The other Claude is working on Phase 2 stuff (AI Receptionist, more industry packs) in parallel.

If you get stuck on anything not covered here, the technical reference is in:
`/home/frank/twickell_deploy/DEPLOY_CHECKLIST.md`
