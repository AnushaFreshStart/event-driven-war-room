# How to Obtain API Keys

To unleash the full AI capabilities of the Event-Driven War Room, you need to provide real API keys in your `.env` file. Follow the step-by-step instructions below to get your keys for OpenRouter, Exa, and Auth0.

---

## 1. OpenRouter (The Diagnoser Agent)
OpenRouter provides unified access to top LLMs (like Claude 3.5 Sonnet, which the Diagnoser uses).

1. Go to [openrouter.ai](https://openrouter.ai/) and sign up or log in.
2. Click your profile icon in the top right corner and select **Keys** (or navigate to `openrouter.ai/keys`).
3. Click the **Create Key** button.
4. Give it a name (e.g., "War Room Agent").
5. Copy the generated key (it usually starts with `sk-or-v1-...`) and paste it into your `.env` file as `OPENROUTER_API_KEY`.

---

## 2. Exa (The Scout Agent)
Exa is an AI-native search engine designed for agents to find code, documentation, and web context.

1. Go to [exa.ai](https://exa.ai/) and sign up or log in.
2. Navigate to the **API Dashboard** (usually at `dashboard.exa.ai/api-keys`).
3. You will see a default key already generated, or you can click **Create API Key**.
4. Copy the key and paste it into your `.env` file as `EXA_API_KEY`.

---

## 3. Auth0 (The Executive Agent)
The Executive Agent uses Auth0's Machine-to-Machine (M2M) flow to securely fetch an access token before executing dangerous commands (like server rollbacks).

### Step A: Find Your Domain
1. Log in to your Auth0 Dashboard at [manage.auth0.com](https://manage.auth0.com/).
2. Look at the top-left corner under your tenant name, or go to **Settings > General**.
3. Locate your **Tenant Domain** (it looks like `dev-abcdefg.us.auth0.com`).
4. Paste it into your `.env` file as `AUTH0_DOMAIN` *(Note: Do not include `https://`)*.

### Step B: Create a Machine-to-Machine App
1. On the left sidebar, click **Applications**, then click **Applications** again.
2. Click the **Create Application** button in the top right.
3. Name it "Executive Agent" and select **Machine to Machine Applications**. Click Create.
4. A dropdown will ask which API you want to authorize. Select the **Auth0 Management API** (or any custom API you have set up).
5. Click **Authorize**.

### Step C: Get Client ID, Secret, and Audience
1. You will be dropped into the **Settings** tab of your new M2M Application.
2. Copy the **Client ID** and paste it into your `.env` file as `AUTH0_CLIENT_ID`.
3. Copy the **Client Secret** and paste it into your `.env` file as `AUTH0_CLIENT_SECRET`.
4. To get the Audience, click the **APIs** tab on that same application page. Next to the Auth0 Management API you authorized, you'll see the **API Audience** (it usually looks like `https://dev-abcdefg.us.auth0.com/api/v2/`).
5. Copy that URL exactly and paste it into your `.env` file as `AUTH0_AUDIENCE`.

---

### Final Verification
Once you have pasted all these values into your `.env` file, simply restart your agents. The scripts will automatically detect the presence of real keys and switch from "Mock" mode to "Live" mode!
