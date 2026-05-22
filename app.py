from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from contextlib import contextmanager
import hashlib
import json
import mimetypes
import os
import re
import secrets
import sqlite3
import threading
import time


HOST = "127.0.0.1"
PORT = 5000
DB_PATH = "gateway.db"
API_KEY = "test_secret_key"
PROCESSING_DELAY_SECONDS = 2
IDEMPOTENCY_TTL_SECONDS = 24 * 60 * 60
MAX_AMOUNT = 1_000_000
RATE_LIMIT_REQUESTS = 30
RATE_LIMIT_WINDOW_SECONDS = 60

store_lock = threading.Lock()
db_lock = threading.Lock()
rate_limit_lock = threading.Lock()
in_flight_records = {}
rate_limit_buckets = {}


@dataclass
class InFlightRecord:
    request_hash: str
    created_at: float
    status_code: int | None = None
    response_body: dict | None = None
    response_headers: dict = field(default_factory=dict)
    finished: threading.Event = field(default_factory=threading.Event)


PAGE = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Idempotency Gateway</title>
    <style>
      :root {
        --bg: #f6f8fb;
        --panel: #ffffff;
        --ink: #142033;
        --muted: #68758a;
        --line: #dce3ee;
        --primary: #145da0;
        --primary-dark: #0f477b;
      }

      * { box-sizing: border-box; }

      body {
        margin: 0;
        font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background: var(--bg);
        color: var(--ink);
      }

      .shell {
        width: min(1060px, calc(100% - 32px));
        margin: 0 auto;
      }

      header {
        background: var(--panel);
        border-bottom: 1px solid var(--line);
      }

      .topbar {
        min-height: 72px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 16px;
      }

      .brand {
        display: flex;
        align-items: center;
        gap: 12px;
        font-weight: 800;
      }

      .logo {
        width: 40px;
        height: 40px;
        display: grid;
        place-items: center;
        border-radius: 8px;
        background: var(--primary);
        color: #fff;
      }

      main { padding: 30px 0; }

      .grid {
        display: grid;
        grid-template-columns: minmax(0, 1fr) 390px;
        gap: 22px;
        align-items: start;
      }

      .panel {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 8px;
        padding: 22px;
      }

      h1 {
        margin: 0;
        font-size: clamp(34px, 5vw, 56px);
        line-height: 1;
        letter-spacing: 0;
      }

      h2 {
        margin: 0 0 16px;
        font-size: 20px;
      }

      p {
        color: var(--muted);
        line-height: 1.6;
      }

      .lead {
        max-width: 700px;
        font-size: 18px;
      }

      label {
        display: block;
        margin: 14px 0 6px;
        color: #2f3b50;
        font-size: 14px;
        font-weight: 700;
      }

      input {
        width: 100%;
        height: 42px;
        border: 1px solid var(--line);
        border-radius: 6px;
        padding: 0 12px;
        font: inherit;
      }

      .actions {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 10px;
        margin-top: 18px;
      }

      button {
        height: 44px;
        border: 0;
        border-radius: 6px;
        background: var(--primary);
        color: white;
        font: inherit;
        font-weight: 800;
        cursor: pointer;
      }

      button.secondary {
        background: #eef3f8;
        color: var(--ink);
        border: 1px solid var(--line);
      }

      button:hover { background: var(--primary-dark); }
      button.secondary:hover { background: #e3eaf2; }

      pre {
        min-height: 210px;
        margin: 18px 0 0;
        padding: 16px;
        overflow: auto;
        border-radius: 8px;
        background: #101827;
        color: #d9f99d;
        font-size: 13px;
        line-height: 1.5;
      }

      .facts {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 12px;
        margin-top: 24px;
      }

      .fact {
        min-height: 100px;
        padding: 16px;
        border: 1px solid var(--line);
        border-radius: 8px;
        background: #f8fbff;
      }

      .fact strong {
        display: block;
        font-size: 24px;
      }

      .fact span {
        display: block;
        margin-top: 8px;
        color: var(--muted);
        font-size: 13px;
      }

      @media (max-width: 860px) {
        .grid,
        .facts {
          grid-template-columns: 1fr;
        }
      }
    </style>
  </head>
  <body>
    <header>
      <div class="shell topbar">
        <div class="brand">
          <div class="logo">IP</div>
          <div>IgirePay Idempotency Gateway</div>
        </div>
        <strong>Pay Once Protocol</strong>
      </div>
    </header>

    <main class="shell">
      <section class="grid">
        <div class="panel">
          <h1>Process every payment once, even when clients retry.</h1>
          <p class="lead">
            This Python API stores payment responses in SQLite, rejects unsafe key reuse,
            waits for in-flight duplicates, and returns cached responses for safe retries.
          </p>

          <div class="facts">
            <div class="fact">
              <strong>SQLite</strong>
              <span>Idempotency survives server restarts</span>
            </div>
            <div class="fact">
              <strong>API Key</strong>
              <span>Payment endpoints are protected</span>
            </div>
            <div class="fact">
              <strong>Audit</strong>
              <span>Attempts are logged for review</span>
            </div>
          </div>
        </div>

        <form class="panel" id="payment-form">
          <h2>Try the API</h2>
          <label for="key">Idempotency-Key</label>
          <input id="key" name="key" required />

          <label for="amount">Amount</label>
          <input id="amount" name="amount" type="number" min="1" step="0.01" value="100" required />

          <label for="currency">Currency</label>
          <input id="currency" name="currency" value="GHS" maxlength="3" required />

          <div class="actions">
            <button type="submit">Process</button>
            <button class="secondary" type="button" id="retry">Retry Same Key</button>
          </div>

          <pre id="output">Ready.</pre>
        </form>
      </section>
    </main>

    <script>
      const form = document.querySelector("#payment-form");
      const keyInput = document.querySelector("#key");
      const output = document.querySelector("#output");
      const retry = document.querySelector("#retry");

      function makeKey() {
        return `demo-${crypto.randomUUID()}`;
      }

      keyInput.value = makeKey();

      async function sendPayment() {
        const started = performance.now();
        const response = await fetch("/process-payment", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": keyInput.value,
            "X-API-Key": "test_secret_key"
          },
          body: JSON.stringify({
            amount: Number(document.querySelector("#amount").value),
            currency: document.querySelector("#currency").value
          })
        });

        const data = await response.json();
        const elapsed = ((performance.now() - started) / 1000).toFixed(2);
        output.textContent = JSON.stringify({
          http_status: response.status,
          x_cache_hit: response.headers.get("X-Cache-Hit") || "false",
          elapsed_seconds: elapsed,
          body: data
        }, null, 2);
      }

      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        await sendPayment();
      });

      retry.addEventListener("click", sendPayment);
    </script>
  </body>
</html>
"""


ENHANCED_PAGE = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>IgirePay Idempotency Gateway</title>
    <style>
      :root {
        --paper: #fbfaf7;
        --ink: #141821;
        --muted: #667085;
        --line: #ded8cd;
        --panel: #ffffff;
        --blue: #155e95;
        --blue-dark: #0f466f;
        --green: #167252;
        --red: #a83b2d;
        --gold: #d99b2b;
        --wash: #eef5f8;
      }

      * {
        box-sizing: border-box;
      }

      html {
        scroll-behavior: smooth;
      }

      body {
        margin: 0;
        background: var(--paper);
        color: var(--ink);
        font-family: Georgia, "Times New Roman", serif;
      }

      body::before {
        content: "";
        position: fixed;
        inset: 0;
        pointer-events: none;
        background-image:
          linear-gradient(rgba(20, 24, 33, 0.035) 1px, transparent 1px),
          linear-gradient(90deg, rgba(20, 24, 33, 0.025) 1px, transparent 1px);
        background-size: 44px 44px;
      }

      a {
        color: inherit;
        text-decoration: none;
      }

      .shell {
        width: min(1160px, calc(100% - 32px));
        margin: 0 auto;
      }

      .site-header {
        position: sticky;
        top: 0;
        z-index: 10;
        border-bottom: 1px solid var(--line);
        background: rgba(251, 250, 247, 0.94);
        backdrop-filter: blur(14px);
      }

      .topbar {
        min-height: 78px;
        display: grid;
        grid-template-columns: minmax(210px, auto) minmax(0, 1fr) auto;
        align-items: center;
        gap: 20px;
      }

      .brand {
        display: flex;
        align-items: center;
        gap: 12px;
        min-width: 220px;
      }

      .brand img {
        width: 46px;
        height: 46px;
        object-fit: contain;
        border-radius: 10px;
        background: white;
        border: 1px solid var(--line);
      }

      .brand-name {
        display: grid;
        gap: 2px;
      }

      .brand-name strong {
        font: 800 16px/1.1 ui-sans-serif, system-ui, sans-serif;
      }

      .brand-name span {
        color: var(--muted);
        font: 600 12px/1.1 ui-sans-serif, system-ui, sans-serif;
      }

      nav {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 8px;
        flex-wrap: wrap;
      }

      nav a {
        padding: 9px 11px;
        border-radius: 999px;
        color: #3f4856;
        font: 700 13px/1 ui-sans-serif, system-ui, sans-serif;
      }

      nav a:hover {
        background: #efe9df;
        color: var(--ink);
      }

      .header-action {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-height: 38px;
        padding: 0 14px;
        border-radius: 999px;
        background: var(--ink);
        color: white;
        font: 800 13px/1 ui-sans-serif, system-ui, sans-serif;
      }

      main {
        position: relative;
      }

      .hero {
        display: grid;
        grid-template-columns: minmax(0, 1fr) minmax(300px, 390px);
        gap: 28px;
        align-items: end;
        padding: 54px 0 28px;
        border-bottom: 3px double var(--line);
      }

      .eyebrow {
        margin: 0 0 14px;
        color: var(--blue);
        font: 900 12px/1 ui-sans-serif, system-ui, sans-serif;
        letter-spacing: 0.14em;
        text-transform: uppercase;
      }

      h1 {
        max-width: 850px;
        margin: 0;
        font-size: clamp(44px, 7vw, 92px);
        line-height: 0.92;
        letter-spacing: 0;
      }

      .standfirst {
        max-width: 760px;
        margin: 22px 0 0;
        color: #404a59;
        font-size: clamp(18px, 2vw, 23px);
        line-height: 1.45;
      }

      .brief {
        border-left: 4px solid var(--gold);
        padding: 18px 0 18px 18px;
      }

      .brief strong {
        display: block;
        margin-bottom: 8px;
        font: 900 13px/1 ui-sans-serif, system-ui, sans-serif;
        letter-spacing: 0.12em;
        text-transform: uppercase;
      }

      .brief p {
        margin: 0;
        color: var(--muted);
        font-size: 17px;
        line-height: 1.55;
      }

      .section {
        padding: 42px 0;
        border-bottom: 1px solid var(--line);
      }

      .section-head {
        display: grid;
        grid-template-columns: 250px minmax(0, 1fr);
        gap: 24px;
        margin-bottom: 24px;
      }

      .kicker {
        margin: 0;
        color: var(--blue);
        font: 900 12px/1 ui-sans-serif, system-ui, sans-serif;
        letter-spacing: 0.14em;
        text-transform: uppercase;
      }

      h2 {
        margin: 0;
        font-size: clamp(30px, 4vw, 54px);
        line-height: 1;
        letter-spacing: 0;
      }

      h3 {
        margin: 0 0 10px;
        font: 900 17px/1.2 ui-sans-serif, system-ui, sans-serif;
      }

      p {
        color: var(--muted);
        line-height: 1.65;
      }

      .feature-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 14px;
      }

      .feature {
        min-height: 190px;
        padding: 20px;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.74);
      }

      .feature strong {
        display: block;
        margin-bottom: 34px;
        color: var(--gold);
        font-size: 28px;
      }

      .protocol {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 10px;
      }

      .step {
        position: relative;
        min-height: 180px;
        padding: 18px;
        border-top: 4px solid var(--ink);
        background: var(--wash);
      }

      .step span {
        display: block;
        margin-bottom: 18px;
        color: var(--blue);
        font: 900 12px/1 ui-sans-serif, system-ui, sans-serif;
        letter-spacing: 0.12em;
      }

      .demo-layout {
        display: grid;
        grid-template-columns: minmax(300px, 390px) minmax(0, 1fr);
        gap: 24px;
        align-items: start;
      }

      .panel {
        border: 1px solid var(--line);
        background: var(--panel);
        padding: 22px;
      }

      label {
        display: block;
        margin: 14px 0 6px;
        color: #2f3b50;
        font: 800 14px/1 ui-sans-serif, system-ui, sans-serif;
      }

      input {
        width: 100%;
        height: 44px;
        border: 1px solid var(--line);
        border-radius: 4px;
        padding: 0 12px;
        color: var(--ink);
        font: 15px/1 ui-sans-serif, system-ui, sans-serif;
      }

      .actions {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 10px;
        margin-top: 18px;
      }

      button {
        height: 44px;
        border: 0;
        border-radius: 4px;
        background: var(--blue);
        color: white;
        cursor: pointer;
        font: 900 14px/1 ui-sans-serif, system-ui, sans-serif;
      }

      button:hover {
        background: var(--blue-dark);
      }

      button.secondary {
        background: #f0ebe2;
        color: var(--ink);
        border: 1px solid var(--line);
      }

      button.secondary:hover {
        background: #e6dece;
      }

      pre {
        width: 100%;
        max-width: 100%;
        min-height: 360px;
        margin: 0;
        padding: 18px;
        overflow: auto;
        border: 1px solid #0e1728;
        background: #101827;
        color: #d9f99d;
        font: 13px/1.55 "Cascadia Code", Consolas, monospace;
      }

      .docs-grid {
        display: grid;
        grid-template-columns: 1.1fr 0.9fr;
        gap: 22px;
      }

      code {
        border: 1px solid var(--line);
        background: #f3eee4;
        padding: 2px 5px;
        border-radius: 4px;
        font-family: "Cascadia Code", Consolas, monospace;
        font-size: 0.92em;
      }

      .faq {
        display: grid;
        gap: 12px;
      }

      details {
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.76);
        padding: 16px 18px;
      }

      summary {
        cursor: pointer;
        font: 900 16px/1.3 ui-sans-serif, system-ui, sans-serif;
      }

      .contact-band {
        display: grid;
        grid-template-columns: 1fr 1fr 1fr;
        gap: 14px;
      }

      .contact-card {
        min-height: 150px;
        padding: 18px;
        border: 1px solid var(--line);
        background: var(--ink);
        color: white;
      }

      .contact-card p {
        color: #d5dbe6;
      }

      footer {
        padding: 28px 0;
        color: #596579;
        font: 700 13px/1.5 ui-sans-serif, system-ui, sans-serif;
      }

      .footer-inner {
        display: flex;
        justify-content: space-between;
        gap: 16px;
        flex-wrap: wrap;
      }

      img,
      svg {
        max-width: 100%;
      }

      @media (max-width: 900px) {
        .hero,
        .section-head,
        .demo-layout,
        .docs-grid {
          grid-template-columns: 1fr;
        }

        .topbar {
          grid-template-columns: 1fr;
          align-items: stretch;
          padding: 16px 0;
          gap: 14px;
        }

        nav {
          justify-content: flex-start;
          overflow-x: auto;
          padding-bottom: 2px;
          scrollbar-width: thin;
        }

        nav a {
          white-space: nowrap;
        }

        .header-action {
          width: fit-content;
        }

        .feature-grid,
        .protocol,
        .contact-band {
          grid-template-columns: 1fr;
        }
      }

      @media (max-width: 640px) {
        .shell {
          width: min(100% - 24px, 1160px);
        }

        .brand img {
          width: 40px;
          height: 40px;
        }

        .brand-name strong {
          font-size: 15px;
        }

        nav {
          display: grid;
          grid-template-columns: repeat(3, minmax(0, 1fr));
          gap: 8px;
          overflow: visible;
        }

        nav a {
          display: flex;
          align-items: center;
          justify-content: center;
          min-height: 38px;
          padding: 8px 6px;
          border: 1px solid var(--line);
          background: rgba(255, 255, 255, 0.7);
          font-size: 12px;
          text-align: center;
        }

        .header-action {
          width: 100%;
        }

        .hero {
          padding: 34px 0 24px;
        }

        h1 {
          font-size: clamp(40px, 13vw, 58px);
          line-height: 0.98;
        }

        h2 {
          font-size: clamp(28px, 9vw, 40px);
          line-height: 1.04;
        }

        .standfirst {
          font-size: 17px;
        }

        .brief,
        .panel,
        .feature,
        .step,
        .contact-card {
          padding: 16px;
        }

        .section {
          padding: 32px 0;
        }

        .actions {
          grid-template-columns: 1fr;
        }

        button,
        input {
          min-height: 46px;
        }

        pre {
          min-height: 280px;
          font-size: 12px;
          white-space: pre-wrap;
          overflow-wrap: anywhere;
        }

        .footer-inner {
          display: grid;
          gap: 8px;
        }
      }

      @media (max-width: 380px) {
        nav {
          grid-template-columns: repeat(2, minmax(0, 1fr));
        }

        h1 {
          font-size: 38px;
        }
      }
    </style>
  </head>
  <body>
    <header class="site-header">
      <div class="shell topbar">
        <a class="brand" href="#home" aria-label="IgirePay home">
          <img src="/img/logo-igire.png" alt="IgirePay logo" />
          <span class="brand-name">
            <strong>IgirePay Gateway</strong>
            <span>Pay-Once Protocol</span>
          </span>
        </a>
        <nav aria-label="Primary navigation">
          <a href="#services">Services</a>
          <a href="#protocol">Protocol</a>
          <a href="#try">Try It</a>
          <a href="#help">Help</a>
          <a href="#contact">Contact</a>
        </nav>
        <a class="header-action" href="#try">Run Demo</a>
      </div>
    </header>

    <main id="home">
      <section class="shell hero">
        <div>
          <p class="eyebrow">Backend reliability for modern payments</p>
          <h1>One payment request. One charge. Every retry handled.</h1>
          <p class="standfirst">
            IgirePay's idempotency layer protects checkout flows from network timeouts,
            duplicate retries, and race conditions while preserving the exact response
            clients need to recover safely.
          </p>
        </div>
        <aside class="brief">
          <strong>Service brief</strong>
          <p>
            The API stores completed payment responses in SQLite, blocks unsafe key reuse,
            waits for in-flight duplicates, and records audit events for operational review.
          </p>
        </aside>
      </section>

      <section class="shell section" id="services">
        <div class="section-head">
          <p class="kicker">Services</p>
          <h2>Built for the uncomfortable moments after a timeout.</h2>
        </div>
        <div class="feature-grid">
          <article class="feature">
            <strong>01</strong>
            <h3>Idempotent payment processing</h3>
            <p>Clients send an `Idempotency-Key`; the gateway stores the first successful response and replays it for safe retries.</p>
          </article>
          <article class="feature">
            <strong>02</strong>
            <h3>Persistent transaction memory</h3>
            <p>SQLite keeps payment records and idempotency responses available after server restarts during demos and local review.</p>
          </article>
          <article class="feature">
            <strong>03</strong>
            <h3>Operational visibility</h3>
            <p>Audit logs, payment status lookup, rate limiting, and structured errors help support teams understand what happened.</p>
          </article>
        </div>
      </section>

      <section class="shell section" id="protocol">
        <div class="section-head">
          <p class="kicker">Protocol</p>
          <h2>The request path is strict, predictable, and replay-safe.</h2>
        </div>
        <div class="protocol">
          <article class="step">
            <span>Step 1</span>
            <h3>Validate access</h3>
            <p>The gateway checks `X-API-Key`, the payment body, and the format of the idempotency key.</p>
          </article>
          <article class="step">
            <span>Step 2</span>
            <h3>Hash request</h3>
            <p>The normalized JSON body is hashed so the gateway can detect when the same key is reused for a different payment.</p>
          </article>
          <article class="step">
            <span>Step 3</span>
            <h3>Process once</h3>
            <p>The first request simulates payment processing, stores the transaction, and saves the exact response.</p>
          </article>
          <article class="step">
            <span>Step 4</span>
            <h3>Replay safely</h3>
            <p>Duplicates return the saved response with `X-Cache-Hit: true`; in-flight duplicates wait for the original result.</p>
          </article>
        </div>
      </section>

      <section class="shell section" id="try">
        <div class="section-head">
          <p class="kicker">Try it</p>
          <h2>Submit a payment, then retry the same request.</h2>
        </div>
        <div class="demo-layout">
          <form class="panel" id="payment-form">
            <h3>Payment request</h3>
            <label for="key">Idempotency-Key</label>
            <input id="key" name="key" required />

            <label for="amount">Amount</label>
            <input id="amount" name="amount" type="number" min="1" step="0.01" value="100" required />

            <label for="currency">Currency</label>
            <input id="currency" name="currency" value="GHS" maxlength="3" required />

            <div class="actions">
              <button type="submit">Process</button>
              <button class="secondary" type="button" id="retry">Retry Same Key</button>
            </div>
          </form>
          <pre id="output">Ready. Submit once, then retry with the same key to see X-Cache-Hit become true.</pre>
        </div>
      </section>

      <section class="shell section" id="help">
        <div class="section-head">
          <p class="kicker">Help and FAQ</p>
          <h2>Developer notes for testing the gateway.</h2>
        </div>
        <div class="docs-grid">
          <div class="panel">
            <h3>Core endpoints</h3>
            <p><code>POST /process-payment</code> processes a payment with `X-API-Key` and `Idempotency-Key` headers.</p>
            <p><code>GET /payments/&lt;transaction_id&gt;</code> returns one payment record.</p>
            <p><code>GET /transactions</code> returns recent charges. <code>GET /audit-logs</code> returns operational events.</p>
            <p><code>GET /health</code> confirms the service is running and does not require authentication.</p>
          </div>
          <div class="faq">
            <details open>
              <summary>What should happen on a duplicate retry?</summary>
              <p>The gateway returns the exact saved response and adds `X-Cache-Hit: true` without processing a second charge.</p>
            </details>
            <details>
              <summary>What if the same key is used with another amount?</summary>
              <p>The gateway returns a `422` response with `IDEMPOTENCY_CONFLICT` to protect data integrity.</p>
            </details>
            <details>
              <summary>How are simultaneous requests handled?</summary>
              <p>The second identical request waits for the first one to finish, then receives the same final result.</p>
            </details>
          </div>
        </div>
      </section>

      <section class="shell section" id="contact">
        <div class="section-head">
          <p class="kicker">Contact</p>
          <h2>Support channels for operators and integrators.</h2>
        </div>
        <div class="contact-band">
          <article class="contact-card">
            <h3>Integration desk</h3>
            <p>integration@igirepay.example</p>
            <p>API keys, onboarding, and test credentials.</p>
          </article>
          <article class="contact-card">
            <h3>Risk operations</h3>
            <p>risk@igirepay.example</p>
            <p>Duplicate-charge reports, audit reviews, and dispute support.</p>
          </article>
          <article class="contact-card">
            <h3>Service status</h3>
            <p>status.igirepay.example</p>
            <p>Health checks, incident notes, and maintenance windows.</p>
          </article>
        </div>
      </section>
    </main>

    <footer>
      <div class="shell footer-inner">
        <span>IgirePay Technologies Ltd. Idempotency Gateway</span>
        <span>Demo API key: test_secret_key</span>
        <span>Built with Python standard library and SQLite</span>
      </div>
    </footer>

    <script>
      const form = document.querySelector("#payment-form");
      const keyInput = document.querySelector("#key");
      const output = document.querySelector("#output");
      const retry = document.querySelector("#retry");

      function makeKey() {
        return `demo-${crypto.randomUUID()}`;
      }

      keyInput.value = makeKey();

      async function sendPayment() {
        const started = performance.now();
        output.textContent = "Processing...";
        const response = await fetch("/process-payment", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": keyInput.value,
            "X-API-Key": "test_secret_key"
          },
          body: JSON.stringify({
            amount: Number(document.querySelector("#amount").value),
            currency: document.querySelector("#currency").value
          })
        });

        const data = await response.json();
        const elapsed = ((performance.now() - started) / 1000).toFixed(2);
        output.textContent = JSON.stringify({
          http_status: response.status,
          x_cache_hit: response.headers.get("X-Cache-Hit") || "false",
          elapsed_seconds: elapsed,
          body: data
        }, null, 2);
      }

      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        await sendPayment();
      });

      retry.addEventListener("click", sendPayment);
    </script>
  </body>
</html>
"""


def now_epoch():
    return time.time()


def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def format_amount(amount):
    if amount == int(amount):
        return str(int(amount))
    return f"{amount:.2f}".rstrip("0").rstrip(".")


def canonical_request_hash(payload):
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def error_body(code, message):
    return {"error": {"code": code, "message": message}}


def send_json(handler, status_code, body, headers=None):
    headers = headers or {}
    encoded = json.dumps(body, sort_keys=True).encode("utf-8")
    handler.send_response(status_code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(encoded)))
    for name, value in headers.items():
        handler.send_header(name, str(value))
    handler.end_headers()
    handler.wfile.write(encoded)


def send_error_json(handler, status_code, code, message, headers=None):
    send_json(handler, status_code, error_body(code, message), headers)


@contextmanager
def get_connection():
    connection = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def init_db():
    with db_lock:
        with get_connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS idempotency_records (
                    key TEXT PRIMARY KEY,
                    request_hash TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    response_body TEXT NOT NULL,
                    response_headers TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id TEXT PRIMARY KEY,
                    amount REAL NOT NULL,
                    currency TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    idempotency_key TEXT,
                    request_hash TEXT,
                    status_code INTEGER,
                    client_ip TEXT,
                    created_at TEXT NOT NULL,
                    details TEXT NOT NULL
                );
                """
            )


def reset_state_for_tests():
    with store_lock:
        in_flight_records.clear()
    with rate_limit_lock:
        rate_limit_buckets.clear()
    with db_lock:
        with get_connection() as connection:
            connection.execute("DELETE FROM idempotency_records")
            connection.execute("DELETE FROM transactions")
            connection.execute("DELETE FROM audit_logs")


def clean_expired_records():
    with db_lock:
        with get_connection() as connection:
            connection.execute("DELETE FROM idempotency_records WHERE expires_at <= ?", (now_epoch(),))


def save_audit(event_type, idempotency_key=None, request_hash=None, status_code=None, client_ip=None, details=None):
    with db_lock:
        with get_connection() as connection:
            connection.execute(
                """
                INSERT INTO audit_logs (
                    event_type, idempotency_key, request_hash, status_code, client_ip, created_at, details
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_type,
                    idempotency_key,
                    request_hash,
                    status_code,
                    client_ip,
                    now_utc(),
                    json.dumps(details or {}, sort_keys=True),
                ),
            )


def get_recent_audit_logs(limit=25):
    with db_lock:
        with get_connection() as connection:
            rows = connection.execute(
                """
                SELECT event_type, idempotency_key, status_code, client_ip, created_at, details
                FROM audit_logs
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    return [
        {
            "event_type": row["event_type"],
            "idempotency_key": row["idempotency_key"],
            "status_code": row["status_code"],
            "client_ip": row["client_ip"],
            "created_at": row["created_at"],
            "details": json.loads(row["details"]),
        }
        for row in rows
    ]


def save_completed_record(idempotency_key, request_hash, status_code, response_body, response_headers):
    expires_at = now_epoch() + IDEMPOTENCY_TTL_SECONDS
    with db_lock:
        with get_connection() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO idempotency_records (
                    key, request_hash, status_code, response_body, response_headers, created_at, expires_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    idempotency_key,
                    request_hash,
                    status_code,
                    json.dumps(response_body, sort_keys=True),
                    json.dumps(response_headers, sort_keys=True),
                    now_epoch(),
                    expires_at,
                ),
            )


def get_completed_record(idempotency_key):
    with db_lock:
        with get_connection() as connection:
            row = connection.execute(
                """
                SELECT request_hash, status_code, response_body, response_headers, expires_at
                FROM idempotency_records
                WHERE key = ?
                """,
                (idempotency_key,),
            ).fetchone()

    if row is None:
        return None

    if row["expires_at"] <= now_epoch():
        clean_expired_records()
        return None

    return {
        "request_hash": row["request_hash"],
        "status_code": row["status_code"],
        "response_body": json.loads(row["response_body"]),
        "response_headers": json.loads(row["response_headers"]),
    }


def save_transaction(transaction):
    with db_lock:
        with get_connection() as connection:
            connection.execute(
                """
                INSERT INTO transactions (transaction_id, amount, currency, status, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    transaction["transaction_id"],
                    transaction["amount"],
                    transaction["currency"],
                    transaction["status"],
                    transaction["created_at"],
                ),
            )


def get_transaction(transaction_id):
    with db_lock:
        with get_connection() as connection:
            row = connection.execute(
                """
                SELECT transaction_id, amount, currency, status, created_at
                FROM transactions
                WHERE transaction_id = ?
                """,
                (transaction_id,),
            ).fetchone()

    if row is None:
        return None

    return dict(row)


def get_recent_transactions(limit=20):
    with db_lock:
        with get_connection() as connection:
            rows = connection.execute(
                """
                SELECT transaction_id, amount, currency, status, created_at
                FROM transactions
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    return [dict(row) for row in rows]


def read_json_body(handler):
    length = int(handler.headers.get("Content-Length", "0"))
    raw_body = handler.rfile.read(length)
    if not raw_body:
        raise ValueError("Request body must be valid JSON.")

    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError("Request body must be valid JSON.") from exc

    if not isinstance(payload, dict):
        raise ValueError("Request body must be a JSON object.")

    return payload


def validate_api_key(handler):
    return handler.headers.get("X-API-Key", "") == API_KEY


def validate_idempotency_key(key):
    if not key:
        return "MISSING_IDEMPOTENCY_KEY", "Missing required Idempotency-Key header."

    if len(key) > 128:
        return "INVALID_IDEMPOTENCY_KEY", "Idempotency-Key must be 128 characters or fewer."

    if not re.fullmatch(r"[A-Za-z0-9._:-]+", key):
        return (
            "INVALID_IDEMPOTENCY_KEY",
            "Idempotency-Key may only contain letters, numbers, dots, underscores, colons, and hyphens.",
        )

    return None, None


def validate_payment(payload):
    amount = payload.get("amount")
    currency = payload.get("currency")

    if not isinstance(amount, (int, float)) or isinstance(amount, bool):
        return None, None, "INVALID_AMOUNT", "amount must be a positive number."

    if amount <= 0:
        return None, None, "INVALID_AMOUNT", "amount must be greater than zero."

    if amount > MAX_AMOUNT:
        return None, None, "INVALID_AMOUNT", f"amount must not exceed {MAX_AMOUNT}."

    if not isinstance(currency, str) or not re.fullmatch(r"[A-Z]{3}", currency):
        return None, None, "INVALID_CURRENCY", "currency must be a 3-letter uppercase code, for example GHS."

    return round(float(amount), 2), currency, None, None


def check_rate_limit(client_ip):
    now = now_epoch()
    with rate_limit_lock:
        attempts = [
            timestamp
            for timestamp in rate_limit_buckets.get(client_ip, [])
            if now - timestamp < RATE_LIMIT_WINDOW_SECONDS
        ]
        if len(attempts) >= RATE_LIMIT_REQUESTS:
            rate_limit_buckets[client_ip] = attempts
            return False
        attempts.append(now)
        rate_limit_buckets[client_ip] = attempts
        return True


def process_payment(payload):
    time.sleep(PROCESSING_DELAY_SECONDS)

    amount, currency, _, _ = validate_payment(payload)
    transaction = {
        "transaction_id": f"txn_{secrets.token_hex(8)}",
        "amount": amount,
        "currency": currency,
        "status": "charged",
        "created_at": now_utc(),
    }
    save_transaction(transaction)

    return 201, {
        "message": f"Charged {format_amount(amount)} {currency}",
        "transaction": transaction,
    }


class IdempotencyGatewayHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path

        if path == "/":
            body = ENHANCED_PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path.startswith("/img/"):
            safe_name = os.path.basename(path)
            image_path = os.path.join("img", safe_name)
            if not os.path.exists(image_path):
                send_error_json(self, 404, "NOT_FOUND", "Image not found.")
                return
            with open(image_path, "rb") as image_file:
                body = image_file.read()
            content_type = mimetypes.guess_type(image_path)[0] or "application/octet-stream"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/health":
            send_json(self, 200, {"status": "ok", "service": "idempotency-gateway"})
            return

        if not validate_api_key(self):
            send_error_json(self, 401, "UNAUTHORIZED", "Missing or invalid X-API-Key header.")
            return

        if path == "/transactions":
            send_json(self, 200, {"transactions": get_recent_transactions()})
            return

        if path == "/audit-logs":
            send_json(self, 200, {"audit_logs": get_recent_audit_logs()})
            return

        payment_match = re.fullmatch(r"/payments/([A-Za-z0-9_:-]+)", path)
        if payment_match:
            transaction = get_transaction(payment_match.group(1))
            if transaction is None:
                send_error_json(self, 404, "PAYMENT_NOT_FOUND", "Payment transaction was not found.")
                return
            send_json(self, 200, {"transaction": transaction})
            return

        send_error_json(self, 404, "NOT_FOUND", "Endpoint not found.")

    def do_POST(self):
        path = urlparse(self.path).path
        client_ip = self.client_address[0]

        if path != "/process-payment":
            send_error_json(self, 404, "NOT_FOUND", "Endpoint not found.")
            return

        if not validate_api_key(self):
            save_audit("unauthorized", client_ip=client_ip, status_code=401)
            send_error_json(self, 401, "UNAUTHORIZED", "Missing or invalid X-API-Key header.")
            return

        if not check_rate_limit(client_ip):
            save_audit("rate_limited", client_ip=client_ip, status_code=429)
            send_error_json(
                self,
                429,
                "RATE_LIMITED",
                "Too many requests. Please retry later.",
                {"Retry-After": str(RATE_LIMIT_WINDOW_SECONDS)},
            )
            return

        idempotency_key = self.headers.get("Idempotency-Key", "").strip()
        key_code, key_message = validate_idempotency_key(idempotency_key)
        if key_message:
            save_audit("validation_failed", idempotency_key=idempotency_key, client_ip=client_ip, status_code=400)
            send_error_json(self, 400, key_code, key_message)
            return

        try:
            payload = read_json_body(self)
        except ValueError as exc:
            save_audit("validation_failed", idempotency_key=idempotency_key, client_ip=client_ip, status_code=400)
            send_error_json(self, 400, "INVALID_JSON", str(exc))
            return

        amount, currency, payment_code, payment_message = validate_payment(payload)
        if payment_message:
            save_audit("validation_failed", idempotency_key=idempotency_key, client_ip=client_ip, status_code=400)
            send_error_json(self, 400, payment_code, payment_message)
            return

        normalized_payload = {"amount": amount, "currency": currency}
        request_hash = canonical_request_hash(normalized_payload)

        clean_expired_records()
        completed_record = get_completed_record(idempotency_key)
        if completed_record is not None:
            if completed_record["request_hash"] != request_hash:
                save_audit(
                    "idempotency_conflict",
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    status_code=422,
                    client_ip=client_ip,
                )
                send_error_json(
                    self,
                    422,
                    "IDEMPOTENCY_CONFLICT",
                    "Idempotency key already used for a different request body.",
                )
                return

            save_audit(
                "cache_hit",
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                status_code=completed_record["status_code"],
                client_ip=client_ip,
            )
            send_json(
                self,
                completed_record["status_code"],
                completed_record["response_body"],
                {**completed_record["response_headers"], "X-Cache-Hit": "true"},
            )
            return

        with store_lock:
            in_flight = in_flight_records.get(idempotency_key)
            if in_flight is None:
                in_flight = InFlightRecord(request_hash=request_hash, created_at=now_epoch())
                in_flight_records[idempotency_key] = in_flight
                should_process = True
            elif in_flight.request_hash != request_hash:
                save_audit(
                    "idempotency_conflict",
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    status_code=422,
                    client_ip=client_ip,
                )
                send_error_json(
                    self,
                    422,
                    "IDEMPOTENCY_CONFLICT",
                    "Idempotency key already used for a different request body.",
                )
                return
            else:
                should_process = False
                wait_for = in_flight.finished

        if not should_process:
            wait_for.wait()
            save_audit(
                "in_flight_replay",
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                status_code=in_flight.status_code,
                client_ip=client_ip,
            )
            send_json(
                self,
                in_flight.status_code,
                in_flight.response_body,
                {**in_flight.response_headers, "X-Cache-Hit": "true"},
            )
            return

        save_audit(
            "processing_started",
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            client_ip=client_ip,
        )
        status_code, response_body = process_payment(normalized_payload)
        response_headers = {
            "X-Cache-Hit": "false",
            "X-Idempotency-TTL-Seconds": str(IDEMPOTENCY_TTL_SECONDS),
        }

        save_completed_record(idempotency_key, request_hash, status_code, response_body, response_headers)
        save_audit(
            "payment_processed",
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            status_code=status_code,
            client_ip=client_ip,
            details={"transaction_id": response_body["transaction"]["transaction_id"]},
        )

        with store_lock:
            in_flight.status_code = status_code
            in_flight.response_body = response_body
            in_flight.response_headers = response_headers
            in_flight.finished.set()
            in_flight_records.pop(idempotency_key, None)

        send_json(self, status_code, response_body, response_headers)

    def log_message(self, format, *args):
        print(f"[idempotency-gateway] {self.address_string()} - {format % args}")


def main():
    init_db()
    server = ThreadingHTTPServer((HOST, PORT), IdempotencyGatewayHandler)
    print(f"Idempotency Gateway running at http://{HOST}:{PORT}")
    print(f"Use X-API-Key: {API_KEY}")
    print("Press Ctrl+C to stop the server.")
    server.serve_forever()


init_db()

if __name__ == "__main__":
    main()
