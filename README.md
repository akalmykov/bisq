# Bisq Mainnet Offer Scraper

Collects live offer snapshots and historical trade statistics from the Bisq P2P network.

## Prerequisites

- Java 21 (OpenJDK)
- Gradle (uses the project's Gradle wrapper)
- Tor with control port enabled and cookie authentication

## Tor Setup

The scraper connects to a running Tor instance via its control port.

### Install and configure Tor (Debian/Ubuntu)

```bash
sudo apt install tor
```

Add to `/etc/tor/torrc`:

```
ControlPort 9051
CookieAuthentication 1
```

Then restart Tor:

```bash
sudo systemctl restart tor@default
```

Verify Tor is running and bootstrapped:

```bash
sudo systemctl status tor@default
curl --socks5-hostname localhost:9050 https://check.torproject.org/api/ip
```

## Running the Scraper

The Tor control auth cookie is stored at `/run/tor/control.authcookie` with restrictive
permissions and gets recreated on each Tor restart. Copy it to a readable location first:

```bash
cp /run/tor/control.authcookie /tmp/tor_control_auth_cookie
```

Then run:

```bash
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./gradlew :core:runScraper \
  -Pargs="--baseCurrencyNetwork=BTC_MAINNET \
          --torControlPort=9051 \
          --torControlCookieFile=/tmp/tor_control_auth_cookie \
          --torControlUseSafeCookieAuth \
          --userDataDir=scraper_data_mainnet \
          --appName=bisq_scraper"
```

### Run persistently in the background (tmux)

The scraper must keep running after you disconnect from SSH. Use `tmux`:

```bash
# Create a new tmux session
tmux new -s bisq-scraper

# Inside tmux, start the scraper (auto-restarts and re-copies cookie on each loop)
while true; do
    cp /run/tor/control.authcookie /tmp/tor_control_auth_cookie
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./gradlew :core:runScraper \
      -Pargs="--baseCurrencyNetwork=BTC_MAINNET \
              --torControlPort=9051 \
              --torControlCookieFile=/tmp/tor_control_auth_cookie \
              --torControlUseSafeCookieAuth \
              --userDataDir=scraper_data_mainnet \
              --appName=bisq_scraper"
    echo "Scraper exited. Restarting in 10s..."
    sleep 10
done
```

Detach from tmux (scraper keeps running): press `Ctrl+B`, then `D`.

Re-attach later to check on it:

```bash
tmux attach -t bisq-scraper
```

The `while true` loop handles two things automatically:
- **Tor restarts** — re-copies `/run/tor/control.authcookie` before each run
- **Scraper crashes** — restarts the scraper after a 10-second pause

## Data Output

All scraped data is stored under `core/scraper_data_mainnet/bisq_scraper/btc_mainnet/db/`
(relative to the repo root).

### Directory structure

```
db/
├── offers/
│   ├── offers_2026-04-10T15-30-00.json   # timestamped snapshot (never modified after creation)
│   ├── offers_2026-04-10T15-31-00.json   # next minute's snapshot (never modified after creation)
│   ├── ...
│   └── offers_latest.json                # overwritten each cycle (convenience copy)
│
└── trade_stats/
    ├── history.jsonl                      # append-only, never rewritten, never deleted
    └── latest.json                        # overwritten only when new trade stats arrive
```

### Data integrity guarantees

| File | Written once or overwritten? | Deleted by cleanup? |
|------|---------------------------|---------------------|
| `offers/offers_<timestamp>.json` | **Written once, never modified** | Yes, after 30 days |
| `offers/offers_latest.json` | Overwritten every dump cycle | No |
| `trade_stats/history.jsonl` | **Append-only, never modified** | **Never** (crash recovery truncates corrupt tail) |
| `trade_stats/latest.json` | Overwritten when new stats arrive | No |

### Dump schedule

1. **First dump** — immediately after P2P network sync completes (all initial data received from seed nodes and peers)
2. **Fallback** — if P2P sync doesn't complete within 5 minutes, dumping starts anyway with whatever data is available
3. **Periodic dumps** — every 1 minute thereafter, as long as the scraper is running

Each dump cycle handles both offers and trade statistics.

### Trade statistics (`trade_stats/history.jsonl`)

**This is the primary data store. It is append-only and never rewritten.** If you stop and
restart the scraper, it resumes where it left off — no data is lost or duplicated.

JSON Lines format. Each line is one trade statistic, prefixed with its Base64-encoded
SHA256+RIPEMD160 hash for deduplication:

```
fcNBAUrJtkK4gCBADsnfRe/RjSU=	{"currency":"ETH","price":2083767,"amount":4170000,"paymentMethod":"28","date":1461086940091}
```

**How deduplication works:**
- On startup, the scraper reads the Base64 hash prefix from every line in `history.jsonl`
  into an in-memory set. This is fast (string comparison, no JSON parsing).
- **Crash recovery:** before reading, the scraper checks the last line of `history.jsonl`.
  If it is incomplete (no tab or no closing `}`), it is truncated. This handles the case
  where the process was killed mid-write — the interrupted trade stat is re-appended on
  the next dump, so no data is lost and no duplicates are created.
- On each dump cycle, each trade stat from the P2P network is checked against this set.
  If the hash is new, the line is appended to `history.jsonl`.
- Already-seen trade stats are silently skipped — they are **never rewritten**.
- If a dump cycle finds zero new trade stats, `history.jsonl` is not touched at all.

**Data sources loaded on each startup:**
1. **Historical resource files** bundled with Bisq (versions 1.5.2 through 1.9.20) —
   these contain the full historical record of Bisq trades since the network launched
2. **Live P2P network data** — new trades received from peers in real-time

On the very first run (no `history.jsonl` exists), the full historical dataset
(~393K trades at time of writing) is written out. On all subsequent runs (including
after restarts), only trades not yet in `history.jsonl` are appended.

Each trade stat contains:
- `currency` — currency code (e.g. "USD", "EUR", "ETH")
- `price` — trade price in the currency's smallest unit
- `amount` — BTC amount in satoshis
- `paymentMethod` — payment method enum ordinal (see Bisq source for mapping)
- `date` — trade timestamp in milliseconds since epoch

### Offer snapshots (`offers/`)

Each file is a JSON array of all currently active offer payloads on the Bisq network.

- A new timestamped file is created each dump cycle (every minute).
- Each timestamped file is **written once and never modified or appended to**.
- `offers_latest.json` is a convenience copy that is overwritten each cycle with the
  latest snapshot.
- Timestamped snapshots older than 30 days are automatically deleted once per hour
  (the `offers_latest.json` is never deleted).

### Known limitations

- **Offers during downtime are lost.** The scraper can only capture offers that exist
  while it is running. If the scraper is stopped, any offers created and removed during
  that window are never recorded. Keep the scraper running continuously via tmux.
- **Trade stats survive restarts.** Trade statistics are cumulative — when the scraper
  restarts, it re-syncs the full set from Bisq's bundled historical data files and from
  peers. Only trades that are genuinely new (not yet in `history.jsonl`) are appended.
  In practice, no trade statistics are lost across restarts.
- **No disk-level durability (fsync).** Data is flushed to the OS page cache on each dump
  cycle but not synced to disk via `fsync`. A sudden power loss could lose the last few
  seconds of appends. This is an acceptable trade-off for scraping (the data can be
  recovered from peers on next sync).

## Build

```bash
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./gradlew :core:compileJava
```
