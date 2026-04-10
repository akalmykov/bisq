# Mainnet Offer Scraper: Research & Strategy Report

## 1. Project Goal
The objective is to implement a robust scraper for Bisq mainnet offers. The scraper should operate as a passive P2P node, discovering and retrieving all active offers and related data (e.g., trade statistics) without requiring a functional wallet or performing any trades.

## 2. Technical Findings

### P2P Network and Data Discovery
*   **DHT Storage:** Bisq uses a distributed hash table (DHT) to store and broadcast offers. All nodes in the network share this data.
*   **Offer Payload:** Offers are encapsulated in `OfferPayloadBase` (for BSQ Swaps) and `OfferPayload` (for Fiat/Altcoin) objects.
*   **OfferBookService:** This is the central component in `bisq-core` that manages the local cache of the offer book. It populates itself by listening to `P2PService` for changes in the DHT.
*   **Unfiltered Access:** While the UI and gRPC API often filter offers based on the user's configured payment methods, the underlying `OfferBookService.getOffers()` method provides access to the complete, unfiltered set of offers available on the network.

### Application Architecture
*   **Guice Dependency Injection:** The project heavily uses Guice. A scraper can be built by initializing a minimal set of modules (`P2PModule`, `OfferModule`, `BitcoinModule`, `EncryptionServiceModule`).
*   **Headless Operation:** The `BisqHeadlessApp` and `BisqExecutable` classes provide a template for running Bisq without a GUI or heavy desktop dependencies.
*   **Tor Integration:** Bisq requires Tor for P2P communication. The existing `TorSetup` handles the lifecycle of the bundled Tor binary.

## 3. Implementation Strategy

### Component: `MainnetOfferScraper`
I will implement a minimal standalone Java application that:
1.  **Initializes a transient node:** Uses a random, non-persistent identity (KeyRing).
2.  **Bootstraps Tor and P2P:** Connects to the Bisq mainnet via seed nodes.
3.  **Synchronizes DHT:** Waits for the initial `onDataReceived` signal from `P2PService`.
4.  **Extracts Data:**
    *   Iterates through `OfferBookService.getOffers()` for the current snapshot.
    *   Uses `OfferBookChangedListener` for real-time updates (newly placed or cancelled offers).
    *   Monitors `TradeStatisticsManager` for trade event data.
5.  **Data Output:** Periodically dumps the discovered offers to a JSON file for verification.

### Resource Optimization
*   **Disable Wallet Logic:** We will bypass SPV (Simple Payment Verification) wallet initialization where possible to save bandwidth and CPU, as we don't need to verify transaction proofs for simple discovery.
*   **Read-only Mode:** The node will not announce itself as a "full node" or provide services other than maintaining its own DHT cache.

## 4. Risk Assessment

| Risk | Impact | Mitigation |
| :--- | :--- | :--- |
| **Tor Connectivity** | High | Ensure system libraries are compatible with bundled Tor; fallback to system Tor if needed. |
| **Sync Time** | Medium | Implement a "warm-up" wait period (2-5 mins) before starting extraction. |
| **Protocol Updates** | Medium | Periodically update the scraper to match the latest `TRADE_PROTOCOL_VERSION`. |
| **Memory Usage** | Low | Bisq is JVM-based and DHT-intensive; allocate at least 1GB RAM. |

## 5. Execution Plan
1.  **Phase 1: Minimal Runner:** Create a test class in `core` or a new module that initializes the Guice injector with minimal modules.
2.  **Phase 2: P2P Connection:** Verify the node can successfully bootstrap Tor and connect to mainnet seed nodes.
3.  **Phase 3: Data Capture:** Implement listeners to capture and log incoming `Offer` objects.
4.  **Phase 4: Validation:** Run end-to-end tests to confirm that mainnet offers are being correctly received and parsed.
