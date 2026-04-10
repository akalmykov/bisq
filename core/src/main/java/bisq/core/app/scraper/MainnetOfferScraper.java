package bisq.core.app.scraper;

import bisq.core.app.misc.ExecutableForAppWithP2p;
import bisq.core.offer.Offer;
import bisq.core.offer.OfferBookService;
import bisq.core.trade.statistics.TradeStatistics3;
import bisq.core.trade.statistics.TradeStatisticsManager;

import bisq.common.UserThread;
import bisq.common.app.Version;
import bisq.core.util.JsonUtil;

import bisq.network.p2p.P2PServiceListener;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MainnetOfferScraper extends ExecutableForAppWithP2p {

    private static final DateTimeFormatter TIMESTAMP_FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH-mm-ss")
            .withZone(ZoneOffset.UTC);

    private static final int OFFER_SNAPSHOT_RETENTION_DAYS = 30;

    public static void main(String[] args) {
        if (args.length == 0) {
            args = new String[]{"--baseCurrencyNetwork=BTC_MAINNET"};
        }
        new MainnetOfferScraper().execute(args);
    }

    private OfferBookService offerBookService;
    private TradeStatisticsManager tradeStatisticsManager;
    private boolean isSynced = false;
    private boolean dumpTaskStarted = false;

    // Track which trade stats we've already written to history.jsonl.
    // Key is Base64-encoded hash; value is irrelevant.
    private final Set<String> seenTradeStatKeys = new HashSet<>();

    public MainnetOfferScraper() {
        super("Bisq Mainnet Scraper", "bisq-scraper", "bisq_scraper", Version.VERSION);
    }

    @Override
    protected void applyInjector() {
        super.applyInjector();
        offerBookService = injector.getInstance(OfferBookService.class);
        tradeStatisticsManager = injector.getInstance(TradeStatisticsManager.class);
    }

    @Override
    protected void startApplication() {
        super.startApplication();
        log.info("Scraper started. Waiting for P2P network sync...");

        // Create output directories
        mkdirs(getOffersDir());
        mkdirs(getTradeStatsDir());

        // Load already-saved trade stat keys so we only append new ones
        loadSeenTradeStatKeys();

        p2PService.addP2PServiceListener(new P2PServiceListener() {
            @Override
            public void onDataReceived() {
                log.info("Initial data received. P2P network is synced.");
                isSynced = true;
                startScrapingTask();
            }

            @Override
            public void onNoSeedNodeAvailable() {
                log.error("No seed nodes available!");
            }

            @Override
            public void onNoPeersAvailable() {
                log.warn("No peers available.");
            }

            @Override
            public void onUpdatedDataReceived() {
                log.info("Updated data received.");
            }

            @Override
            public void onTorNodeReady() {
                log.info("Tor node is ready.");
            }

            @Override
            public void onHiddenServicePublished() {
                log.info("Hidden service published.");
            }

            @Override
            public void onSetupFailed(Throwable throwable) {
                log.error("P2P setup failed", throwable);
            }
        });

        // Fallback: start scraping after 5 minutes anyway if we have some data
        UserThread.runAfter(() -> {
            if (!isSynced) {
                log.warn("Wait period over, starting scraping task anyway.");
                startScrapingTask();
            }
        }, 5, TimeUnit.MINUTES);
    }

    private void startScrapingTask() {
        if (dumpTaskStarted) return;
        dumpTaskStarted = true;

        // Initial dump immediately
        UserThread.execute(this::dumpData);

        // Then periodic dumps every 1 minute
        UserThread.runPeriodically(this::dumpData, 1, TimeUnit.MINUTES);

        // Cleanup old offer snapshots once per hour
        UserThread.runPeriodically(this::cleanupOldOfferSnapshots, 1, TimeUnit.HOURS);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    // Directory helpers
    ///////////////////////////////////////////////////////////////////////////////////////////

    private File getOffersDir() {
        return new File(config.storageDir, "offers");
    }

    private File getTradeStatsDir() {
        return new File(config.storageDir, "trade_stats");
    }

    private void mkdirs(File dir) {
        if (!dir.exists() && !dir.mkdirs()) {
            log.warn("Failed to create directory: {}", dir.getAbsolutePath());
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    // Data dumping
    ///////////////////////////////////////////////////////////////////////////////////////////

    private void dumpData() {
        try {
            dumpOffers();
            dumpTradeStats();
        } catch (Exception e) {
            log.error("Error during data dump", e);
        }
    }

    private void dumpOffers() {
        List<Offer> offers = offerBookService.getOffers();
        log.info("Currently discovered {} offers", offers.size());

        if (offers.isEmpty()) return;

        String timestamp = TIMESTAMP_FMT.format(Instant.now());
        String json = JsonUtil.objectToJson(
                offers.stream().map(Offer::getOfferPayloadBase).collect(Collectors.toList())
        );

        // Save timestamped snapshot
        File snapshot = new File(getOffersDir(), "offers_" + timestamp + ".json");
        saveToFile(snapshot, json);

        // Also keep a "latest" copy for convenience
        saveToFile(new File(getOffersDir(), "offers_latest.json"), json);
    }

    private void dumpTradeStats() {
        List<TradeStatistics3> stats = tradeStatisticsManager
                .getObservableTradeStatisticsSet().stream()
                .collect(Collectors.toList());

        log.info("Currently discovered {} trade statistics ({} already saved)",
                stats.size(), seenTradeStatKeys.size());

        if (stats.isEmpty()) return;

        File historyFile = new File(getTradeStatsDir(), "history.jsonl");
        int newCount = 0;

        // Append-only: only write stats we haven't seen before.
        // Each line is: <base64_hash>\t<json>
        try (FileWriter writer = new FileWriter(historyFile, true)) {
            for (TradeStatistics3 stat : stats) {
                String key = Base64.getEncoder().encodeToString(stat.getHash());
                if (seenTradeStatKeys.add(key)) {
                    writer.write(key + "\t" + JsonUtil.toJsonCompact(stat) + "\n");
                    newCount++;
                }
            }
            writer.flush();
        } catch (IOException e) {
            log.error("Failed to append trade stats to history.jsonl", e);
            return;
        }

        if (newCount > 0) {
            log.info("Appended {} new trade statistics to history.jsonl", newCount);

            // Save full current set as "latest" for convenience
            String fullJson = JsonUtil.objectToJson(stats);
            saveToFile(new File(getTradeStatsDir(), "latest.json"), fullJson);
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    // History tracking
    ///////////////////////////////////////////////////////////////////////////////////////////

    /**
     * On startup, load the set of trade stat hashes we've already saved to history.jsonl
     * so we only append new entries on subsequent dumps.
     *
     * If the scraper was killed mid-write (OOM, kill -9, power loss), the last line of
     * history.jsonl may be truncated/corrupt. We detect and truncate any such line so that:
     * - The interrupted trade stat is NOT marked as "seen" and will be re-appended
     * - No garbage hashes pollute the dedup set
     */
    private void loadSeenTradeStatKeys() {
        File historyFile = new File(getTradeStatsDir(), "history.jsonl");
        if (!historyFile.exists()) return;

        // Repair: truncate incomplete trailing line from a crash mid-write
        repairTrailingLine(historyFile);

        try (BufferedReader reader = new BufferedReader(new FileReader(historyFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                int tab = line.indexOf('\t');
                if (tab > 0) {
                    seenTradeStatKeys.add(line.substring(0, tab));
                }
            }
        } catch (IOException e) {
            log.error("Failed to load trade stat keys from history.jsonl", e);
        }

        log.info("Loaded {} existing trade stat keys from history.jsonl", seenTradeStatKeys.size());
    }

    /**
     * If the last line of the file does not end with a newline and does not look like
     * a valid entry (contains a tab followed by a JSON object), truncate it.
     * This repairs the file after a crash during an append.
     */
    private void repairTrailingLine(File file) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            long length = raf.length();
            if (length == 0) return;

            // Seek near the end to read the last portion
            int scanSize = (int) Math.min(length, 4096);
            raf.seek(length - scanSize);
            byte[] buf = new byte[scanSize];
            int read = raf.read(buf);
            String tail = new String(buf, 0, read, "UTF-8");

            // Find the last newline
            int lastNewline = tail.lastIndexOf('\n');
            if (lastNewline < 0) {
                // No newline at all — single corrupt line or very small file
                if (!tail.contains("\t") || !tail.contains("{")) {
                    log.warn("history.jsonl appears corrupt (no valid lines). Truncating.");
                    raf.setLength(0);
                    return;
                }
                // Single valid line with no trailing newline — that's ok
                return;
            }

            // Check if there's content after the last newline
            String afterLastNewline = tail.substring(lastNewline + 1);
            if (!afterLastNewline.isEmpty()) {
                // There's a partial line at the end — check if it's valid
                if (afterLastNewline.contains("\t") && afterLastNewline.trim().endsWith("}")) {
                    // Looks complete (has tab and ends with }), just missing newline — ok
                    return;
                }
                log.warn("history.jsonl has incomplete trailing line (likely from crash). Truncating.");
                long truncateTo = length - scanSize + lastNewline + 1;
                raf.setLength(truncateTo);
            }
        } catch (IOException e) {
            log.error("Failed to repair history.jsonl", e);
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    // Maintenance
    ///////////////////////////////////////////////////////////////////////////////////////////

    private void cleanupOldOfferSnapshots() {
        File offersDir = getOffersDir();
        File[] files = offersDir.listFiles((dir, name) ->
                name.startsWith("offers_") && name.endsWith(".json") && !name.contains("latest"));
        if (files == null) return;

        long cutoff = Instant.now()
                .minusMillis(OFFER_SNAPSHOT_RETENTION_DAYS * 24L * 60 * 60 * 1000)
                .toEpochMilli();
        int deleted = 0;
        for (File file : files) {
            if (file.lastModified() < cutoff) {
                if (file.delete()) deleted++;
            }
        }
        if (deleted > 0) {
            log.info("Cleaned up {} old offer snapshots (older than {} days)", deleted, OFFER_SNAPSHOT_RETENTION_DAYS);
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    // File I/O
    ///////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Write content to a file atomically (write to .tmp then rename).
     */
    private void saveToFile(File file, String content) {
        File temp = new File(file.getAbsolutePath() + ".tmp");
        try (FileWriter writer = new FileWriter(temp)) {
            writer.write(content);
        } catch (IOException e) {
            log.error("Failed to write temp file for {}", file.getAbsolutePath(), e);
            return;
        }

        // Atomic rename
        if (!temp.renameTo(file)) {
            file.delete();
            if (!temp.renameTo(file)) {
                log.error("Failed to save to {}", file.getAbsolutePath());
            }
        }
    }

    @Override
    protected void doExecute() {
        super.doExecute();
        keepRunning();
    }
}
