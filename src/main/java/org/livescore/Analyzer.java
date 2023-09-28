package org.livescore;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.livescore.model.LBEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Analyzer {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public record StatsEntry(long count, long processingTime) {}
    public record CsvEntry(String uri, String countRate, String trafficRate,
                           long count, long value,
                           String iOs, String android, String web, String other) {}
    public record LBStats(long count, long size, long iOs, long android, long web, long other) {}

    private final int level;
    private final long split;
    public final Map<Long, Map<String, StatsEntry>> slotsMap = new HashMap<>();
    public final Map<Long, Map<String, LBStats>> lbSlotsMap = new HashMap<>();

    private long logCount = 0L;
    private long noSizeResponses = 0L;

    public Analyzer(int level, long split) {
        this.level = level;
        this.split = split;
    }

    public void analyze(LogEntry logEntry) {
        long slot = logEntry.timestamp / split;
        Map<String, StatsEntry> statsMap = slotsMap.computeIfAbsent(slot, k -> new HashMap<>());
        String uri = calculateUri(logEntry.uri);
        StatsEntry statsEntry = statsMap.computeIfAbsent(uri, k -> new StatsEntry(0L, 0L));
        StatsEntry newStatsEntry = new StatsEntry(statsEntry.count() + 1, statsEntry.processingTime() + logEntry.processingTime);

        statsMap.put(uri, newStatsEntry);
        slotsMap.put(slot, statsMap);
    }

    public void analyze(LBEntry logEntry) {
        long slot = logEntry.timestamp().toInstant().toEpochMilli() / split;

        Map<String, LBStats> statsMap = lbSlotsMap.computeIfAbsent(slot, k -> new HashMap<>());
        String uri = calculateUri(logEntry.httpRequest().requestUrl());
        LBStats lbStats = statsMap.computeIfAbsent(uri, k -> new LBStats(0L, 0L, 0L, 0L, 0L, 0L));
        long responseSize = 0;
        try {
            responseSize = Long.parseLong(logEntry.httpRequest().responseSize());
        } catch (NumberFormatException nfe) {
            noSizeResponses++;
        }

        boolean isIOS = logEntry.httpRequest().userAgent() != null && logEntry.httpRequest().userAgent().contains("LiveScore/iOS");
        boolean isAndroid = logEntry.httpRequest().userAgent() != null && logEntry.httpRequest().userAgent().contains("LiveScore/Android");
        boolean isWeb = logEntry.httpRequest().referer() != null && logEntry.httpRequest().referer().contains(".livescore.");
        boolean isOther = !(isIOS || isAndroid || isWeb);

        LBStats newLbStatsEntry = new LBStats(lbStats.count() + 1,
                lbStats.size() + responseSize,
                isIOS ? lbStats.iOs() + responseSize : lbStats.iOs(),
                isAndroid ? lbStats.android() + responseSize : lbStats.android(),
                isWeb ? lbStats.web() + responseSize : lbStats.web(),
                isOther ? lbStats.other() + responseSize : lbStats.other());

        if (++logCount % 100000 == 0)
            log.debug("LB log processed: L{}, C{}, {}", level, logCount, newLbStatsEntry);

        statsMap.put(uri, newLbStatsEntry);
        lbSlotsMap.put(slot, statsMap);
    }

    public void flushResults(String outDir) {
        makeSlotDirs(outDir);
    }

    public void flushResults2(String outDir) {
        makeSlotDirs2(outDir);
    }

    private void makeSlotDirs(String outDir) {
        slotsMap.keySet().forEach(k -> {
            try {
                Path slotPath = Paths.get(outDir, convertSlotToPath(k));
                Files.createDirectories(slotPath);
                writeCsv(slotsMap.get(k), slotPath);
            } catch (IOException e) {
                System.err.println(e);
            }
        });
    }

    private void makeSlotDirs2(String outDir) {
        lbSlotsMap.keySet().forEach(k -> {
            try {
                Path slotPath = Paths.get(outDir, convertSlotToPath(k));
                Files.createDirectories(slotPath);
                writeCsv2(lbSlotsMap.get(k), slotPath);
            } catch (IOException e) {
                System.err.println(e);
            }
        });
    }

    private void writeCsv(Map<String, StatsEntry> statsMap, Path slotPath) {
        long totalCount = statsMap.values().stream()
                .map(v -> v.count).mapToLong(Long::longValue).sum();

        List<CsvEntry> sortedList = statsMap.entrySet().stream()
                .map(e -> new CsvEntry(e.getKey(),
                        String.format("%.2f%%", (double)e.getValue().count() / totalCount * 100.),
                        "",
                        e.getValue().count(), e.getValue().processingTime(),
                        "", "", "", ""))
                .sorted(Comparator.comparingLong(CsvEntry::count).reversed())
                .toList();

        String fileName = slotPath.toString() + "/L" + level + ".csv";
        try (CSVPrinter printer = new CSVPrinter(new FileWriter(fileName), CSVFormat.EXCEL)) {
            printer.printRecord("uri", "countRate", "count", "totalTime");
            for (CsvEntry entry : sortedList) {
                printer.printRecord(entry.uri(), entry.countRate(), entry.count(), entry.value());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void writeCsv2(Map<String, LBStats> statsMap, Path slotPath) {
        long totalCount = statsMap.values().stream()
                .map(LBStats::count).mapToLong(Long::longValue).sum();

        long totalTraffic = statsMap.values().stream()
                .map(LBStats::size).mapToLong(Long::longValue).sum();

        List<CsvEntry> sortedList = statsMap.entrySet().stream()
                .map(e -> new CsvEntry(e.getKey(),
                        String.format("%.2f%%", (double)e.getValue().count() / totalCount * 100.),
                        String.format("%.2f%%", (double)e.getValue().size() / totalTraffic * 100.),
                        e.getValue().count(),
                        e.getValue().size(),
                        String.format("%.2f%%", (double)e.getValue().iOs() / e.getValue().size() * 100.),
                        String.format("%.2f%%", (double)e.getValue().android() / e.getValue().size() * 100.),
                        String.format("%.2f%%", (double)e.getValue().web() / e.getValue().size() * 100.),
                        String.format("%.2f%%", (double)e.getValue().other() / e.getValue().size() * 100.)))
                .sorted(Comparator.comparingLong(CsvEntry::value).reversed())
                .toList();

        String fileName = slotPath.toString() + "/L" + level + ".csv";
        try (CSVPrinter printer = new CSVPrinter(new FileWriter(fileName), CSVFormat.EXCEL)) {
            printer.printRecord("uri", "traffic rate", "calls rate", "number of calls", "traffic size",
                    "iOS", "Android", "Web", "Other");
            for (CsvEntry entry : sortedList) {
                printer.printRecord(entry.uri(),
                        entry.trafficRate(),
                        entry.countRate(),
                        entry.count(),
                        FileUtils.byteCountToDisplaySize(entry.value()),
                        entry.iOs(),
                        entry.android(),
                        entry.web(),
                        entry.other());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        log.debug("Total reqs: {}, total traffic: {}, no size responses: {}", totalCount, totalTraffic, noSizeResponses);
    }

    private String convertSlotToPath(Long slot) {
        long timestamp = slot * split;
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(timestamp);

        return String.format("%d-%02d-%02d-%02d-%02d",
                calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH) + 1,
                calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY),
                calendar.get(Calendar.MINUTE));
    }

    private String calculateUri(String uri) {
        String[] levels = uri.split("[/?&]");
        int finalLevel = Math.min(levels.length - 1, level);
        int finalPos = 0;
        for (int i = 0; i < finalLevel + 1; ++i) {
            finalPos += levels[i].length() + 1;
        }

        return uri.substring(0, finalPos - 1);
    }

}
