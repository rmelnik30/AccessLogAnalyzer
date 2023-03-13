package org.livescore;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Analyzer {

    public record StatsEntry(long count, long processingTime) {}
    public record CsvEntry(String uri, String percentage, long count, long processingTime) {}
    private final int level;
    private final long split;
    public final Map<Long, Map<String, StatsEntry>> slotsMap = new HashMap<>();

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

    public void flushResults(String outDir) {
        makeSlotDirs(outDir);
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

    private void writeCsv(Map<String, StatsEntry> statsMap, Path slotPath) {
        long totalCount = statsMap.values().stream()
                .map(v -> v.count).mapToLong(Long::longValue).sum();

        List<CsvEntry> sortedList = statsMap.entrySet().stream()
                .map(e -> new CsvEntry(e.getKey(),
                        String.format("%.2f%%", (double)e.getValue().count() / totalCount * 100.),
                        e.getValue().count(), e.getValue().processingTime()))
                .sorted(Comparator.comparingLong(CsvEntry::count).reversed())
                .toList();

        String fileName = slotPath.toString() + "/L" + level + ".csv";
        try (CSVPrinter printer = new CSVPrinter(new FileWriter(fileName), CSVFormat.EXCEL)) {
            printer.printRecord("uri", "percentage", "count", "totalTime");
            for (CsvEntry entry : sortedList) {
                printer.printRecord(entry.uri(), entry.percentage(), entry.count(), entry.processingTime());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
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
