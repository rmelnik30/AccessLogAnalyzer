package org.livescore;

import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;

import java.io.*;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Stream;

public class Parser {

    private static final String LOG_FORMAT = "%h - - [%{%a,%e %b %Y %T %Z}t] \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" %{ms}Tms";
    private static final nl.basjes.parse.core.Parser<LogEntry> logParser = new HttpdLoglineParser<>(LogEntry.class, LOG_FORMAT);

    private final List<Analyzer> analyzerList;
    private final String outDir;

    public Parser(List<Analyzer> analyzerList, String outDir) {
        this.analyzerList = analyzerList;
        this.outDir = outDir;
        nl.basjes.parse.core.Parser<Object> dummyParser = new HttpdLoglineParser<>(Object.class, LOG_FORMAT);
        List<String> possiblePaths = dummyParser.getPossiblePaths();
        for (String path: possiblePaths) {
            System.out.println(path);
        }
    }

    public void parse(File file) throws IOException {
        try (Stream<String> linesStream = Files.lines(file.toPath())) {
            linesStream.forEach(line -> {
                if (line.contains("/healthcheck") || line.contains("/prometheus"))
                    return;

                try {
                    LogEntry logEntry = logParser.parse(line);
                    analyzerList.forEach(analyzer -> analyzer.analyze(logEntry));
                } catch (DissectionFailure|InvalidDissectorException|MissingDissectorsException e) {
                    System.err.println(e);
                }
            });
        }
        analyzerList.forEach(analyzer -> analyzer.flushResults(outDir));
    }

    public void parse(InputStream inputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        while(reader.ready()) {
            String line = reader.readLine();
            if (line.contains("/healthcheck") || line.contains("/prometheus"))
                continue;

            try {
                LogEntry logEntry = logParser.parse(line);
                analyzerList.forEach(analyzer -> analyzer.analyze(logEntry));
            } catch (DissectionFailure|InvalidDissectorException|MissingDissectorsException e) {
                System.err.println(e);
            }
        }
        analyzerList.forEach(analyzer -> analyzer.flushResults(outDir));
    }

}
