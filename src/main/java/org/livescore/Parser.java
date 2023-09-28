package org.livescore;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;
import org.livescore.model.LBEntry;

import java.io.*;
import java.util.List;

public class Parser {

    private static final String LOG_FORMAT = "%h - - [%{%a, %e %b %Y %T %Z}t] \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" %{ms}Tms";
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
        // Create and configure an ObjectMapper instance
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        // Create a JsonParser instance
        try (JsonParser jsonParser = mapper.getFactory().createParser(file)) {

            // Check the first token
            if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
                throw new IllegalStateException("Expected content to be an array");
            }

            // Iterate over the tokens until the end of the array
            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                LBEntry lbEntry = mapper.readValue(jsonParser, LBEntry.class);
                analyzerList.forEach(analyzer -> analyzer.analyze(lbEntry));
            }
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
