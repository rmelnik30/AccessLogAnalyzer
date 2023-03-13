package org.livescore;

import org.apache.commons.cli.*;
import org.apache.commons.compress.archivers.ArchiveException;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

public class Main {

    public static void main(String[] args) {
        Option input = Option.builder("f")
                .argName("file")
                .hasArg()
                .required()
                .desc("input file or folder to be analyzed")
                .build();
        Option output = Option.builder("o")
                .argName("output")
                .hasArg()
                .desc("output folder to store analysis results")
                .build();
        Option split = Option.builder("s")
                .argName("split")
                .hasArg()
                .desc("time range to split input logs by")
                .build();

        Options options = new Options();
        options.addOption(input);
        options.addOption(output);
        options.addOption(split);

        // create the parser
        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            String inputFile = line.getOptionValue(input);

            String outputFolder = Objects.requireNonNullElse(line.getOptionValue(output), "./");

            long splitDuration = Duration.parse(Objects.requireNonNullElse(line.getOptionValue(split), "P1D")).toMillis();

            launch(inputFile, outputFolder, splitDuration);
        }
        catch (NullPointerException | ParseException | IllegalArgumentException | IOException | ArchiveException exp) {
            // oops, something went wrong
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            // automatically generate the help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("AccessLogAnalyzer", options, true);
            System.exit(5);
        }
    }

    private static void launch(String inputFile, String outputFolder, long splitDuration) throws IOException, ArchiveException {
        Analyzer l1Analyzer = new Analyzer(1, splitDuration);
        Analyzer l2Analyzer = new Analyzer(2, splitDuration);
        Analyzer l3Analyzer = new Analyzer(3, splitDuration);
        Analyzer l4Analyzer = new Analyzer(4, splitDuration);
        Analyzer l5Analyzer = new Analyzer(5, splitDuration);
        Analyzer l6Analyzer = new Analyzer(6, splitDuration);
        Analyzer l7Analyzer = new Analyzer(7, splitDuration);
        List<Analyzer> analyzerList = List.of(l1Analyzer,
                l2Analyzer,
                l3Analyzer,
                l4Analyzer,
                l5Analyzer,
                l6Analyzer,
                l7Analyzer);

        Parser parser = new Parser(analyzerList, outputFolder);
        Walker walker = new Walker(parser);
        walker.walk(inputFile);
        analyzerList.forEach(analyzer -> analyzer.flushResults(outputFolder));
    }

}