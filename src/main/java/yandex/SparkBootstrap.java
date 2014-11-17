package yandex;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.kohsuke.args4j.*;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Yandex Spark playground entry point.
 *
 * @author mdodsworth
 */
public class SparkBootstrap {

    public static void main(String[] args) {
        SparkArgumentContainer arguments = parseArguments(args);

        Path path = arguments.input;
        SparkConf conf = new SparkConf().setAppName("Yandex Playground");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> queryData = context.textFile(path.toString());

        long count = queryData.filter(query -> query.contains("blah")).count();
        System.out.println(count);
    }

    /**
     * @return the parsed command-line arguments.
     * @throws IllegalArgumentException when the supplied command-line arguments are invalid.
     */
    private static SparkArgumentContainer parseArguments(String[] args) throws IllegalArgumentException {
        SparkArgumentContainer arguments = new SparkArgumentContainer();

        CmdLineParser parser = new CmdLineParser(arguments);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            System.err.println("java SparkBootstrap [options...] arguments...");
            parser.printUsage(System.err);
            System.err.println();
            System.err.println("  Example: java SparkBootstrap" + parser.printExample(OptionHandlerFilter.ALL));
            throw new IllegalArgumentException(e);
        }

        return arguments;
    }

    private static class SparkArgumentContainer {
        @Option(name = "-i", aliases = "-input", metaVar = "INPUT", usage = "input file or directory")
        private Path input = Paths.get("input");

        @Option(name = "-o", aliases = "-output", metaVar = "OUTPUT", usage = "output file or directory")
        private Path output = Paths.get("output");
    }
}
