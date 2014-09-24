package flink.tutorial.wordcount;

import flink.tutorial.wordcount.util.WordCountData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over text files.
 * <p/>
 * <p/>
 * The input is a plain text file with lines separated by newline characters.
 * <p/>
 * <p/>
 * Usage: <code>WordCount &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link flink.tutorial.wordcount.util.WordCountData}.
 * <p/>
 * <p/>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 */
@SuppressWarnings("serial")
public class WordCount {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String... args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataSet<String> text = getTextDataSet(env);

        DataSet<Tuple2<String, Integer>> counts =
                        // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        // sum up the word counts
                        .reduce(new Counter());
                        // more elegant with aggregator
                        //.sum(1);

        // emit result
        if (fileOutput) {
            counts.writeAsCsv(outputPath, "\n", " ");
        } else {
            counts.print();
        }

        // execute program
        env.execute("WordCount Example");
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the reduce function that sums up all the words.
     */
    private static class Counter implements ReduceFunction<Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            return new Tuple2<String, Integer>(value1.f0, value1.f1 + value2.f1);
        }
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static String textPath;
    private static String outputPath;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            // parse input arguments
            fileOutput = true;
            if (args.length == 2) {
                textPath = args[0];
                outputPath = args[1];
            } else {
                System.err.println("Usage: WordCount <text path> <result path>");
                return false;
            }
        } else {
            System.out.println("Executing WordCount example with built-in default data.");
            System.out.println("  Provide parameters to read input data from a file.");
            System.out.println("  Usage: WordCount <text path> <result path>");
        }
        return true;
    }

    private static DataSet<String> getTextDataSet(ExecutionEnvironment env) {
        if (fileOutput) {
            // read the text file from given input path
            return env.readTextFile(textPath);
        } else {
            // get default test text data
            return WordCountData.getDefaultTextLineDataSet(env);
        }
    }
}