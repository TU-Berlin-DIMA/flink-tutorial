package flink.tutorial.wordcount.pojo;

import flink.tutorial.wordcount.util.WordCountData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
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
    //     WORDCOUNT POJO
    // *************************************************************************

    public static final class Word {
        private String word;
        public int count;

        public Word() {
        }

        public Word(String word, int count) {
            this.word = word;
            this.count = count;
        }

        public String toString() {
            return "(" + word + ", " + count + ")";
        }
    }

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

        DataSet<Word> counts =
                // TODO: Implement a tokenizer that creates a Word POJO for each word
                text.flatMap(new Tokenizer())
                        // TODO: implement a group by using a KeySelector that extracts the word as key
                        .groupBy(new WordSelector())
                                // TODO: implement a Reduce function working on the Word POJO
                        .reduce(new WordReducer());

        // emit result
        if (fileOutput) {
            counts.writeAsCsv(outputPath, "\n", " ");
        } else {
            counts.print();
        }

        // execute program
        env.execute("WordCount Example with POJOs");
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
     */
    public static final class Tokenizer implements FlatMapFunction<String, Word> {

        @Override
        public void flatMap(String value, Collector<Word> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Word(token, 1));
                }
            }
        }
    }

    /**
     * Implements a key selector function that extracts the word of the Word POJO as key.
     */
    private static class WordSelector implements KeySelector<Word, String> {
        @Override
        public String getKey(Word value) throws Exception {
            // TODO: your implementation here
            return null;
        }
    }

    /**
     * Implements the reduce function that sums up all the words.
     */
    private static class WordReducer implements ReduceFunction<Word> {

        @Override
        public Word reduce(Word value1, Word value2) throws Exception {
            // TODO: your implementation here
            return null;
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
