package flink.tutorial.wordcount;

import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.JavaProgramTestBase;

/**
 *
 */
public class WordCountTest extends JavaProgramTestBase {

    protected String textPath;
    protected String resultPath;

    @Override
    protected void preSubmit() throws Exception {
        textPath = createTempFile("text.txt", WordCountData.TEXT);
        resultPath = getTempDirPath("result");
    }

    @Override
    protected void testProgram() throws Exception {

        WordCount wc = new WordCount();
        wc.main(textPath, resultPath);
    }

    @Override
    protected void postSubmit() throws Exception {
        // Test results
        compareResultsByLinesInMemory(WordCountData.COUNTS, resultPath);
    }
}
