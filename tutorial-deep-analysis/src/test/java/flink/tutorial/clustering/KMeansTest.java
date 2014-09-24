package flink.tutorial.clustering;

import org.apache.flink.test.testdata.KMeansData;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.util.ArrayList;
import java.util.List;


public class KMeansTest extends JavaProgramTestBase {

    protected String dataPath;
    protected String clusterPath;
    protected String resultPath;

    @Override
    protected void testProgram() throws Exception {
        KMeans kmeans = new KMeans();
        kmeans.main(dataPath, clusterPath, resultPath, "20");
    }

    @Override
    protected void preSubmit() throws Exception {
        dataPath = createTempFile("datapoints.txt", KMeansData.DATAPOINTS);
        clusterPath = createTempFile("initial_centers.txt", KMeansData.INITIAL_CENTERS);
        resultPath = getTempDirPath("result");
    }

    @Override
    protected void postSubmit() throws Exception {
        List<String> resultLines = new ArrayList<String>();
        readAllResultLines(resultLines, resultPath);

        KMeansData.checkResultsWithDelta(KMeansData.CENTERS_AFTER_20_ITERATIONS_SINGLE_DIGIT, resultLines, 0.1);
    }
}
