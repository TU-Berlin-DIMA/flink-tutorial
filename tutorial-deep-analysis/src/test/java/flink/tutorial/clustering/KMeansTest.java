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
    protected void preSubmit() throws Exception {
        dataPath = createTempFile("datapoints.txt", KMeansData.DATAPOINTS_2D);
        clusterPath = createTempFile("initial_centers.txt", KMeansData.INITIAL_CENTERS_2D);
        resultPath = getTempDirPath("result");
    }

    @Override
    protected void testProgram() throws Exception {
        KMeans kmeans = new KMeans();
        kmeans.main(dataPath, clusterPath, resultPath, "20", "true");
    }

    @Override
    protected void postSubmit() throws Exception {
        List<String> resultLines = new ArrayList<String>();
        readAllResultLines(resultLines, resultPath);

        KMeansData.checkResultsWithDelta(KMeansData.CENTERS_2D_AFTER_20_ITERATIONS_DOUBLE_DIGIT, resultLines, 0.1);
    }
}
