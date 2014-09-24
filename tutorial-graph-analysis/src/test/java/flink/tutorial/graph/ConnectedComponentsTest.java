package flink.tutorial.graph;

import org.apache.flink.test.testdata.ConnectedComponentsData;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.io.BufferedReader;


public class ConnectedComponentsTest extends JavaProgramTestBase {

    private static final long SEED = 0xBADC0FFEEBEEFL;

    private static final int NUM_VERTICES = 1000;

    private static final int NUM_EDGES = 10000;


    protected String verticesPath;
    protected String edgesPath;
    protected String resultPath;

    @Override
    protected void preSubmit() throws Exception {
        verticesPath = createTempFile("vertices.txt", ConnectedComponentsData.getEnumeratingVertices(NUM_VERTICES));
        edgesPath = createTempFile("edges.txt", ConnectedComponentsData.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED));
        resultPath = getTempFilePath("results");
    }

    @Override
    protected void testProgram() throws Exception {
        ConnectedComponents cc = new ConnectedComponents();
        cc.main(verticesPath, edgesPath, resultPath, "100");
    }

    @Override
    protected void postSubmit() throws Exception {
        for (BufferedReader reader : getResultReader(resultPath)) {
            ConnectedComponentsData.checkOddEvenResult(reader);
        }
    }
}
