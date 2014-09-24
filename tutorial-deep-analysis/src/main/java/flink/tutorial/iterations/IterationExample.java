package flink.tutorial.iterations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.IterativeDataSet;

public class IterationExample {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // create data set out from list
        DataSet<Integer> numbers = env.fromElements(1, 2, 3, 4, 5);

        // define iteration which iterates 10 times and an initial work set
        IterativeDataSet<Integer> loop = numbers.iterate(10);

        // step function: adds 1 to each value
        DataSet<Integer> stepfunction = loop.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        });

        // define iteration result
        DataSet<Integer> result = loop.closeWith(stepfunction);

        // print result
        result.print();

        // execute program
        env.execute("Iteration Example");
    }
}
