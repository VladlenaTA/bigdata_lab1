import bdtc.lab1.CarMileageAverage;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CarMileageAverageTest {

    private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
    private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;

    @Before
    public void setUp() {
        CarMileageAverage.MileageMapper mapper = new CarMileageAverage.MileageMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        CarMileageAverage.AverageReducer reducer = new CarMileageAverage.AverageReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapperValidData() throws IOException {
        mapDriver
                .withInput(new LongWritable(1), new Text("1 25.5")) // "1" is assumed to be a car model
                .withOutput(new Text("1"), new DoubleWritable(25.5))
                .runTest();
    }

    @Test
    public void testMapperInvalidData() throws IOException {
        mapDriver
                .withInput(new LongWritable(1), new Text("Toyota invalid"))
                .runTest();
        Counter counter = mapDriver.getCounters().findCounter("Invalid Data", "Invalid Mileage");
        assert (counter.getValue() == 1);
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver
                .withInput(new Text("1"), Arrays.asList(new DoubleWritable(25.5), new DoubleWritable(30.0)))
                .withOutput(new Text("Vesta"), new DoubleWritable(27.75))
                .runTest();
    }

}
