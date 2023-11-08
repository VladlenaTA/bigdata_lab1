import bdtc.lab1.CarMileage;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CarMileageTest {

    private JavaSparkContext sc;

    private String createdInput = "input2/car_data_test";
    private String createdOutput = "output_test";

    @Before
    public void setUp() {
        sc = new JavaSparkContext("local", "CarMileageTest");
    }

    @After
    public void tearDown() {
        deleteTempFiles();
        sc.stop();
    }

    private void deleteTempFiles(){
        deleteDirectory(new File(createdInput));
        deleteDirectory(new File(createdOutput));
    }

    private static boolean deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();

            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        // Рекурсивное удаление поддиректорий
                        deleteDirectory(file);
                    } else {
                        // Удаление файлов в директории
                        file.delete();
                    }
                }
            }

            // Удаление самой директории
            return directory.delete();
        }

        return false;
    }

    @Test
    public void testMain() {
        // Create a sample input file with valid and invalid data
        JavaRDD<String> inputRDD = sc.parallelize(Arrays.asList(
                "1 10.0",
                "2 15.0",
                "3 invalid",  // Invalid data with non-numeric mileage
                "6 20.0",
                "7 30.0"     // Invalid data with unknown car model
        ));
        inputRDD.saveAsTextFile(createdInput);

        // Call the main method with the test input file
        String[] args = {createdInput, createdOutput};
        CarMileage.main(args);

        // Check the output file for the expected results
        JavaRDD<String> outputRDD = sc.textFile(createdOutput);

        // Check the expected results
        List<String> expected = Arrays.asList("(Vesta,10.0)", "(Matiz,15.0)", "(Nissan,20.0)");
        List<String> actual = outputRDD.collect();

        assertEquals(expected, actual);
    }


}
