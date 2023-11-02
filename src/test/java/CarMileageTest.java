import bdtc.lab1.CarMileage;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CarMileageTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CarMileageTest")
                .master("local")
                .getOrCreate();

        // Create input data RDD
        List<Tuple2<String, Double>> inputData = Arrays.asList(
                new Tuple2<>("1", 10.5),
                new Tuple2<>("2", 8.2),
                new Tuple2<>("3", Double.NaN),
                new Tuple2<>("4", 12.1),
                new Tuple2<>("5", 9.8),
                new Tuple2<>("6", 15.3)
        );
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<Tuple2<String, Double>> inputRDD = javaSparkContext.parallelize(inputData);

        // Execute CarMileage code
        CarMileage.main(new String[0]);

        // Extract the average mileage results
        JavaPairRDD<String, Double> averageRDD = spark.read().textFile("output").javaRDD()
                .mapToPair(line -> {
                    String[] parts = line.split(" ");
                    return new Tuple2<>(parts[0], Double.parseDouble(parts[1]));
                });

        Map<String, Double> averageMileageMap = averageRDD.collectAsMap();

        // Assert that the average mileages are calculated correctly
        assert (averageMileageMap.get("Vesta") == 10.5);
        assert (averageMileageMap.get("Matiz") == 8.2);
        assert (averageMileageMap.get("Toyota") == 12.1);
        assert (averageMileageMap.get("Honda") == 9.8);
        assert (averageMileageMap.get("Nissan") == 15.3);

        // Clean up
        spark.stop();
    }
}