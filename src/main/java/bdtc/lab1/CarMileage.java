package bdtc.lab1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CarMileage {
    private static final Map<Integer, String> carModels = new HashMap<>();

    static {
        carModels.put(1, "Vesta");
        carModels.put(2, "Matiz");
        carModels.put(3, "Uaz");
        carModels.put(4, "Toyota");
        carModels.put(5, "Honda");
        carModels.put(6, "Nissan");
    }

    public static void main(String[] args) {
        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .appName("CarMileage")
                .master("local")
                .getOrCreate();

        // Enable log4j output during execution
        spark.sparkContext().setLogLevel("INFO");

        // Load input data from file
        JavaRDD<String> inputRDD = spark.read().textFile(args[0]).javaRDD();

        // Mapping data to pairs (carModel, mileage)
        JavaPairRDD<String, Double> mappedRDD = inputRDD.flatMapToPair(line -> {
            String[] tokens = line.split(" ");
            if (tokens.length >= 2) {
                int carModelNumber;
                try {
                    carModelNumber = Integer.parseInt(tokens[0]);
                } catch (NumberFormatException e) {
                    // Handle invalid car model number and skip the record
                    return Collections.emptyIterator();
                }

                String carModel = carModels.get(carModelNumber);
                if (carModel != null) {
                    double mileage;
                    try {
                        mileage = Double.parseDouble(tokens[1]);
                    } catch (NumberFormatException e) {
                        // Handle invalid mileage and skip the record
                        return Collections.emptyIterator();
                    }

                    return Collections.singletonList(new Tuple2<>(carModel, mileage)).iterator();
                }
            }

            return Collections.emptyIterator();
        });

        // Count invalid mileage values
        long invalidCount = inputRDD.count() - mappedRDD.count();

        System.out.println("Invalid Mileage Count: " + invalidCount);

        // Reduce by carModel to compute sum and count of mileage for each model
        JavaPairRDD<String, Tuple2<Double, Integer>> reducedRDD = mappedRDD.aggregateByKey(
                new Tuple2<>(0.0, 0),
                (accumulator, mileage) -> new Tuple2<>(accumulator._1() + mileage, accumulator._2() + 1),
                (accumulator1, accumulator2) -> new Tuple2<>(accumulator1._1() + accumulator2._1(), accumulator1._2() + accumulator2._2())
        );

        // Compute average mileage for each model
        JavaPairRDD<String, Double> averageRDD = reducedRDD.mapValues(t -> t._1() / t._2());

        // Save results to output file
        averageRDD.saveAsTextFile(args[1]);

        // Stop Spark session
        //spark.stop();
    }
}
