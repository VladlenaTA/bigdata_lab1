package bdtc.lab1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CarMileageAverage {
    public static class MileageMapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final Text carModel = new Text();
        private final DoubleWritable mileage = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            if (tokenizer.hasMoreTokens()) {
                carModel.set(tokenizer.nextToken());
                if (tokenizer.hasMoreTokens()) {
                    try {
                        mileage.set(Double.parseDouble(tokenizer.nextToken()));
                        context.write(carModel, mileage);
                    } catch (NumberFormatException e) {
                        // Увеличьте счетчик "Invalid Mileage" при обнаружении некорректных данных
                        Counter counter = context.getCounter("Invalid Data", "Invalid Mileage");
                        counter.increment(1);
                    }
                }
            }
        }
    }

    public static class AverageReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private final DoubleWritable result = new DoubleWritable();
        private final Map<Integer, String> carModels = new HashMap<>();
        private final Map<String, Double> modelMileageSum = new HashMap<>();
        private final Map<String, Integer> modelMileageCount = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            carModels.put(1, "Vesta");
            carModels.put(2, "Matiz");
            carModels.put(3, "Uaz");
            carModels.put(4, "Toyota");
            carModels.put(5, "Honda");
            carModels.put(6, "Nissan");
        }
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            String carModelName = carModels.get(Integer.parseInt(key.toString()));
            if (carModelName != null) {
                double sum = 0.0;
                int count = 0;
                for (DoubleWritable value : values) {
                    sum += value.get();
                    count++;
                }
                double average = (double) sum / count;
                modelMileageSum.put(carModelName, modelMileageSum.getOrDefault(carModelName, 0.0) + sum);
                modelMileageCount.put(carModelName, modelMileageCount.getOrDefault(carModelName, 0) + count);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Double> entry : modelMileageSum.entrySet()) {
                String carModelName = entry.getKey();
                double sum = entry.getValue();
                int count = modelMileageCount.get(carModelName);
                double average = (double) sum / count;
                result.set(average);
                context.write(new Text(carModelName), result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "browser count");
        job.setJarByClass(CarMileageAverage.class);
        job.setMapperClass(MileageMapper.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //System.exit(job.waitForCompletion(true) ? 0 : 1);

        if (job.waitForCompletion(true)) {
            Counter counter = job.getCounters().findCounter("Invalid Data", "Invalid Data");
            long invalidMileageCount = counter.getValue();

            System.out.println("Invalid Data Count: " + invalidMileageCount);
            System.exit(0);
        } else {
            System.exit(1);
    }
}
}

