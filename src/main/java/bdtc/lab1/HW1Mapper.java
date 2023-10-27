package bdtc.lab1;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        UserAgent userAgent = UserAgent.parseUserAgentString(line);
        if (userAgent.getBrowser() == Browser.UNKNOWN) {
            context.getCounter(CounterType.MALFORMED).increment(1);
        } else {
            word.set(userAgent.getBrowser().getName());
            context.write(word, one);
        }
    }
}
