import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GrandChild {
    public static class FamilyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("child")) return;
            String[] p = line.split("\\s+");
            if (p.length < 2) return;
            context.write(new Text(p[0]), new Text("1:" + p[1]));
            context.write(new Text(p[1]), new Text("2:" + p[0]));
        }
    }

    public static class FamilyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            context.write(new Text("grandchild"), new Text("grandparent"));
        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> parents = new ArrayList<>();
            List<String> children = new ArrayList<>();
            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("1:")) parents.add(s.substring(2));
                else if (s.startsWith("2:")) children.add(s.substring(2));
            }
            for (String child : children) {
                for (String parent : parents) context.write(new Text(child), new Text(parent));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "grandchild");
        job.setJarByClass(GrandChild.class);
        job.setMapperClass(FamilyMapper.class);
        job.setReducerClass(FamilyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
