import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KNNDriver {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("k", args[2]);
    conf.set("train", "iris_train.csv");
    if (args.length >= 4) {
      conf.set("train", args[3]);
    }
    Job job = Job.getInstance(conf, "knn");
    job.setJarByClass(KNNDriver.class);
    job.setMapperClass(KNNMapper.class);
    job.setMapOutputValueClass(distanceLabel.class);
    job.setReducerClass(KNNReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
