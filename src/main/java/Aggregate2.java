import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Aggregate2 extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(getClass());
        job.setJobName(getClass().getSimpleName());

        // input path correspond � args[0] = le fichier � �tudier
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // output path correspond � args[1] = le fichier de output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // on set le mapper qui va mapper l'input
        job.setMapperClass(Mapper2.class);
        // on set le reducer pour le reduce job qui va reduce toutes les maps ensemble
        job.setReducerClass(Reducer2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // lance un tool avec une configuration : Aggregate2 est un objet Tool, et on lui donne les param�tres renseign�es dans le terminal (args)
        int rc = ToolRunner.run(new Aggregate2(), args);
        System.exit(rc);
    }
}