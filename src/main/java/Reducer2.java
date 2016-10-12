import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer2 extends Reducer<Text, LongWritable, Text, LongWritable> {

    // valeur apr�s reduce
    private LongWritable result = new LongWritable();

    // reduce(key, values, context)
    public void reduce(Text key, Iterable<LongWritable> values, Context context)throws IOException, InterruptedException{
        long temp = 0;

        // on lit chaque valeur du tableau values contenant les paires ayant la m�me key
        // puis, on somme chaque val
        for(LongWritable val: values){
            temp += val.get();
        }

        // on set la somme des valeurs dans un objet LongWritable
        result.set(temp);
        // on �crit une nouvelle paire contenant la cl� ainsi que la somme des valeurs ayant cette m�me cl�
        context.write(key, result);
    }
}