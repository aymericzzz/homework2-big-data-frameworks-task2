import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper2 extends Mapper<LongWritable, Text, Text, LongWritable>{

    // objet LongWritable pour la valeur
    private LongWritable one = new LongWritable(1);
    // objet Text pour la cl�
    private Text word = new Text();

    // map(key, eache line, mapper context)
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
        // on s�pare chq cat�gorie d�limit�e par un ; => first name;genders;origins;value
        String[] split = line.toString().split(";");

        // on prend la 3e case qui est l'origine et on s�pare chq origine, qu'on stocke dans origins
        String[] origins = split[2].split(",");
        // la paire cr��e est compos�e du nombre d'origines et cette "line" et de la valeur 1
        word.set(String.valueOf(origins.length));
        context.write(word, one);
    }
}