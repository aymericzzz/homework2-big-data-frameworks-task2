import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper2 extends Mapper<LongWritable, Text, Text, LongWritable>{

    // objet LongWritable pour la valeur
    private LongWritable one = new LongWritable(1);
    // objet Text pour la clé
    private Text word = new Text();

    // map(key, eache line, mapper context)
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
        // on sépare chq catégorie délimitée par un ; => first name;genders;origins;value
        String[] split = line.toString().split(";");

        // on prend la 3e case qui est l'origine et on sépare chq origine, qu'on stocke dans origins
        String[] origins = split[2].split(",");
        // la paire créée est composée du nombre d'origines et cette "line" et de la valeur 1
        word.set(String.valueOf(origins.length));
        context.write(word, one);
    }
}