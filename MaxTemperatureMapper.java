// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper
        extends Mapper<LongWritable, Text, LongWritable, Text> {

    private DirectoryTreeOutputs<LongWritable,Text> partitionOutput;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        int [] cn = {0,1,2};
        String [] patterns = {".*([0-9]{2})","\"(.*)\"","\"(.*)\""};
        String [] replacements = {"$1","$1","$1"};
        try {
            partitionOutput = new DirectoryTreeOutputs<LongWritable, Text>(context,cn,patterns,replacements);
        }
        catch (DirectoryTreeOutputs.InvalidPatternsException e) {

        }
    }
    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        partitionOutput.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        partitionOutput.write(key,value);
    }
}
