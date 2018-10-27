package agd;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;

//import org.apache.log4j.Logger;

public class MSSPReducer
    extends Reducer<Text, Text, Text, Text> {
    
    //private Logger logger = Logger.getLogger(this.getClass());

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        Text result = new Text();
        Integer minDistance = Integer.MAX_VALUE;
        String neighbours = null;
        Counter updated =
            context.getCounter(MSSP.MoreIterations.numUpdated);
        Integer existingDistance = Integer.MAX_VALUE;

        for (Text val : values) {
            String[] curNodeData = val.toString().split("\\s+");
            
            if (curNodeData.length == 1) {
                Integer distance = Integer.parseInt(curNodeData[0]);
                if (distance < minDistance) {
                    minDistance = distance;
                }
            } else {
                existingDistance = Integer.parseInt(curNodeData[1]);
                if (existingDistance < minDistance) {
                    minDistance = existingDistance;
                }
                neighbours = curNodeData[0];
            }
            
        }
        

        if (minDistance < existingDistance) {
            updated.increment(1);
        }
        
        result.set(String.join(" ", neighbours,
                Integer.toString(minDistance)));
        context.write(key, result);
    }
}
