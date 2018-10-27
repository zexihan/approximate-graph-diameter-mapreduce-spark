package agd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

//import org.apache.log4j.Logger;

public class ShortestPathsMapper
    extends Mapper<Object, Text, Text, Text>{

    //private Logger logger = Logger.getLogger(this.getClass());
    
    private static final String NULL_NODE = "null";

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
            
        Configuration conf = context.getConfiguration();
        String source = conf.get("source");
        String line = value.toString();
        String[] nodes = line.split("\\s+");
        if (nodes.length < 2) {
            return;
        }
        
        String from = nodes[0];
        //String partPath = source;
        Integer distance = Integer.MAX_VALUE;
        String zero = Integer.toString(0);
        String inf = Integer.toString(Integer.MAX_VALUE);
        Text outKey = new Text(from);
        Text outValue = new Text();
        
        if (nodes.length == 2) {
            // First iteration, input is the adjacency list
            if (from.equals(source)) {
                distance = 0;
                outValue.set(String.join(" ", nodes[1], zero));
            } else {
                outValue.set(String.join(" ", nodes[1], inf));
            }
        } else {
            // Since the second iteration, input is the reducer's output
            distance = Integer.parseInt(nodes[2]);
            //partPath = nodes[3];
            outValue.set(String.join(" ", nodes[1], nodes[2]));
        }
        // Pass along the graph structure: NODE ADJACENCY-LIST DISTANCE PATH
        context.write(outKey, outValue);
        
        for (String neighbour : nodes[1].split(",")) {
            //String pathFromSource = source;
            if (neighbour.equals(NULL_NODE)) {
                return;
            }
            outKey.set(neighbour);
            Integer newDistance = distance;
            if (newDistance != Integer.MAX_VALUE) {
                newDistance += 1;
            
                outValue.set(Integer.toString(newDistance));
                // NODE DISTANCE
                context.write(outKey, outValue);
            }
        }
    }
}
