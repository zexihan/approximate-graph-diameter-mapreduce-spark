package agd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

//import org.apache.log4j.Logger;

public class MSSPMapper
    extends Mapper<Object, Text, Text, Text>{

    //private Logger logger = Logger.getLogger(this.getClass());
    
    private static final String NULL_NODE = "null";

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
            
        Configuration conf = context.getConfiguration();
        String source1 = conf.get("source1");
        String source2 = conf.get("source2");
        String line = value.toString();
        String[] nodes = line.split("\\s+");
        if (nodes.length < 2) {
            return;
        }
        
        String from = nodes[0];
        Integer distance = Integer.MAX_VALUE;
        Integer distance1 = Integer.MAX_VALUE;
        Integer distance2 = Integer.MAX_VALUE;
        String zero = Integer.toString(0);
        String inf = Integer.toString(Integer.MAX_VALUE);
        
        Text outKey = new Text(from);
        Text outValue = new Text();
        Text outKey1 = new Text(from + "to" + source1);
        Text outValue1 = new Text();
        Text outKey2 = new Text(from + "to" + source2);
        Text outValue2 = new Text();
        
        if (nodes.length == 2) {
            // First iteration, input is the adjacency list
            if (from.equals(source1)) {
                distance1 = 0;
                outValue1.set(String.join(" ", nodes[1], zero));
                // NODE ADJACENCY-LIST DISTANCE
                context.write(outKey1, outValue1);       
                
            } else if (from.equals(source2)) {
            	distance2 = 0;
                outValue2.set(String.join(" ", nodes[1], zero));
                // NODE ADJACENCY-LIST DISTANCE
                context.write(outKey2, outValue2);
                
            } else {
            	outValue.set(String.join(" ", nodes[1], inf));
            	// NODE ADJACENCY-LIST DISTANCE
            	context.write(outKey1, outValue);
            	context.write(outKey2, outValue);
            	
            }
            
            for (String neighbour : nodes[1].split(",")) {
                if (neighbour.equals(NULL_NODE)) {
                    return;
                }
                outKey1.set(neighbour + "to" + source1);
                outKey2.set(neighbour + "to" + source2);
                
                Integer newDistance1 = distance1;
                Integer newDistance2 = distance2;
                if (newDistance1 != Integer.MAX_VALUE) {
                    newDistance1 += 1;
                
                    outValue1.set(Integer.toString(newDistance1));
                    // NODE DISTANCE
                    context.write(outKey1, outValue1);
                }
                if (newDistance2 != Integer.MAX_VALUE) {
                    newDistance2 += 1;
                
                    outValue2.set(Integer.toString(newDistance2));
                    // NODE DISTANCE
                    context.write(outKey2, outValue2);
                }
            }
            
        } else {
            // Since second iteration, input is the reducer's output
        	distance = Integer.parseInt(nodes[2]);
        	outValue.set(String.join(" ", nodes[1], nodes[2]));
        	// NODE ADJACENCY-LIST DISTANCE
        	context.write(outKey, outValue);
        	
        	for (String neighbour : nodes[1].split(",")) {
                if (neighbour.equals(NULL_NODE)) {
                    return;
                }
                String source = from.split("to")[1];
                outKey.set(neighbour + "to" + source);
                
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
}
