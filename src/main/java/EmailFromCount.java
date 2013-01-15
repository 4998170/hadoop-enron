import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.codehaus.jackson.map.ObjectMapper;
        
public class EmailFromCount {
        
 public static class Map extends Mapper<NullWritable, Text, Text, Text> {
    public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String content = value.toString();
    	Session s = Session.getDefaultInstance(new Properties());
    	InputStream is = new ByteArrayInputStream(content.getBytes());
    	try {
			MimeMessage message = new MimeMessage(s, is);
			for (Address from : message.getFrom()) {
				for (Address to : message.getRecipients(Message.RecipientType.TO)) {
					context.write(new Text(from.toString()), new Text(to.toString()));
				}
			}
		} catch (MessagingException e) {
			// pass
		} catch (NullPointerException e) {
			// pass
		}
    }
 } 
        
 public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
    	HashMap<String, Integer> freq = new HashMap<String, Integer>();
        for (Text value : values) {
        	String recipient = value.toString();
        	int count = freq.containsKey(recipient) ? freq.get(recipient) : 0;
        	freq.put(recipient, count + 1);
        }
        
    	Put put = new Put(key.getBytes());
    	ObjectMapper mapper = new ObjectMapper();
    	String freq_json = mapper.writeValueAsString(freq);
    	put.add(Bytes.toBytes("email"), Bytes.toBytes("sent_to"), Bytes.toBytes(freq_json));
        	
    	context.write(null, put);
    }
 }
        
 public static void main(String[] args) throws Exception {
	Configuration conf = HBaseConfiguration.create();
    Job job = new Job(conf, "sent-to-count");
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Writable.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    TableMapReduceUtil.initTableReducerJob(
    		"enron",	      // output table
    		Reduce.class,     // reducer class
    		job);
        
    job.setInputFormatClass(WholeFileInputFormat.class);
    job.setOutputFormatClass(TableOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0] + "/*/*/"));
        
    job.waitForCompletion(true);
 }
        
}
