package MapJoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapJoin {

	/**
	 * @param args
	 */
	private static Map<String,String> mp=new HashMap<String, String>();
	public static class JoinMapper extends Mapper<LongWritable,Text,MoveRate,NullWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String splits[]=value.toString().split("::");
			String moveid=splits[1];
			int rate =Integer.parseInt(splits[2]);
			long ts=Long.parseLong(splits[3]);
			String userid=splits[0];
			String li[]=mp.get(moveid).split("::");
			String movetype=li[0];
			String movename=li[1];
			MoveRate n=new MoveRate(moveid, userid, rate, movename, movetype, ts);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
		
			URI i[]=context.getCacheFiles();
			String path=i[0].getPath();
			BufferedReader bf=new BufferedReader(new FileReader(path));
			String aString=bf.readLine();
			while(aString!=null){
                String lls[]=aString.split("::");				
				String mid=lls[0];
                String mname=lls[1];
                String mtype=lls[2];
				mp.put(mid, mtype+"::"+mname);
				aString=bf.readLine();
			}
			
			
		}
		
		
	}
	public static class ReduceJoin extends Reducer<MoveRate,NullWritable,MoveRate,NullWritable>{
		   @Override
		protected void reduce(MoveRate arg0, Iterable<NullWritable> arg1,
				Context con)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		      con.write(arg0, NullWritable.get());
		}
		
	}
	public static void main(String[] args) throws IOException, URISyntaxException {
		// TODO Auto-generated method stub
        Configuration conf=new Configuration();
        Job job=Job.getInstance();
        
        URI uri=new URI("file:///user/input/2.txt");
        job.addCacheFile(uri);
        
        job.setJarByClass(MapJoin.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(ReduceJoin.class);
        job.setOutputKeyClass(MoveRate.class);
        job.setOutputValueClass(NullWritable.class);
        
//        DistributedCache.addCacheFile(uri, conf);
        FileInputFormat.addInputPath(job, new Path("file:///user/input/1.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs:///user/root/input"));
	}

}
