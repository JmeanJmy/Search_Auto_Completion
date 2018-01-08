import oracle.jrockit.jfr.StringConstantPool;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;

		@Override
		public void setup(Context context) {
			// how to get the threashold parameter from the configuration?
			threashold = context.getConfiguration().getInt("threashold", 10);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {

				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			//how to filter the n-gram lower than threashold

			if (count < threashold) {
				return;
			}

			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < words.length - 1; i++){
				sb.append(words[i] + " ");
			}

			String outputKey = sb.toString().trim();
			String outputValue = words[words.length - 1] + "=" + count;
			context.write(new Text(outputKey), new Text(outputValue));
			
			//this is --> cool = 20

			//what is the outputkey?
			//what is the outputvalue?
			
			//write key-value to reducer?
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
		//collect from mapper
		//topK from value list
		//write to mysql
		//value list = <big, data, girl, boy...>
		int topK;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


			TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
			for (Text value : values){
				String curValue = value.toString().trim();
				String word = curValue.split("=")[0];
				int count = Integer.parseInt(curValue.split("=")[1]);

				if (tm.containsKey(count)){
					 tm.get(count).add(word);
				}else{
					List<String> list = new ArrayList<String>();
					list.add(word);
					tm.put(count, list);
				}
			}

			Iterator<Integer> iter = tm.keySet().iterator();
			for (int j = 0; iter.hasNext() && j < topK; ){
				int keyCount = iter.next();
				List<String> words = tm.get(keyCount);
				for (String curword : words){
					context.write(new DBOutputWritable(key.toString(), curword, keyCount), NullWritable.get());
					j++;
				}
			}
		}
	}
}
