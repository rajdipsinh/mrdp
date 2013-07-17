package org.in.hadoop.ch2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxCountReducer extends
		Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
	
	private MinMaxCountTuple result = new MinMaxCountTuple();
	@Override
	public void reduce( Text key, 
			Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException{
		result.setMin(null);
		result.setMax(null);
		result.setCount(0);
		int sum = 0;
		for (MinMaxCountTuple tuple : values) {
			if (null == result.getMin() 
					|| (tuple.getMin().compareTo(result.getMin())) < 0) {
				result.setMin(tuple.getMin());
			}
			
			if (result.getMax() == null ||
					tuple.getMax().compareTo(result.getMax()) > 0) {
					result.setMax(tuple.getMax());
			}
			
			sum += tuple.getCount();
		}
		
		result.setCount(sum);
		context.write(key, result); 
	}
}
