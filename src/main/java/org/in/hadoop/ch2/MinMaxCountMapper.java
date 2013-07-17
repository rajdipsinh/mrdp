package org.in.hadoop.ch2;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.in.hadoop.util.CommentTag;
import org.in.hadoop.util.MRDPUtil;

/**
 * @author traw
 *
 */
public class MinMaxCountMapper extends 
        Mapper<Object, Text, Text, MinMaxCountTuple> {
	private Text outUserId = new Text();
	private MinMaxCountTuple outTuple = new MinMaxCountTuple();
	/*
	 * temp buffer String
	 */
	private String string ;
	private Date tempDate;
	@Override
	public void map (Object key, Text value, Context context ) throws IOException, InterruptedException {
		Map<String, String> parsed = MRDPUtil.transformXmlToMap(value.toString());
		
		// Prepare UserId
		string = parsed.get(CommentTag.USERID.toString());
		if (null == string || string.isEmpty()) {
			return;
		}
		
		outUserId.set(string); 
		
		// Prepare outTuple
		string = parsed.get(CommentTag.CREATION_DATE.toString());
		try {
			tempDate = MinMaxCountTuple.DATE_FORMAT.parse(string);
			outTuple.setMin(tempDate);
			outTuple.setMax(tempDate);
			outTuple.setCount(1l);
		} catch (ParseException e) {
			e.printStackTrace();
			return;
		}		
		context.write(outUserId, outTuple);
	}
}
