package org.in.hadoop.ch2;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MinMaxCountReducerTest {

	private MinMaxCountReducer minMaxCountReducer;
	@Mock
	private Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple>.Context context;
	

	@Before
	public void setUp() throws Exception {
		minMaxCountReducer = new MinMaxCountReducer();
	}

	@Test
	public void testReduce() throws ParseException, IOException, InterruptedException {
        
		
		Text userId = new Text("73");
		
		Date date = MinMaxCountTuple.DATE_FORMAT.parse("2010-08-10T20:47:19.800");
		
		MinMaxCountTuple tuple = new MinMaxCountTuple();
		tuple.setMin(date);
		tuple.setMax(date);
		tuple.setCount(1l);
		
        date = MinMaxCountTuple.DATE_FORMAT.parse("2010-08-11T20:47:19.800");
		
		MinMaxCountTuple tuple1 = new MinMaxCountTuple();
		tuple1.setMin(date);
		tuple1.setMax(date);
		tuple1.setCount(1l);
		

        date = MinMaxCountTuple.DATE_FORMAT.parse("2010-08-12T20:47:19.800");
		
		MinMaxCountTuple tuple2 = new MinMaxCountTuple();
		tuple2.setMin(date);
		tuple2.setMax(date);
		tuple2.setCount(1l);
		
		List<MinMaxCountTuple> values = Arrays.asList(tuple, tuple1, tuple2);
		
		minMaxCountReducer.reduce(userId, values, context);
		
		MinMaxCountTuple outTuple = new MinMaxCountTuple();
		outTuple.setMin(tuple.getMin());
		outTuple.setMax(tuple2.getMax());
		outTuple.setCount(3l);
		
		verify(context).write(userId, outTuple);

	}

}
