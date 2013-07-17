package org.in.hadoop.ch2;

import static org.mockito.Mockito.verify;

import static org.junit.Assert.*;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MinMaxCountMapperTest {
	
	private MinMaxCountMapper minMaxCountMapper;
	@Mock
	private Mapper<Object, Text, Text, MinMaxCountTuple>.Context context;
	private String inputRow;


	@Before
	public void setUp() throws Exception {
		minMaxCountMapper = new MinMaxCountMapper();
		inputRow = "<row Id=\"1\" PostId=\"2\" Score=\"4\"" +
				" Text=\"Sample\" " +
				"CreationDate=\"2010-08-10T20:47:19.800\" UserId=\"73\" />";
	}

	@Test
	public void testMap() throws IOException, InterruptedException, ParseException {
		
		minMaxCountMapper.map(new Object(), new Text(inputRow), context);
		
		Text userId = new Text("73");
		
		Date date = MinMaxCountTuple.DATE_FORMAT.parse("2010-08-10T20:47:19.800");
		
		MinMaxCountTuple tuple = new MinMaxCountTuple();
		tuple.setMin(date);
		tuple.setMax(date);
		tuple.setCount(1l);
		
		verify(context).write(userId, tuple);
	}

}
