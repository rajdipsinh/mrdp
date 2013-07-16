package org.in.hadoop.util;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.in.hadoop.util.CommentTag;
import org.in.hadoop.util.MRDPUtil;
import org.junit.Test;


public class MRDPUtilTest {

	@Test
	public void testTransformXmlToMap() {
		
		String inputRow = "<row Id=\"1\" PostId=\"2\" Score=\"4\"" +
				" Text=\"Sample Test\" " +
				"CreationDate=\"2010-08-10T20:47:19.800\" UserId=\"73\" />";
		
		Map<String, String> mockOutput = new HashMap<String, String>();
		mockOutput.put(CommentTag.ID.toString(), "1");
		mockOutput.put(CommentTag.POSTID.toString(), "2");
		mockOutput.put(CommentTag.SCORE.toString(), "4");
		mockOutput.put(CommentTag.TEXT.toString(), "Sample Test");
		mockOutput.put(CommentTag.CREATION_DATE.toString(), "2010-08-10T20:47:19.800");
		mockOutput.put(CommentTag.USERID.toString(), "73");
		
		assertEquals(mockOutput, MRDPUtil.transformXmlToMap(inputRow));		
	}

}
