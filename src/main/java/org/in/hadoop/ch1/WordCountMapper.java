package org.in.hadoop.ch1;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.in.hadoop.util.CommentTag;
import org.in.hadoop.util.MRDPUtil;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

/**
 *
 * Date: 7/15/13
 * Time: 11:59 AM
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {

        Map<String, String> parsed = MRDPUtil.transformXmlToMap(value.toString());

        String text = parsed.get(CommentTag.TEXT.toString());
        // Usecase if comment not contain text
        if (null == text) {
            return;
        }

        text = StringEscapeUtils.unescapeHtml(text.toLowerCase());
        text = text.replaceAll("'", "");
        text = text.replaceAll("[^a-zA-Z0-9\\s]", " ");

        StringTokenizer iterator = new StringTokenizer(text);
        while (iterator.hasMoreElements()) {
            word.set(iterator.nextToken());
            context.write(word, ONE);
        }
    }
}
