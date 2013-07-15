package org.in.hadoop.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 7/15/13
 * Time: 11:47 AM
 */
public class MRDPUtil {

    public static Map<String, String> transformXmlToMap(String xmlInput) {
        Map<String, String> commentMap = new HashMap<String, String>();

        String input = xmlInput.trim();
        input = input.substring(5, input.length() - 3);
        String[] tokens = input.split("\"");

        for (int i = 0; i < tokens.length -1; i += 2) {
            String key = tokens[i].trim();
            String value = tokens[i+1].trim();
            key = key.substring(0, key.length() - 1);
            commentMap.put(key, value);
        }
        return commentMap;
    }

}
