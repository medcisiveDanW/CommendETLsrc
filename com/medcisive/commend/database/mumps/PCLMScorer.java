package com.medcisive.commend.database.mumps;

/**
 *
 * @author vhapalchambj
 */
public class PCLMScorer extends MentalHealthScorer {
    public PCLMScorer() {
        super("PCLM","PCL-M");
        scoreMap.put("716", 1);
        scoreMap.put("1007",2);
        scoreMap.put("718", 3);
        scoreMap.put("719", 4);
        scoreMap.put("720", 5);

        // Skipped?
        scoreMap.put("1155", 0);
    }
}
