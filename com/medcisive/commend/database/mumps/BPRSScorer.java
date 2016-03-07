package com.medcisive.commend.database.mumps;

/**
 *
 * @author vhapalchambj
 */
public class BPRSScorer extends MentalHealthScorer {
    public BPRSScorer() {
        super("BPRS","BPRS");
        scoreMap.put("614", 1);
        scoreMap.put("615", 2);
        scoreMap.put("616", 3);
        scoreMap.put("617", 4);
        scoreMap.put("618", 5);
        scoreMap.put("619", 6);
        scoreMap.put("620", 7);
    }
}
