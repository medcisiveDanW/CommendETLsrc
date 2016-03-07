package com.medcisive.commend.database.mumps;

/**
 *
 * @author vhapalchambj
 */
public class AIMSScorer extends MentalHealthScorer {
    public AIMSScorer() {
        super("AIMS","AIMS");
        scoreMap.put("1", 0);
        scoreMap.put("2", 1);
        scoreMap.put("3", 2);
        scoreMap.put("4", 3);
        scoreMap.put("5", 4);

        scoreMap.put("6", 0);
        scoreMap.put("7", 1);
        scoreMap.put("8", 2);
        scoreMap.put("9", 3);
        scoreMap.put("10",4);

        scoreMap.put("11", 0);
        scoreMap.put("12", 1);
        scoreMap.put("13", 2);
        scoreMap.put("14", 3);
        scoreMap.put("15", 4);

        scoreMap.put("16", 0);
        scoreMap.put("17", 1);

        // no input!
        scoreMap.put("",0);
    }
}
