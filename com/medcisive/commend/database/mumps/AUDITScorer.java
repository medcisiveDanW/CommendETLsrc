package com.medcisive.commend.database.mumps;

/**
 *
 * @author vhapalchambj
 */
public class AUDITScorer extends MentalHealthScorer {
    public AUDITScorer() {
        super("AUDIT","AUDIT");
        scoreMap.put("217", 0);
        scoreMap.put("218", 1);
        scoreMap.put("219", 2);
        scoreMap.put("220", 3);
        scoreMap.put("221", 4);

        
        scoreMap.put("223", 1);
        scoreMap.put("224", 2);
        scoreMap.put("225", 3);
        scoreMap.put("226", 4);

        scoreMap.put("227", 1);
        scoreMap.put("228", 2);
        scoreMap.put("229", 3);
        scoreMap.put("230", 4);

        
        scoreMap.put("232", 0);
        scoreMap.put("233", 1);
        scoreMap.put("234", 2);
        scoreMap.put("235", 3);
        scoreMap.put("236", 4);

        
        scoreMap.put("237", 0);
        scoreMap.put("238", 2);
        scoreMap.put("239", 4);

        // Just in case
        scoreMap.put("222", 1);
        scoreMap.put("231", 1);
    }
}
