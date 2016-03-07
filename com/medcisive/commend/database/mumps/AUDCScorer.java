package com.medcisive.commend.database.mumps;

/**
 *
 * @author vhapalchambj
 */
public class AUDCScorer extends MentalHealthScorer {
    public AUDCScorer() {
        super("AUDC", "AUDC");
        scoreMap.put("217", 0);
        scoreMap.put("218", 1);
        scoreMap.put("219", 2);
        scoreMap.put("220", 3);
        scoreMap.put("221", 4);

        scoreMap.put("1333",0);
        scoreMap.put("223", 1);
        scoreMap.put("224", 2);
        scoreMap.put("225", 3);
        scoreMap.put("226", 4);

        // 217 repeated here with val 0
        scoreMap.put("227", 1);
        scoreMap.put("228", 2);
        scoreMap.put("229", 3);
        scoreMap.put("230", 4);

        scoreMap.put("1156", 0);

        // and for some reason 222!
        scoreMap.put("222", 0);
        // sometimes there are no answers!
        scoreMap.put("", 0);
        // Skipped?
        scoreMap.put("1155", 0);
    }

    /*
    @Override
    public Integer getScore(java.util.HashMap<String,String> questionChoiceId) {
        return 0;
    }
     * */
}
