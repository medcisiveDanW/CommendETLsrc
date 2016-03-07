package com.medcisive.commend.database.mumps;

import java.util.HashMap;

/**
 *
 * @author vhapalchambj
 */
public class MentalHealthScorer {
    protected String scoreName;
    protected String commendName;
    protected java.util.HashMap<String,Integer> scoreMap;
    
    public MentalHealthScorer(String scoreName) {
        this.scoreName = scoreName;
        scoreMap = new java.util.HashMap();
    }
    
    public MentalHealthScorer(String scoreName, String commendName) {
        this(scoreName);
        this.commendName = commendName;
    }
    public Integer getScore(java.util.HashMap<String,String> questionChoiceId) {
        int total = 0;
        for(String key : questionChoiceId.keySet()) {
            String choice = questionChoiceId.get(key);
            Integer choiceValue = scoreMap.get(choice);
            if(choiceValue!=null) {
                total += choiceValue;
            }
            else {
                System.out.println("Error: " + scoreName + " key: " + key + " choice: " + choice + " map: " + choiceValue);
            }
        }
        return total;
    }
    public String getName() { return this.commendName; }
    public java.util.HashMap getScoreMap() { return scoreMap; }
}
