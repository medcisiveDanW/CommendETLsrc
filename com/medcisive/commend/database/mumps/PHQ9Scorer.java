package com.medcisive.commend.database.mumps;

/**
 *
 * @author vhapalchambj
 */
public class PHQ9Scorer extends MentalHealthScorer {
    public PHQ9Scorer() {
        super("PHQ9","PHQ-9");
        scoreMap.put("212", 0);
        scoreMap.put("1008",1);
        scoreMap.put("1009",2);
        scoreMap.put("1010",3);
        scoreMap.put("1334",0);
        scoreMap.put("1335",1);
        scoreMap.put("1336",2);
        scoreMap.put("1337",3);
        
        // no input!
        scoreMap.put("",0);
    }
    @Override
    public Integer getScore(java.util.HashMap<String,String> questionChoiceId) {
        int total = 0;
        for(String key : questionChoiceId.keySet()) {
            String choice = questionChoiceId.get(key);
            Integer choiceValue = scoreMap.get(choice);
            if(choiceValue!=null) {
                if(!key.equalsIgnoreCase("4019")) {
                    total += choiceValue;
                }
            }
            else {
                System.out.println("Error: " + scoreName + " key: " + key + " choice: " + choice + " map: " + choiceValue);
            }
        }
        return total;
    }
}
