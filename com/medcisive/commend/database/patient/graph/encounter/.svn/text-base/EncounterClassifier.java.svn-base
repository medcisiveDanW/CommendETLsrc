package com.medcisive.commend.database.patient.graph.encounter;

/**
 *
 * @author vhapalchambj
 */
public class EncounterClassifier {
    private static final java.util.Map<String,EncounterClassifierRule> _ruleMap = new java.util.HashMap();

    static {
        _ruleMap.put("default", new EncounterClassifierRuleDefault());
    }

    public static synchronized Encounter.Class getHighestPriorityClass(java.util.Set<String> cptCodes) throws Exception {
        return _ruleMap.get("default").getHighestPriorityClass(cptCodes);
    }
}