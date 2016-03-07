package com.medcisive.commend.database.site;

import com.medcisive.commend.database.site.EncounterClassifier.EncounterClass;
import com.medcisive.commend.database.site.EncounterClassifier.EncounterType;
import java.util.Set;

/**
 *
 * @author vhapalchambj
 */
public class EncounterClassifierRuleHistoric extends EncounterClassifierRule {
    static {
        _cptToType.put("90807", EncounterType.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT);
        _cptToType.put("90806", EncounterType.PSYCHOTHERAPY_60MINUTE);
        _cptToType.put("90818", EncounterType.PSYCHOTHERAPY_60MINUTE);
        _cptToType.put("90819", EncounterType.PSYCHOTHERAPY_60MINUTE);
        _cptToType.put("90808", EncounterType.PSYCHOTHERAPY_90MINUTE);
        _cptToType.put("90809", EncounterType.PSYCHOTHERAPY_90MINUTE); // should be MM
        _cptToType.put("90821", EncounterType.PSYCHOTHERAPY_90MINUTE);
        _cptToType.put("90822", EncounterType.PSYCHOTHERAPY_90MINUTE);
        _cptToType.put("90847", EncounterType.FAMILY_THERAPY);
        _cptToType.put("90853", EncounterType.GROUP_THERAPY);
        _cptToType.put("90801", EncounterType.EVALUATION);
        _cptToType.put("90862", EncounterType.MEDICATION_MANAGEMENT);
        _cptToType.put("M0064", EncounterType.MEDICATION_MANAGEMENT);
        _cptToType.put("90805", EncounterType.MEDICATION_MANAGEMENT);
        _cptToType.put("90815", EncounterType.CARE_OR_SUPPORT);
        _cptToType.put("90804", EncounterType.CARE_OR_SUPPORT);
        _cptToType.put("T1016", EncounterType.CARE_OR_SUPPORT);
        _cptToType.put("H0005", EncounterType.ADDICTION);
        _cptToType.put("99441", EncounterType.TELEPHONE);
        _cptToType.put("99442", EncounterType.TELEPHONE);
        _cptToType.put("99443", EncounterType.TELEPHONE);
        _cptToType.put("98966", EncounterType.TELEPHONE);
        _cptToType.put("98967", EncounterType.TELEPHONE);
        _cptToType.put("98968", EncounterType.TELEPHONE);
    }

    private static java.util.Set<EncounterType> getEncounterTypes(java.util.Set<String> cptCodes) {
        java.util.Set<EncounterType> result = new java.util.HashSet();
        for (String s : cptCodes) {
            EncounterType et = _cptToType.get(s);
            if(et!=null) { result.add(et); }
            else { result.add(EncounterType.OTHER); }
        }
        if (result.isEmpty()) {
            result.add(EncounterType.NO_CPT);
        }
        return result;
    }

    private static java.util.Set<EncounterClass> getEncounterClasses(java.util.Set<EncounterType> cptCodes) {
        java.util.Set<EncounterClass> result = new java.util.HashSet();
        for (EncounterType t : cptCodes) {
            result.add(_typeToClass.get(t));
        }
        if (result.isEmpty()) {
            result.add(EncounterClass.OTHER);
        }
        return result;
    }

    @Override
    public EncounterClass getHighestPriorityClass(Set<String> cptCodes) {
        java.util.Set<EncounterClass> set = getEncounterClasses(getEncounterTypes(cptCodes));
        int result = 4;
        for(EncounterClass e : set) {
            int current = _classToPriority.get(e);
            if(result>current) { result = current; }
        }
        return _classToPriority.inverse().get(result);
    }
}
