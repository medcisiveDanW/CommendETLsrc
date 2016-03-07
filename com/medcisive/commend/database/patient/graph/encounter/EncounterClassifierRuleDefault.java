package com.medcisive.commend.database.patient.graph.encounter;

import java.util.Set;

/**
 *
 * @author vhapalchambj
 */
public class EncounterClassifierRuleDefault extends EncounterClassifierRule {
    static {
        _cptToType.put("90801", Encounter.Type.EVALUATION);
        _cptToType.put("90791", Encounter.Type.EVALUATION);
        _cptToType.put("90792", Encounter.Type.EVALUATION);
        _cptToType.put("99201", Encounter.Type.EVALUATION);
        _cptToType.put("99202", Encounter.Type.EVALUATION);
        _cptToType.put("99203", Encounter.Type.EVALUATION);
        _cptToType.put("99204", Encounter.Type.EVALUATION);
        _cptToType.put("99205", Encounter.Type.EVALUATION);
        _cptToType.put("90806", Encounter.Type.PSYCHOTHERAPY);
        _cptToType.put("90808", Encounter.Type.PSYCHOTHERAPY);
        _cptToType.put("90818", Encounter.Type.PSYCHOTHERAPY);
        _cptToType.put("90819", Encounter.Type.PSYCHOTHERAPY);
        _cptToType.put("90821", Encounter.Type.PSYCHOTHERAPY);
        _cptToType.put("90822", Encounter.Type.PSYCHOTHERAPY);
        _cptToType.put("90834", Encounter.Type.PSYCHOTHERAPY);
        _cptToType.put("90837", Encounter.Type.PSYCHOTHERAPY);
        _cptToType.put("90807", Encounter.Type.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT);
        _cptToType.put("90809", Encounter.Type.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT);
        _cptToType.put("90833", Encounter.Type.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT);
        _cptToType.put("90836", Encounter.Type.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT);
        _cptToType.put("90838", Encounter.Type.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT);
        _cptToType.put("90853", Encounter.Type.GROUP_THERAPY);
        _cptToType.put("90847", Encounter.Type.FAMILY_THERAPY);
        _cptToType.put("90862", Encounter.Type.MEDICATION_MANAGEMENT);
        _cptToType.put("M0064", Encounter.Type.MEDICATION_MANAGEMENT);
        _cptToType.put("90805", Encounter.Type.MEDICATION_MANAGEMENT);
        _cptToType.put("99212", Encounter.Type.MEDICATION_MANAGEMENT);
        _cptToType.put("99213", Encounter.Type.MEDICATION_MANAGEMENT);
        _cptToType.put("99214", Encounter.Type.MEDICATION_MANAGEMENT);
        _cptToType.put("99215", Encounter.Type.MEDICATION_MANAGEMENT);
        _cptToType.put("90815", Encounter.Type.CARE_OR_SUPPORT);
        _cptToType.put("90804", Encounter.Type.CARE_OR_SUPPORT);
        _cptToType.put("T1016", Encounter.Type.CARE_OR_SUPPORT);
        _cptToType.put("90832", Encounter.Type.CARE_OR_SUPPORT);
        _cptToType.put("90839", Encounter.Type.CARE_OR_SUPPORT);
        _cptToType.put("H0005", Encounter.Type.ADDICTION);
        _cptToType.put("99441", Encounter.Type.TELEPHONE);
        _cptToType.put("99442", Encounter.Type.TELEPHONE);
        _cptToType.put("99443", Encounter.Type.TELEPHONE);
        _cptToType.put("98966", Encounter.Type.TELEPHONE);
        _cptToType.put("98967", Encounter.Type.TELEPHONE);
        _cptToType.put("98968", Encounter.Type.TELEPHONE);
    }

    private static java.util.Set<Encounter.Type> getEncounterTypes(java.util.Set<String> cptCodes) {
        java.util.Set<Encounter.Type> result = new java.util.HashSet();
        for (String s : cptCodes) {
            Encounter.Type et = _cptToType.get(s);
            if(et!=null) { result.add(et); }
            else { result.add(Encounter.Type.OTHER); }
        }
        if (result.isEmpty()) {
            result.add(Encounter.Type.NO_CPT);
        }
        return result;
    }

    private static java.util.Set<Encounter.Class> getEncounterClasses(java.util.Set<Encounter.Type> typeSet) {
        java.util.Set<Encounter.Class> result = new java.util.HashSet();
        for (Encounter.Type t : typeSet) {
            result.add(_typeToClass.get(t));
        }
        if (result.isEmpty()) {
            result.add(Encounter.Class.OTHER);
        }
        return result;
    }

    @Override
    public Encounter.Class getHighestPriorityClass(Set<String> cptCodes) {
        java.util.Set<Encounter.Class> set = getEncounterClasses(getEncounterTypes(cptCodes));
        int result = 7;
        for(Encounter.Class e : set) {
            int current = _classToPriority.get(e);
            if(result>current) { result = current; }
        }
        return _classToPriority.inverse().get(result);
    }
}
