package com.medcisive.commend.database.site;

import com.medcisive.commend.database.site.EncounterClassifier.EncounterClass;
import com.medcisive.commend.database.site.EncounterClassifier.EncounterType;
import java.util.Set;

/**
 *
 * @author vhapalchambj
 */
public class EncounterClassifierRule2013 extends EncounterClassifierRule {
    private static final java.util.Set<String> _psychotherapyAndMMSet = new java.util.HashSet();
    private static final String EXTENDED_SERVICE = "99354";
    static {
        //_cptToType.put("90801", EncounterType.EVALUATION);
        _cptToType.put("90791", EncounterType.EVALUATION);
        _cptToType.put("90792", EncounterType.EVALUATION);
        _cptToType.put("99201", EncounterType.EVALUATION);
        _cptToType.put("99202", EncounterType.EVALUATION);
        _cptToType.put("99203", EncounterType.EVALUATION);
        _cptToType.put("99204", EncounterType.EVALUATION);
        _cptToType.put("99205", EncounterType.EVALUATION);
        //_cptToType.put("90806", EncounterType.PSYCHOTHERAPY);
        //_cptToType.put("90808", EncounterType.PSYCHOTHERAPY);
        //_cptToType.put("90818", EncounterType.PSYCHOTHERAPY);
        //_cptToType.put("90819", EncounterType.PSYCHOTHERAPY);
        //_cptToType.put("90821", EncounterType.PSYCHOTHERAPY);
        //_cptToType.put("90822", EncounterType.PSYCHOTHERAPY);
        _cptToType.put("90834", EncounterType.PSYCHOTHERAPY_60MINUTE);
        _cptToType.put("90837", EncounterType.PSYCHOTHERAPY_60MINUTE);
        //_cptToType.put("90807", EncounterType.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT);
        //_cptToType.put("90809", EncounterType.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT);
        _cptToType.put("90833", EncounterType.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT);
        _cptToType.put("90836", EncounterType.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT);
        _cptToType.put("90838", EncounterType.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT);
        _cptToType.put("90853", EncounterType.GROUP_THERAPY);
        _cptToType.put("90847", EncounterType.FAMILY_THERAPY);
        //_cptToType.put("90862", EncounterType.MEDICATION_MANAGEMENT);
        //_cptToType.put("M0064", EncounterType.MEDICATION_MANAGEMENT);
        //_cptToType.put("90805", EncounterType.MEDICATION_MANAGEMENT);
        _cptToType.put("99212", EncounterType.MEDICATION_MANAGEMENT);
        _cptToType.put("99213", EncounterType.MEDICATION_MANAGEMENT);
        _cptToType.put("99214", EncounterType.MEDICATION_MANAGEMENT);
        _cptToType.put("99215", EncounterType.MEDICATION_MANAGEMENT);
        //_cptToType.put("90815", EncounterType.CARE_OR_SUPPORT);
        //_cptToType.put("90804", EncounterType.CARE_OR_SUPPORT);
        _cptToType.put("T1016", EncounterType.CARE_OR_SUPPORT);
        _cptToType.put("90832", EncounterType.CARE_OR_SUPPORT);
        _cptToType.put("90839", EncounterType.CARE_OR_SUPPORT);
        _cptToType.put("H0005", EncounterType.ADDICTION);
        _cptToType.put("99441", EncounterType.TELEPHONE);
        _cptToType.put("99442", EncounterType.TELEPHONE);
        _cptToType.put("99443", EncounterType.TELEPHONE);
        _cptToType.put("98966", EncounterType.TELEPHONE);
        _cptToType.put("98967", EncounterType.TELEPHONE);
        _cptToType.put("98968", EncounterType.TELEPHONE);
        _psychotherapyAndMMSet.add("90833");
        _psychotherapyAndMMSet.add("90836");
        _psychotherapyAndMMSet.add("90838");
    }

    private static java.util.Set<EncounterType> getEncounterTypes(java.util.Set<String> cptCodes) {
        java.util.Set<EncounterType> result = new java.util.HashSet();
        if (cptCodes == null || cptCodes.size() <= 0 ) {
           result.add(EncounterType.NO_CPT);    
           return result;
        }
                
        // Rule for identifying PSYCHOTHERAPY_90MINUTE
        if (cptCodes.contains(EXTENDED_SERVICE) && cptCodes.contains("90837")) {
           result.add(EncounterType.PSYCHOTHERAPY_90MINUTE);
        }
        
        for (String s : cptCodes) {
           EncounterType et = _cptToType.get(s);
           if(et!=null) { 
              result.add(et);
           }
           else { 
             result.add(EncounterType.OTHER); 
           }
        }
        
        /*boolean medicationManagement = cptCodes.contains("99212") || cptCodes.contains("99213") || cptCodes.contains("99214") || cptCodes.contains("99215");
        boolean psychotherapyAndMM = cptCodes.contains("90833") || cptCodes.contains("90836") || cptCodes.contains("90838");
        cptCodes.removeAll(_psychotherapyAndMMSet);
        
        if(medicationManagement && psychotherapyAndMM) {
            result.add(EncounterType.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT);
        } else if(cptCodes.contains("90832") && (cptCodes.size()==1)) {
            result.add(EncounterType.CARE_OR_SUPPORT);
        } else {
            for (String s : cptCodes) {
                EncounterType et = _cptToType.get(s);
                if(et!=null) { result.add(et); }
                else { result.add(EncounterType.OTHER); }
            }
        }*/
        return result;
    }

    private static java.util.Set<EncounterClass> getEncounterClasses(java.util.Set<EncounterType> typeSet) {
        java.util.Set<EncounterClass> result = new java.util.HashSet();
        for (EncounterType t : typeSet) {
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
        int result = 9;
        for(EncounterClass e : set) {
            int current = _classToPriority.get(e);
            if(result>current) { result = current; }
        }
        return _classToPriority.inverse().get(result);
    }
}
