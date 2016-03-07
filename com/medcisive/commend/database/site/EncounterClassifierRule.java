package com.medcisive.commend.database.site;

import com.medcisive.commend.database.site.EncounterClassifier.EncounterClass;
import com.medcisive.commend.database.site.EncounterClassifier.EncounterType;

/**
 *
 * @author vhapalchambj
 */
abstract public class EncounterClassifierRule {
    protected static final java.util.Map<String, EncounterClassifier.EncounterType> _cptToType = new java.util.HashMap();
    protected static final java.util.Map<EncounterClassifier.EncounterType,EncounterClassifier.EncounterClass> _typeToClass = new java.util.HashMap();
    protected static final com.google.common.collect.BiMap<EncounterClassifier.EncounterClass,Integer> _classToPriority = com.google.common.collect.HashBiMap.create();

    static {
        _typeToClass.put(EncounterType.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT, EncounterClass.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT);
        _typeToClass.put(EncounterType.PSYCHOTHERAPY_60MINUTE, EncounterClass.PSYCHOTHERAPY_60MINUTE);
        _typeToClass.put(EncounterType.PSYCHOTHERAPY_90MINUTE, EncounterClass.PSYCHOTHERAPY_90MINUTE);
        //_typeToClass.put(EncounterType.PSYCHOTHERAPY, EncounterClass.INDIVIDUAL);
        _typeToClass.put(EncounterType.FAMILY_THERAPY, EncounterClass.PSYCHOTHERAPY_60MINUTE);
        _typeToClass.put(EncounterType.EVALUATION, EncounterClass.EVALUATION);
        _typeToClass.put(EncounterType.MEDICATION_MANAGEMENT, EncounterClass.MEDICATION_MANAGEMENT);
        _typeToClass.put(EncounterType.CARE_OR_SUPPORT, EncounterClass.CASE_MANAGEMENT);
        _typeToClass.put(EncounterType.ADDICTION, EncounterClass.PSYCHOTHERAPY_60MINUTE);
        _typeToClass.put(EncounterType.GROUP_THERAPY, EncounterClass.GROUP);
        _typeToClass.put(EncounterType.TELEPHONE, EncounterClass.TELEPHONE);
        _typeToClass.put(EncounterType.OTHER, EncounterClass.OTHER);
        _typeToClass.put(EncounterType.NO_CPT, EncounterClass.OTHER);
        _classToPriority.put(EncounterClass.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT, 1);
        _classToPriority.put(EncounterClass.MEDICATION_MANAGEMENT, 2);
        _classToPriority.put(EncounterClass.PSYCHOTHERAPY_90MINUTE, 3);
        _classToPriority.put(EncounterClass.PSYCHOTHERAPY_60MINUTE, 4);
        _classToPriority.put(EncounterClass.EVALUATION, 5);
        _classToPriority.put(EncounterClass.CASE_MANAGEMENT, 6);
        _classToPriority.put(EncounterClass.GROUP, 7);
        _classToPriority.put(EncounterClass.TELEPHONE, 8);  
        _classToPriority.put(EncounterClass.OTHER, 9); 
    }

    abstract public EncounterClass getHighestPriorityClass(java.util.Set<String> cptCodes);
}
