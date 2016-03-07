package com.medcisive.commend.database.patient.graph.encounter;

/**
 *
 * @author vhapalchambj
 */
abstract public class EncounterClassifierRule {
    protected static final java.util.Map<String, Encounter.Type> _cptToType = new java.util.HashMap();
    protected static final java.util.Map<Encounter.Type,Encounter.Class> _typeToClass = new java.util.HashMap();
    protected static final com.google.common.collect.BiMap<Encounter.Class,Integer> _classToPriority = com.google.common.collect.HashBiMap.create();

    static {
        _typeToClass.put(Encounter.Type.PSYCHOTHERAPY_60MINUTE, Encounter.Class.INDIVIDUAL);
        _typeToClass.put(Encounter.Type.PSYCHOTHERAPY_90MINUTE, Encounter.Class.INDIVIDUAL);
        _typeToClass.put(Encounter.Type.PSYCHOTHERAPY, Encounter.Class.INDIVIDUAL);
        _typeToClass.put(Encounter.Type.EVALUATION, Encounter.Class.INDIVIDUAL);
        _typeToClass.put(Encounter.Type.ADDICTION, Encounter.Class.INDIVIDUAL);
        _typeToClass.put(Encounter.Type.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT, Encounter.Class.MEDICATION);
        _typeToClass.put(Encounter.Type.MEDICATION_MANAGEMENT, Encounter.Class.MEDICATION);
        _typeToClass.put(Encounter.Type.CARE_OR_SUPPORT, Encounter.Class.CASE);
        _typeToClass.put(Encounter.Type.FAMILY_THERAPY, Encounter.Class.FAMILY);
        _typeToClass.put(Encounter.Type.GROUP_THERAPY, Encounter.Class.GROUP);
        _typeToClass.put(Encounter.Type.TELEPHONE, Encounter.Class.TELEPHONE);
        _typeToClass.put(Encounter.Type.OTHER, Encounter.Class.OTHER);
        _typeToClass.put(Encounter.Type.NO_CPT, Encounter.Class.OTHER);
        _classToPriority.put(Encounter.Class.INDIVIDUAL, 1);
        _classToPriority.put(Encounter.Class.MEDICATION, 2);
        _classToPriority.put(Encounter.Class.CASE, 3);
        _classToPriority.put(Encounter.Class.FAMILY, 4);
        _classToPriority.put(Encounter.Class.GROUP, 5);
        _classToPriority.put(Encounter.Class.TELEPHONE, 6);
        _classToPriority.put(Encounter.Class.OTHER, 7);
    }

    abstract public Encounter.Class getHighestPriorityClass(java.util.Set<String> cptCodes);
}
