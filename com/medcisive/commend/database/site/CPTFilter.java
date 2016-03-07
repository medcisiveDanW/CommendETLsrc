package com.medcisive.commend.database.site;

public class CPTFilter {

    public enum Role {
        PSYCHIATRIST,PSYCHOLOGIST,SOCIALWORKER,OTHER;

        public static Role getRole(String role) {
            if(Role.PSYCHIATRIST.toString().equalsIgnoreCase(role)) {
                return Role.PSYCHIATRIST;
            } else if(Role.PSYCHOLOGIST.toString().equalsIgnoreCase(role)) {
                return Role.PSYCHOLOGIST;
            } else if(Role.SOCIALWORKER.toString().equalsIgnoreCase(role)) {
                return Role.SOCIALWORKER;
            }
            return Role.OTHER;
        }
    }

    public static final String[] PSYCHIATRIST_CODES_ARRAY = new String[]{
        "99212", "99213", "99214", "99215", "99201",
        "99202", "99203", "99204", "99205", "99241",
        "99242", "99243", "99244", "99245", "99251",
        "99252", "99253", "99254", "99255", "99221",
        "99222", "99223", "99231", "99232", "99233",
        "99238", "99218", "99219", "99220", "99224",
        "99225", "99226", "99217", "99234", "99235",
        "99236", "90791", "90792", "90833", "90836",
        "90838", "90832", "90834", "90837", "90853",
        "90846", "90847", "90875", "90876", "99354",
        "99355", "99356", "99357", "90875", "90839",
        "90840", "96150", "96151", "96152", "96153",
        "96154", "96155", "99367", "99368", "97535",
        "97532", "97537", "90887", "J1637", "H0004",
        "J1630", "J2794", "96119", "96118", "96120",
        "90899", "90889", "J2680", "90885", "96103",
        "96102", "H0038", "97533", "96125", "96372",
        "97530", "97150", "G0117", "97546", "97545"
    };
    public static final java.util.HashSet<String> _psychiatristCodeSet = new java.util.HashSet(java.util.Arrays.asList(PSYCHIATRIST_CODES_ARRAY));
    public static final String[] PSYCHOLOGIST_CODES_ARRAY =
            new String[]{"90832", "90834", "90837", "90853", "90846",
        "90847", "90875", "90876", "90875", "90839",
        "90840", "96150", "96151", "96152", "96153",
        "96154", "96155", "99367", "99368", "97535",
        "97532", "97537", "90887", "96119", "96118",
        "96120", "90899", "90889", "90885", "96103",
        "96102", "H0046", "H0038", "97533", "96125",
        "H0004", "Q3014", "97530", "97150", "G0177",
        "97546", "97545"
    };
    public static final java.util.HashSet<String> _psychologistCodeSet = new java.util.HashSet(java.util.Arrays.asList(PSYCHOLOGIST_CODES_ARRAY));
    public static final String[] SOCIALWORKER_CODES_ARRAY =
            new String[]{"T1016", "97532", "97537", "90882", "99420",
        "99366", "99368", "G0175", "99499", "99499",
        "G0177", "S9446", "S9445", "98960", "98961",
        "98962", "99420", "96150", "96151", "96152",
        "96153", "96154", "96155", "G0155", "99509",
        "99510", "S9127", "A0160", "G0176", "S0257",
        "S0250", "S0255", "H0004", "90899", "H0046",
        "H0031", "97533", "96125", "Q3014", "97546",
        "97545", "90791", "90832", "90834", "90837",
        "99354", "99355", "99356", "99357", "90839",
        "90840", "90846", "90847", "90849", "90849",
        "90853", "90887", "S9453"
    };
    public static final java.util.HashSet _socialworkerCodeSet = new java.util.HashSet(java.util.Arrays.asList(SOCIALWORKER_CODES_ARRAY));
    // EMCodesOrderMap is a mapping of Evaluation and Management (E&M) codes to their order of
    // priority as expressed as an integer.
    // Only psychiatrists are allowed to use E&M codes, and for any encounter, only
    // 1 E&M code can be used, the one with the highest priority
    public static final java.util.HashMap<String, Integer> _emCodeOrderMap = new java.util.HashMap();

    static {

        _emCodeOrderMap.put("99205", new Integer(140));
        _emCodeOrderMap.put("99204", new Integer(130));
        _emCodeOrderMap.put("99203", new Integer(120));
        _emCodeOrderMap.put("99202", new Integer(110));
        _emCodeOrderMap.put("99201", new Integer(100));

        _emCodeOrderMap.put("99245", new Integer(90));
        _emCodeOrderMap.put("99244", new Integer(80));
        _emCodeOrderMap.put("99243", new Integer(70));
        _emCodeOrderMap.put("99242", new Integer(60));
        _emCodeOrderMap.put("99241", new Integer(50));

        _emCodeOrderMap.put("99215", new Integer(40));
        _emCodeOrderMap.put("99214", new Integer(30));
        _emCodeOrderMap.put("99213", new Integer(20));
        _emCodeOrderMap.put("99212", new Integer(10));
    }

    /*
     * Given the 2 inputs, the role of the provider, and a list of CPT codes from a given
     * encounter, the method getAllowedCodeList returns a list of allowed CPT codes for the
     * the role specified for that encounter.
     *
     * 3 roles are allowed: Psychiatrist, Psychologist and Social Worker.
     * The corresponding HashSet's: PSYCHIATRIST_CODES, PSYCHOLOGIST_CODES, and SOCIALWORKER_CODES contain
     * the set of allowed codes for each role
     *
     */
    public static java.util.Set<String> getAllowedCodeList(String roleString, java.util.Set<String> cptSet) throws java.lang.IllegalArgumentException {
        if (roleString == null || cptSet == null || cptSet.isEmpty()) {
            throw new java.lang.IllegalArgumentException("CPTFilter.getAllowedCodeList: Role(" + roleString + ") CPTset(" + cptSet + ")");
        }
        java.util.HashSet<String> referenceCodesToUse = new java.util.HashSet();
        Role role = Role.getRole(roleString);
        if (role == Role.PSYCHIATRIST) {
            referenceCodesToUse = _psychiatristCodeSet;
        } else if (role == Role.PSYCHOLOGIST) {
            referenceCodesToUse = _psychologistCodeSet;
        } else if (role == Role.SOCIALWORKER) {
            referenceCodesToUse = _socialworkerCodeSet;
        }
        java.util.Set<String> result = new java.util.HashSet();
        for (String cpt : cptSet) {
            if (referenceCodesToUse.contains(cpt)) {
                result.add(cpt);
            }
        }
        if (role == Role.PSYCHIATRIST) {
            result = checkEMCodes(result);
        }
        return result;
    }

    public static java.util.Set<String> checkEMCodes(java.util.Set<String> cptSet) {
        if (cptSet == null || cptSet.size() <= 0) {
            return cptSet;
        }
        boolean foundEMCode = false;
        int maxPriority = -1000;    // default negative priority, lower than any found in EMCodesOrderMap
        String maxPriorityCode = "";
        java.util.Set<String> result = new java.util.HashSet();
        for (String cpt : cptSet) {
            Integer newPriority = _emCodeOrderMap.get(cpt);
            if (newPriority == null) {
                result.add(cpt);
                continue;
            } else {
                foundEMCode = true;
                if (newPriority.intValue() > maxPriority) {
                    maxPriority = newPriority.intValue();
                    maxPriorityCode = cpt;
                }
            }
        }
        if (foundEMCode) {
            result.add(maxPriorityCode);
        }
        return result;
    }
}
