package com.medcisive.commend.database.site;

import com.medcisive.utility.LogUtility;
import java.sql.ResultSet;
import java.util.*;

/**
 *
 * @author vhapalchambj
 */
public class EncounterClassifier {
    private static final java.util.Map<String,EncounterClassifierRule> _ruleMap = new java.util.HashMap();
    private static java.sql.Timestamp _latestTimestamp;
    private static final java.util.Set<String> _unknownCPTCodeSet = new java.util.HashSet();   
    
    public static int NUM_CPT_PATTERNS = 12;     
    public static final String[] EM_CODES_ARRAY = new String[]{
        "99367", "99441", "99442", "99443",
        "99211", "99212", "99213", "99214", "99215",
        "99241", "99242", "99243", "99244", "99245",
        "99201", "99202", "99203", "99204", "99205"
    };
    public static final java.util.HashSet<String> EMCodes = new java.util.HashSet(java.util.Arrays.asList(EM_CODES_ARRAY)); 
    
    public static final String[] PRESCRIBING_CODES_ARRAY = new String[]{
        "99367", "99441", "99442", "99443",
        "99211", "99212", "99213", "99214", "99215",
        "99241", "99242", "99243", "99244", "99245",
        "99201", "99202", "99203", "99204", "99205",
        "90792"
    };
    public static final java.util.HashSet<String> PrescribingCodes = new java.util.HashSet(java.util.Arrays.asList(PRESCRIBING_CODES_ARRAY));    
    
    public static final String[] INTAKE_CODES_ARRAY = new String[]{
        "90791", "90792"
    };
    public static final java.util.HashSet<String> IntakeCodes = new java.util.HashSet(java.util.Arrays.asList(INTAKE_CODES_ARRAY));    
        
    public static final String[] ADDONWITHEM_CODES_ARRAY = new String[]{
        "90833", "90836","90838"
    };
    public static final java.util.HashSet<String> AddONWithEMCodes = new java.util.HashSet(java.util.Arrays.asList(ADDONWITHEM_CODES_ARRAY));    

    public static final String[] ADDON_CODES_ARRAY = new String[]{
        "90833", "90836", "90838",
        "90785", "99354", "99355", "90840"
    };
    public static final java.util.HashSet<String> AddONCodes = new java.util.HashSet(java.util.Arrays.asList(ADDON_CODES_ARRAY));    

    
    public static final String[] CRISIS_CODES_ARRAY = new String[]{
        "90839"
    };
    public static final java.util.HashSet<String> CrisisCodes = new java.util.HashSet(java.util.Arrays.asList(CRISIS_CODES_ARRAY));    
    
    public static final String[] HIGHCOMPLEXITY_CODES_ARRAY = new String[]{
        "99215","99245","99205",
    };
    public static final java.util.HashSet<String> HighComplexityCodes = new java.util.HashSet(java.util.Arrays.asList(HIGHCOMPLEXITY_CODES_ARRAY));    

    public static final String[] MEDCOMPLEXITY_CODES_ARRAY = new String[]{
        "99214","99244","99204",
    };
    public static final java.util.HashSet<String> MedComplexityCodes = new java.util.HashSet(java.util.Arrays.asList(MEDCOMPLEXITY_CODES_ARRAY));    

    public static final String[] LOWCOMPLEXITY_CODES_ARRAY = new String[]{
        "99211","99212","99213","99241","99242","99243","99201","99202","99203"
    };
    public static final java.util.HashSet<String> LowComplexityCodes = new java.util.HashSet(java.util.Arrays.asList(LOWCOMPLEXITY_CODES_ARRAY));    
    
    public static final String[] GROUPTHERAPY_CODES_ARRAY = new String[]{
        "90853"
    };
    public static final java.util.HashSet<String> GroupTherapyCodes = new java.util.HashSet(java.util.Arrays.asList(GROUPTHERAPY_CODES_ARRAY));    
     
    public static final String[] PROLONGEDSERVICE_CODES_ARRAY = new String[]{
        "99354","99355"
    };
    public static final java.util.HashSet<String> ProlongedServiceCodes = new java.util.HashSet(java.util.Arrays.asList(PROLONGEDSERVICE_CODES_ARRAY));    
      
    public static final String[] INTERACTIVECOMPLEXITY_CODES_ARRAY = new String[]{
        "90785"
    };
    public static final java.util.HashSet<String> InteractiveComplexityCodes = new java.util.HashSet(java.util.Arrays.asList(INTERACTIVECOMPLEXITY_CODES_ARRAY));    
    
    
    
    static {
        _ruleMap.put("Historic", new EncounterClassifierRuleHistoric());
        _ruleMap.put("2013", new EncounterClassifierRule2013());
   
    }

    public enum EncounterType {
        PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT,
        PSYCHOTHERAPY_60MINUTE,
        PSYCHOTHERAPY_90MINUTE,
        //PSYCHOTHERAPY,
        FAMILY_THERAPY,
        GROUP_THERAPY,
        EVALUATION,
        MEDICATION_MANAGEMENT,
        CARE_OR_SUPPORT,
        ADDICTION,
        TELEPHONE,
        OTHER,
        NO_CPT
    }

    public enum EncounterClass {
        INDIVIDUAL,
        EVALUATION,
        PSYCHOTHERAPY_60MINUTE,
        PSYCHOTHERAPY_90MINUTE,
        PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT,
        CASE_MANAGEMENT,
        MEDICATION_MANAGEMENT,
        GROUP,
        TELEPHONE,
        OTHER,
        TOTAL
    }

    public static synchronized EncounterClass getHighestPriorityClass(ResultSet rs) throws Exception {
        java.util.Set<String> cptCodes = getCptCodesFromRS(rs);
        if(_latestTimestamp.before(java.sql.Timestamp.valueOf("2013-01-01 00:00:00.0"))) {
            return _ruleMap.get("Historic").getHighestPriorityClass(cptCodes);
        } else {
            return _ruleMap.get("2013").getHighestPriorityClass(cptCodes);
        }
    }

    public static synchronized float getWRVUTotalFromCPTSet(java.util.Set<String> cptCodes, java.util.Map<String,Double> wRVUMap) {
        if(cptCodes==null || wRVUMap==null) { return 0; }
        float result = 0;
        for(String s: cptCodes) {
            if(wRVUMap.get(s)!=null) {
                result += wRVUMap.get(s);
            } else {
                _unknownCPTCodeSet.add(s);
            }
        }
        return result;
    }
    
    public static synchronized int[] determinePatterns(java.util.Set<String> cptCodes) {
        // The array thisEncounterPattern is an array of 0 and 1.  
        // thisEncounterPattern[k] = 0, if the kth pattern is not found, 1 otherwise
        int[] thisEncounterPattern = new int[NUM_CPT_PATTERNS];
        for (int k =0 ; k<NUM_CPT_PATTERNS; k++) {
           thisEncounterPattern[k] = 0;
        }      
        if(cptCodes==null) { return thisEncounterPattern; }
        

        
        int countEM = 0;   
        int countPrescribing = 0;
        int countIntake = 0;
        int countAddONWithEM = 0;
        int countAddON = 0;
        
        int countCrisis = 0;
        int countHighComplexity = 0;
        int countMedComplexity = 0;
        int countLowComplexity = 0;
        int countGroupTherapy = 0; 
        
        int countProlongedService = 0;
        int countInteractiveComplexity = 0;
        
        StringBuffer cptCodesList = new StringBuffer();
        for (String s: cptCodes) {
           cptCodesList.append(s).append(",");
           if (EMCodes.contains(s))  { countEM++; }
           if (PrescribingCodes.contains(s)) { countPrescribing++; }
           if (IntakeCodes.contains(s)) { countIntake++; }
           if (AddONWithEMCodes.contains(s))  { countAddONWithEM++; }
           if (AddONCodes.contains(s)) { countAddON++; }
           
           if (CrisisCodes.contains(s)) { countCrisis++; }
           if (HighComplexityCodes.contains(s))  { countHighComplexity++; }
           if (MedComplexityCodes.contains(s)) { countMedComplexity++; }
           if (LowComplexityCodes.contains(s)) { countLowComplexity++; }
           if (GroupTherapyCodes.contains(s))  { countGroupTherapy++; }
           
           if (ProlongedServiceCodes.contains(s)) { countProlongedService++; }
           if (InteractiveComplexityCodes.contains(s)) { countInteractiveComplexity++; }           
        }
        //LogUtility.warn(" Input CPTCodes: " + cptCodesList.toString());
        // Having done the counts, now we are able to fill in this encounter's pattern
        // Note that the default of 0 for all patterns has already been filled in
        if (countEM > 1 ) thisEncounterPattern[0] = 1;
        if (countPrescribing > 0 ) thisEncounterPattern[1] = 1;
        if (countIntake > 0 ) thisEncounterPattern[2] = 1;
        if (countEM > 0 && countAddONWithEM > 0 ) thisEncounterPattern[3] = 1; 
        if (countAddON > 0 ) thisEncounterPattern[4] = 1; 
        
        if (countCrisis > 0 ) thisEncounterPattern[5] = 1;
        if (countHighComplexity > 0 ) thisEncounterPattern[6] = 1;
        if (countMedComplexity > 0 ) thisEncounterPattern[7] = 1;
        if (countLowComplexity > 0 ) thisEncounterPattern[8] = 1; 
        if (countGroupTherapy > 0 ) thisEncounterPattern[9] = 1;         
        
        if (countProlongedService > 0 ) thisEncounterPattern[10] = 1;
        if (countInteractiveComplexity > 0 ) thisEncounterPattern[11] = 1; 
        /*       
        LogUtility.warn(" Encounter pattern: tooManyEM "  + thisEncounterPattern[0] + 
                        " Prescribing "  + thisEncounterPattern[1] +
                        "   intake "  + thisEncounterPattern[2] +
                        " addOnWithEM "  + thisEncounterPattern[3] +
                        " addOn "  + thisEncounterPattern[4] + " \n " +
                " Crisis "  + thisEncounterPattern[5] +
                " High Complex. "  + thisEncounterPattern[6] +
                " Med "  + thisEncounterPattern[7] +
                " Low "  + thisEncounterPattern[8] +
                " Group "  + thisEncounterPattern[9] + " \n " +
                " ProlongedService "  + thisEncounterPattern[10] +
                " Interactive Complexity "  + thisEncounterPattern[11]                 
                );
        */
        return thisEncounterPattern;
    }    

    public static synchronized EncounterClass getHighestPriorityClass(java.util.Set<String> cptCodes) throws Exception {
        if(_latestTimestamp.before(java.sql.Timestamp.valueOf("2013-01-01 00:00:00.0"))) {
            return _ruleMap.get("Historic").getHighestPriorityClass(cptCodes);
        } else {
            return _ruleMap.get("2013").getHighestPriorityClass(cptCodes);
        }
    }

    public static synchronized java.util.TreeSet<String> getCptCodesFromRS(ResultSet rs) throws Exception {
        java.util.TreeSet<String> result = new java.util.TreeSet();
        String visitSid = null;
        String patientSid = null;
        java.sql.Timestamp visitDateTime = null;
        _latestTimestamp = null;
        boolean isLastRow = false;    // check if last row
        try {
            do {
                isLastRow = rs.isLast();
                if(visitSid==null && patientSid==null) {
                    // Here to handle first row of new encounter
                    visitSid = rs.getString("visitSID");
                    patientSid = rs.getString("patientSID");
                    visitDateTime = rs.getTimestamp("visitDateTime");
                } else if(!visitDateTime.equals(rs.getTimestamp("visitDateTime")) || !patientSid.equalsIgnoreCase(rs.getString("patientSID"))) {                                                            
                    rs.previous();
                    break;
                }                
                _latestTimestamp = visitDateTime;
                result.add(rs.getString("CPTCode"));
                if (isLastRow) {
                  break;
                }
            } while(rs.next());
        }
        catch(java.sql.SQLException e) { LogUtility.error(e); System.err.println("Error: getCptCodesFromRS - " + e); }
        if(_latestTimestamp==null && !result.isEmpty()) {
            throw new java.lang.Exception("EncounterClassifier.getCptCodesFromRS: Could not find a valid Timestamp.");
        }
        return result;
    }

    public static void printUnknownCPTCodeSet() {
        for(String s: _unknownCPTCodeSet) {
            System.out.println(s);
        }
    }
}