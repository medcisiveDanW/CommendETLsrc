/*
 * This class is used to determine the procedure type for an encounter
 * based on the CPT codes. There are only 12 possible procedure types. Each
 * procedure type is assigned a priority.  When there are multiple The highest priority procedure 
 * procedure types, the one with the highest priority is chosen.
 * 
 */
package com.medcisive.commend.database.site;

import java.util.*;
/**
 *
 * @author vhapalwangd
 */
public class ProcTypeCalculator {
    
    
    // List of possible therapies in order of precedence
    // established 20120417
    public static String PSYRAPY_WITH_MED_MGMT = "PsyWithMM";
    public static String PSYRAPY_60MIN = "Psy60min";
    public static String PSYRAPY_90MIN = "Psy90min";
    public static String FAM_THERAPY = "FamThrpy";
    public static String GROUP_THERAPY = "GrpThrpy";
    
    public static String EVALUATION = "Eval";
    public static String MED_MGMT = "MM";
    public static String CARE_OR_SUPPORT = "CareSupp";
    public static String ADDICTION = "Addictn";
    public static String TELEPHONE = "Telephn";
   
    public static String OTHER = "OtherPrc";
    public static String NO_CPT = "NoCPT";
    
    // Classification of encounters into 4 classes based on CPT codes
    public static String ENC_CLASS_INDIVIDUAL = "Indiv";
    public static String ENC_CLASS_GROUP = "Group";
    public static String ENC_CLASS_TELEPHONE = "Tele";
    public static String ENC_CLASS_OTHER = "Other";
    
    
    public static HashMap<String,String> CPT_TO_PROCTYPE_MAP = new HashMap<String,String>();
    public static HashMap<String,String> PROCTYPE_TO_ENCCLASS_MAP = new HashMap<String,String>();    
    public static HashMap<String,Integer> CPT_PRIORITY = new HashMap<String,Integer>();
                    
    static {        
       // set up the HashMap which serves as a look up for the ProcedureType based on 
       // the CPT code supplied
       CPT_TO_PROCTYPE_MAP.put("90807",PSYRAPY_WITH_MED_MGMT);
       
       CPT_TO_PROCTYPE_MAP.put("90806",PSYRAPY_60MIN);       
       CPT_TO_PROCTYPE_MAP.put("90818",PSYRAPY_60MIN);       
       CPT_TO_PROCTYPE_MAP.put("90819",PSYRAPY_60MIN); 
       
       CPT_TO_PROCTYPE_MAP.put("90808",PSYRAPY_90MIN);       
       CPT_TO_PROCTYPE_MAP.put("90809",PSYRAPY_90MIN);       
       CPT_TO_PROCTYPE_MAP.put("90821",PSYRAPY_90MIN);         
       CPT_TO_PROCTYPE_MAP.put("90822",PSYRAPY_90MIN);
       
       CPT_TO_PROCTYPE_MAP.put("90847",FAM_THERAPY);       
       CPT_TO_PROCTYPE_MAP.put("90853",GROUP_THERAPY);       
       CPT_TO_PROCTYPE_MAP.put("90801",EVALUATION); 
       
       CPT_TO_PROCTYPE_MAP.put("90862",MED_MGMT);       
       CPT_TO_PROCTYPE_MAP.put("M0064",MED_MGMT);       
       CPT_TO_PROCTYPE_MAP.put("90805",MED_MGMT);        

       CPT_TO_PROCTYPE_MAP.put("90815",CARE_OR_SUPPORT);       
       CPT_TO_PROCTYPE_MAP.put("90804",CARE_OR_SUPPORT);       
       CPT_TO_PROCTYPE_MAP.put("T1016",CARE_OR_SUPPORT);
       
       CPT_TO_PROCTYPE_MAP.put("H0005",ADDICTION);
       
       CPT_TO_PROCTYPE_MAP.put("99441",TELEPHONE);        
       CPT_TO_PROCTYPE_MAP.put("99442",TELEPHONE);       
       CPT_TO_PROCTYPE_MAP.put("99443",TELEPHONE);       
       CPT_TO_PROCTYPE_MAP.put("98966",TELEPHONE);       
       CPT_TO_PROCTYPE_MAP.put("98967",TELEPHONE);
       CPT_TO_PROCTYPE_MAP.put("98968",TELEPHONE);
       
       // Set up HashMap PROCTYPE_TO_ENCCLASS_MAP for mapping from a procedure type to 
       // determine the class of encounter
       PROCTYPE_TO_ENCCLASS_MAP.put(PSYRAPY_WITH_MED_MGMT,ENC_CLASS_INDIVIDUAL);       
       PROCTYPE_TO_ENCCLASS_MAP.put(PSYRAPY_60MIN,ENC_CLASS_INDIVIDUAL);       
       PROCTYPE_TO_ENCCLASS_MAP.put(PSYRAPY_90MIN,ENC_CLASS_INDIVIDUAL);       
       PROCTYPE_TO_ENCCLASS_MAP.put(FAM_THERAPY,ENC_CLASS_INDIVIDUAL);        
       PROCTYPE_TO_ENCCLASS_MAP.put(GROUP_THERAPY,ENC_CLASS_GROUP);
       
       PROCTYPE_TO_ENCCLASS_MAP.put(EVALUATION,ENC_CLASS_INDIVIDUAL);       
       PROCTYPE_TO_ENCCLASS_MAP.put(MED_MGMT,ENC_CLASS_INDIVIDUAL);         
       PROCTYPE_TO_ENCCLASS_MAP.put(CARE_OR_SUPPORT,ENC_CLASS_INDIVIDUAL);       
       PROCTYPE_TO_ENCCLASS_MAP.put(ADDICTION,ENC_CLASS_INDIVIDUAL);       
       PROCTYPE_TO_ENCCLASS_MAP.put(TELEPHONE,ENC_CLASS_TELEPHONE);
       
       PROCTYPE_TO_ENCCLASS_MAP.put(OTHER,ENC_CLASS_OTHER);        
       PROCTYPE_TO_ENCCLASS_MAP.put(NO_CPT,ENC_CLASS_OTHER);       

           
       // Priority's set for the various procedure types;
       // 1 hast the highest priority; larger numbers have lower priority      
       CPT_PRIORITY.put(PSYRAPY_WITH_MED_MGMT, new Integer(1));
       CPT_PRIORITY.put(PSYRAPY_60MIN, new Integer(2));
       CPT_PRIORITY.put(PSYRAPY_90MIN, new Integer(3));
       CPT_PRIORITY.put(FAM_THERAPY, new Integer(4));       
       CPT_PRIORITY.put(GROUP_THERAPY, new Integer(5));
       CPT_PRIORITY.put(EVALUATION, new Integer(6));
       CPT_PRIORITY.put(MED_MGMT, new Integer(7));
       CPT_PRIORITY.put(CARE_OR_SUPPORT, new Integer(8));  
       CPT_PRIORITY.put(ADDICTION, new Integer(9));
       CPT_PRIORITY.put(TELEPHONE, new Integer(10));
       CPT_PRIORITY.put(OTHER, new Integer(11));
       CPT_PRIORITY.put(NO_CPT, new Integer(12));           
    }
 
    public EncounterCPTAndDiag m_enc;      // the encounter whose CPT codes we wish to 
                                           // characterize (if present)
    public HashSet<String> m_setCPTCodes = null;  // list of cptCodes to analyze                                           
  
    public ProcTypeCalculator(EncounterCPTAndDiag anEnc) {
        m_enc = anEnc;
        m_setCPTCodes = anEnc.m_setCPTCodes;        
    }
    
    public ProcTypeCalculator(HashSet<String> setCPTCodes) {
        m_setCPTCodes = setCPTCodes;
    }
    
    
    public String determineProcType() {
     
       String procTypeHighestP = OTHER;
       // Check that we have codes to analyze
       HashSet cptCodes = m_setCPTCodes;      
       if (cptCodes == null || cptCodes.size() <= 0 ){
          return NO_CPT;
       }
       
       // Initialize with next-to-lowest priority: OTHER
       Integer highestP = new Integer(11);
       
       Iterator iter = cptCodes.iterator();
       while (iter.hasNext()) {
          String aCPT = (String) iter.next();
          String aProcType = CPT_TO_PROCTYPE_MAP.get(aCPT);
          Integer aPriority;
          if (aProcType == null || aProcType.length() <= 0 ) {
             aProcType = OTHER;  
             aPriority = new Integer(11);
          } else {                   
             aPriority = CPT_PRIORITY.get(aProcType);
          }
          
          if (aPriority.intValue() < highestP.intValue()) {
             highestP = aPriority.intValue();  
             procTypeHighestP = aProcType;
          }          
       }
       return procTypeHighestP; 
    }
    
    public String classifyEncounter() {
      String procType = determineProcType();
      if (procType == null || procType.length() <= 0) {
        return ENC_CLASS_OTHER;
      } 
      
      String encounterClass = PROCTYPE_TO_ENCCLASS_MAP.get(procType);
      if (encounterClass == null) {
        return ENC_CLASS_OTHER;
      }
      return encounterClass;    
    }
    
    
}
