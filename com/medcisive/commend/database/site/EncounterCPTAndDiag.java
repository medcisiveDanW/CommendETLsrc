package com.medcisive.commend.database.site;

import java.util.*;
import java.sql.*;

/**
 *
 * @author VHAPALWANGD
 */
public class EncounterCPTAndDiag {
    public String m_sta3n;                      // site
    public String m_SSN;                        // ssn of patient of interest
    public String m_patientDFN;                 // patient DFN
    public Timestamp m_encounterDateTime;       // time of this encounter
    public HashSet<String> m_setCPTCodes;              // Set of CPT codes entered for this encounter
    public String m_primaryDiag;                  // Primary diagnosis for this encounter (The primary diag should always exist)
    public HashSet<String> m_setSecondaryDiag;          // Set of secondary diagnoses (if any)
    public String m_strCPTCodes;                  // all CPT Codes in a comma delimited String
    public String m_encProcType = null;           // The encounter's procedure type is
    // determined by the ProcTypeCalculator class; there are 12 possible procedure
    // types mapped to various CPTCodes
    public String m_strSecondaryDiag;             // all Secondary diag in a comma delimited string (could be null)
    public boolean m_containsTargetTherapy = false;  // flag used for displaying colours
    String[] targetTpyArray = new String[]{"90801", "90806", "90807", "90808", "90809", "90818", "90819", "90821", "90822", "90847", "90853"};

    public EncounterCPTAndDiag(String sta3n, String SSN, String dfn, Timestamp encDateTime) {
        m_sta3n = sta3n;
        m_SSN = SSN;
        m_patientDFN = dfn;
        m_encounterDateTime = encDateTime;
        m_setCPTCodes = new HashSet<String>();
    }

    public void setPrimaryDiag(String aDiag) {
        m_primaryDiag = aDiag;
    }

    public void addCPTCode(String CPTCode) {
        if (checkIfTargetTherapy(CPTCode)) {
            m_containsTargetTherapy = true;  //this is a target therapy
        }
        m_setCPTCodes.add(CPTCode);
    }

    public void addSecondaryDiag(String secondaryDiag) {
        if (m_setSecondaryDiag == null) {
            m_setSecondaryDiag = new HashSet<String>();
        }
        m_setSecondaryDiag.add(secondaryDiag);
    }

    public String getStrListCPTCodes() {
        if (m_setCPTCodes == null || m_setCPTCodes.isEmpty()) {
            return null;
        }
        int count = 0;
        StringBuilder res = new StringBuilder();
        for (String aCPTCode : m_setCPTCodes) {
            res.append(aCPTCode).append(", ");
            count++;
            if (count > 10) {
                break;
            }
        }
        int len = res.length();
        res.delete(len - 2, len);  //delete last comma
        m_strCPTCodes = res.toString();
        return m_strCPTCodes;
    }

    public String getStrListSecondaryDiag() {
        if (m_setSecondaryDiag == null || m_setSecondaryDiag.isEmpty()) {
            return null;
        }
        StringBuilder res = new StringBuilder();
        for (String aSecDiag : m_setSecondaryDiag) {
            res.append(aSecDiag).append(", ");
        }
        int len = res.length();
        res.delete(len - 2, len);  //delete last comma
        m_strSecondaryDiag = res.toString();
        return m_strSecondaryDiag;
    }

    private boolean checkIfTargetTherapy(String CPTCode) {
        //Note: CPTCode is of the form 90806(GP) for TT target therapies
        boolean res = false;
        for (String aTT : targetTpyArray) {
            if (CPTCode.indexOf(aTT) != -1) {
                res = true;
                break;
            }
        }
        return res;
    }

    public boolean calculateProcedureType() {
        ProcTypeCalculator aPCalc = new ProcTypeCalculator(this);
        m_encProcType = aPCalc.determineProcType();
        return true;
    }
}
