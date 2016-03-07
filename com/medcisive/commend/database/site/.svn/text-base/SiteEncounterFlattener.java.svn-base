package com.medcisive.commend.database.site;

import com.medcisive.utility.LogUtility;
import com.medcisive.utility.sql2.DBC;

/**
 *
 * @author vhapalchambj
 */
public class SiteEncounterFlattener extends Thread {

    private String _sta3n;
    private String _ssn;
    private DBC _dbc;
    private static final java.util.ArrayList<String> _result = new java.util.ArrayList();

    public SiteEncounterFlattener(String sta3n, String ssn, DBC dbc) {
        _sta3n = sta3n;
        _ssn = ssn;
        _dbc = dbc;
    }

    @Override
    public void run() {
        java.util.ArrayList<EncounterCPTAndDiag> encList = getProfile(0);
        if ((encList==null) || encList.isEmpty()) { return; }
        java.lang.StringBuilder sb = new java.lang.StringBuilder();
        sb.append("INSERT INTO Commend.dbo.CommendVISNEncounterFlatten (SSN,sta3n,encounterDate,cssClass,cptCodes,procedureType,primaryDiagnosis,secondaryDiagnosis) \n");
        for (EncounterCPTAndDiag anEnc : encList) {
            java.sql.Timestamp encounterDate = anEnc.m_encounterDateTime;
            String cssClass = "noTargetTpy";
            if (anEnc.m_containsTargetTherapy == true) {
                cssClass = "containsTargetTpy";
            }
            String secondary = anEnc.getStrListSecondaryDiag();
            if (secondary == null || secondary.length() <= 0) {
                secondary = "-";
            }
            String cptCodes = anEnc.getStrListCPTCodes();
            String procType = anEnc.m_encProcType;
            String primary = anEnc.m_primaryDiag;
            sb.append(
                    "SELECT "
                    + DBC.fixString(_ssn) + ","
                    + DBC.fixString(_sta3n) + ","
                    + DBC.fixTimestamp(encounterDate) + ","
                    + DBC.fixString(cssClass) + ","
                    + DBC.fixString(cptCodes) + ","
                    + DBC.fixString(procType) + ","
                    + DBC.fixString(primary) + ","
                    + DBC.fixString(secondary) + " UNION ALL \n");
        }
        int index = sb.lastIndexOf("UNION ALL");
        if(index>0) {
            synchronized(_result) {
                _result.add(sb.substring(0,index));
            }
        }
    }

    private java.util.ArrayList<EncounterCPTAndDiag> getProfile(int depth) {
        java.util.ArrayList<EncounterCPTAndDiag> result = EncounterProfiler.doProfiling(_sta3n, _ssn, _dbc);
        if(result==null) {
            if(depth<3) {
                try { Thread.sleep(2000); } catch (java.lang.InterruptedException e) {LogUtility.error(e);}
                int temp = depth + 1;
                System.out.println("getProfile: trying again - attempt number " + temp);
                result = getProfile(temp);
                if(result!=null) {
                    System.out.println("getProfile: attempt number " + temp + " worked!");
                }
            }
        }
        return result;
    }

    public static void saveBatch(DBC dbc) {
        synchronized(_result) {
            java.lang.StringBuilder sb = new java.lang.StringBuilder();
            for(String s : _result) {
                sb.append(s);
            }
            dbc.update(sb.toString());
            sb = null;
            _result.clear();
        }
    }
}
