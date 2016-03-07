package com.medcisive.commend.database.site;

import com.medcisive.utility.LogUtility;
import com.medcisive.utility.sql2.DBC;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 *
 * @author VHAPALWANGD
 */
public class EncounterProfiler {

    public EncounterProfiler() {}

    public static ArrayList doProfiling(final String sta3n, final String SSN, DBC dbc) {
        if (sta3n == null || SSN == null) {
            return null;
        }
        String query =
                "select \n"
                + "  prc.visitDateTime, \n"
                + "  prc.cptCode, \n"
                + "  prc.cptName, \n"
                + "  dia.icd9, \n"
                + "  dia.primarySecondary \n"
                + "from dbo.CommendVISNProcedure prc, \n"
                + "     dbo.CommendVISNDiagnosis dia \n"
                + "where prc.patientSSN = " + DBC.fixString(SSN) + " \n" // used to be like, testing =
                + " and prc.sta3n = " + DBC.fixString(sta3n) + " \n"
                + " and prc.sta3n = dia.sta3n \n"
                + " and dia.patientSSN = prc.patientSSN \n"
                + " and dia.visitSID = prc.visitSID \n"
                + " and dia.primaryStopCode BETWEEN 500 AND 599 \n"
                + "order by prc.visitDateTime";
        final ArrayList<EncounterCPTAndDiag> m_listProfiledEncs = new ArrayList<EncounterCPTAndDiag>();
        dbc.query(query, new com.medcisive.utility.sql2.SQLObject() {
            java.sql.Timestamp oldTs = null;
            EncounterCPTAndDiag existingEnc = null;
            
            @Override
            public void row(ResultSet rs) throws SQLException {
                java.sql.Timestamp ts = rs.getTimestamp("visitDateTime");
                String cptCode = rs.getString("cptCode");
                String diag = rs.getString("icd9");
                String primaryDiagFlag = rs.getString("primarySecondary");  // Y or N
                if (primaryDiagFlag!=null) {
                    if (ts.equals(oldTs)) {
                        // Here for the same encounter
                        existingEnc.addCPTCode(cptCode);
                        if (primaryDiagFlag.equals("P")) { existingEnc.setPrimaryDiag(diag);
                        } else { existingEnc.addSecondaryDiag(diag); }
                    } else {
                        if (existingEnc != null) {
                            existingEnc.calculateProcedureType();
                            m_listProfiledEncs.add(existingEnc);
                        }
                        // Here when we have a new encounter
                        EncounterCPTAndDiag newEnc = new EncounterCPTAndDiag(sta3n, SSN, null, ts);
                        newEnc.addCPTCode(cptCode);
                        if (primaryDiagFlag.equals("P")) { newEnc.setPrimaryDiag(diag);
                        } else { newEnc.addSecondaryDiag(diag); }
                        oldTs = ts;
                        existingEnc = newEnc;
                    }
                }
            }
            
            @Override
            public void post(){
                if (existingEnc != null) {
                    existingEnc.calculateProcedureType();
                    //Here to save the last encounter
                    m_listProfiledEncs.add(existingEnc);
                }
            }
        });
        if (m_listProfiledEncs.isEmpty()) {
            return null;
        }
        return m_listProfiledEncs;
    }
}
