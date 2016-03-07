package com.medcisive.commend.database.patient.graph.encounter;

import com.medcisive.utility.LogUtility;
import com.medcisive.utility.sql2.DBC;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author vhapalchambj
 */
public class PaitentEncounterGraphDBC extends com.medcisive.utility.sql2.DBCUtil {
    private final int _bufferSize = 1000;
    
    public PaitentEncounterGraphDBC() {}

    public void fillPatentEncounterGraphTable(final int sta3n) {
        _dest.update("DELETE FROM Commend.dbo.CommendPatientEncounterGraph WHERE Sta3n=" + sta3n);
        String query =
                "SELECT flat.sta3n \n"
                + "	 ,demo.PatientSID \n"
                + "      ,flat.encounterDate \n"
                + "      ,flat.cptCodes \n"
                + "FROM \n"
                + "	Commend.dbo.CommendVISNEncounterFlatten flat, \n"
                + "	Commend.dbo.CommendDemographics demo \n"
                + "WHERE \n"
                + "	flat.SSN = demo.SSN \n"
                + " AND flat.sta3n=" + sta3n + " \n"
                + "ORDER BY demo.patientSID, flat.encounterDate";
        
        _dest.query(query, new com.medcisive.utility.sql2.SQLObject() {
            int SID = -1;
            java.sql.Timestamp date = null;
            String codes = null;
            StringBuilder result = new StringBuilder();
            String color = null;
            Encounter.Class t = Encounter.Class.OTHER;
            java.util.List codeList = null;
            int counter = 0;
            com.medcisive.utility.sql2.DBC push = _dest.clone();
            
            @Override
            public void row(ResultSet rs) throws SQLException {
                SID = rs.getInt("PatientSID");
                date = rs.getTimestamp("encounterDate");
                codes = rs.getString("cptCodes");
                codeList = com.medcisive.utility.Util.stringToList(codes);
                try {
                    t = EncounterClassifier.getHighestPriorityClass(new java.util.HashSet<String>(codeList));
                } catch (Exception ex) { LogUtility.error(ex); t = Encounter.Class.OTHER; }
                switch(t) {
                    case INDIVIDUAL:
                        color = "'#093'";
                        break;
                    case MEDICATION:
                        color = "'#33F'";
                        break;
                    case CASE:
                        color = "'#09C'";
                        break;
                    case FAMILY:
                        color = "'#6CF'";
                        break;
                    case GROUP:
                        color = "'#C90'";
                        break;
                    case TELEPHONE:
                        color = "'#C0C'";
                        break;
                    case OTHER:
                        color = "'#C00'";
                        break;
                    default:
                        color = "'#C00'";
                        break;
                }
                String insert =
                        "INSERT INTO Commend.dbo.CommendPatientEncounterGraph \n"
                        + "VALUES ("
                        + sta3n + ","
                        + SID + ","
                        + DBC.fixTimestamp(date) + ","
                        + "'circle',"
                        + color + ","
                        + DBC.fixString(t.toString()) + ") \n";
                result.append(insert);
                counter++;
                if(counter>_bufferSize) {
                    push.update(result.toString());
                    result = new StringBuilder();
                    counter = 0;
                }   
            }
            
            @Override
            public void post() {
                if(result.length()>0) {
                    push.update(result.toString());
                }
            }
        });
    }
}
