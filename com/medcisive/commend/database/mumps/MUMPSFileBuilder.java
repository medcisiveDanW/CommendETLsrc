package com.medcisive.commend.database.mumps;

import com.medcisive.utility.LogUtility;
import com.medcisive.utility.sql2.SQLObject;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

/**
 *
 * @author vhapalchambj
 */
public class MUMPSFileBuilder extends com.medcisive.utility.sql2.DBCUtil {

    public MUMPSFileBuilder() {}

    public String getFile(int sta3n) {
        StringBuilder result = new StringBuilder();
        String query =
                "SELECT \n"
                + "	sa.SurveyAnswerSID \n"
                + "	,sa.SurveyAnswerIEN \n"
                + "	,sa.Sta3n \n"
                + "	,sa.SurveyAdministrationIEN \n"
                + "	,sa.SurveyAdministrationSID \n"
                + "	,sa.PatientIEN \n"
                + "	,sa.PatientSID \n"
                + "	,sa.SurveyIEN \n"
                + "	,sa.SurveySID \n"
                + "	,sa.SurveyName \n"
                + "	,sa.SurveyGivenDateTime \n"
                + "	,sa.SurveyQuestionIEN \n"
                + "	,sa.SurveyQuestionSID \n"
                + "	,sa.SurveyChoiceIEN \n"
                + "	,sa.SurveyChoiceSID \n"
                + "	,sa.ETLBatchID \n"
                + "	,sa.VistaCreateDate \n"
                + "	,sa.VistaEditDate \n"
                + "     ,d.name \n"
                + "     ,d.SSN \n"
                + "FROM VDWWork.MH.SurveyAnswer sa, \n"
                + "     Commend.dbo.CommendDemographics d \n"
                + "WHERE SurveySID > 0 \n"
                + "  AND d.Sta3n = " + sta3n + " \n"
                + "  AND sa.Sta3n = d.Sta3n \n"
                + "  AND d.PatientSID = sa.PatientSID \n"
                + "ORDER BY PatientSID, SurveyAdministrationSID, SurveyGivenDateTime";
        final java.util.Map<String,Patient> pats = new java.util.HashMap();

        _dest.query(query, new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                java.util.Map<String,Object> map = new java.util.HashMap();
                    map.put("SurveyAnswerSID", rs.getString("SurveyAnswerSID"));
                    map.put("SurveyAnswerIEN", rs.getString("SurveyAnswerIEN"));
                    map.put("Sta3n", rs.getString("Sta3n"));
                    map.put("SurveyAdministrationIEN", rs.getString("SurveyAdministrationIEN"));
                    map.put("SurveyAdministrationSID", rs.getString("SurveyAdministrationSID"));
                    map.put("PatientIEN", rs.getString("PatientIEN"));
                    map.put("PatientSID", rs.getString("PatientSID"));
                    map.put("SurveyIEN", rs.getString("SurveyIEN"));
                    map.put("SurveySID", rs.getString("SurveySID"));
                    map.put("SurveyName", rs.getString("SurveyName"));
                    map.put("SurveyGivenDateTime", rs.getTimestamp("SurveyGivenDateTime"));
                    map.put("SurveyQuestionIEN", rs.getString("SurveyQuestionIEN"));
                    map.put("SurveyQuestionSID", rs.getString("SurveyQuestionSID"));
                    map.put("SurveyChoiceIEN", rs.getString("SurveyChoiceIEN"));
                    map.put("SurveyChoiceSID", rs.getString("SurveyChoiceSID"));
                    map.put("ETLBatchID", rs.getString("ETLBatchID"));
                    map.put("VistaCreateDate", rs.getString("VistaCreateDate"));
                    map.put("VistaEditDate", rs.getString("VistaEditDate"));
                    map.put("PatientName", rs.getString("name"));
                    map.put("SSN", rs.getString("SSN"));
                    if(pats.containsKey((String)map.get("PatientSID"))) {
                        Patient p = pats.get((String)map.get("PatientSID"));
                        p.add(map);
                    } else {
                        Patient p = new Patient(map);
                        pats.put(p.patientSID, p);
                    }
            }
        });
        for(Patient p : pats.values()) {
            result.append(p.tally());
        }
        return result.toString();
    }

    private class Patient {
        String patientSID = null;
        private java.util.Map<String,Score> scores = new java.util.HashMap();

        public Patient(java.util.Map<String,Object> map) {
            patientSID = (String)map.get("PatientSID");
            Score s = new Score(map);
            scores.put(s.SurveyAdministrationSID, s);
        }

        public void add(java.util.Map<String,Object> map) {
            if(scores.containsKey((String)map.get("SurveyAdministrationSID"))) {
                Score s = scores.get((String)map.get("SurveyAdministrationSID"));
                s.add(map);
            } else {
                Score s = new Score(map);
                s.add(map);
                scores.put(s.SurveyAdministrationSID, s);
            }
        }

        public String tally() {
            StringBuilder result = new StringBuilder();
            String tempStringBuilder = null;
            for(Score s : scores.values()) {
                tempStringBuilder = s.tally() + "\n";
                result.append(tempStringBuilder);
            }
            return result.toString();
        }
    }

    private class Score {
        private String SurveyAdministrationSID = null;
        private String firstQuestionSID = null;
        private java.util.Map<String,java.util.Map> rows = new java.util.HashMap();

        public Score(java.util.Map<String,Object> row) {
            SurveyAdministrationSID = (String)row.get("SurveyAdministrationSID");
            firstQuestionSID = (String)row.get("SurveyQuestionSID");
            rows.put((String)row.get("SurveyQuestionSID"),row);
        }

        public void add(java.util.Map<String,Object> row) {
            rows.put((String)row.get("SurveyQuestionSID"),row);
        }
        //MH^StationID^PatientID^PatientName^SSN^AdministrationID^DateGiven^InstrumentName^QuestionID^ChoiceID
        public String tally() {
            StringBuilder result = new StringBuilder();
            java.util.Map map = rows.get(firstQuestionSID);
            String SurveyGivenDateTime = new SimpleDateFormat("MM/dd/yy").format((java.sql.Timestamp)map.get("SurveyGivenDateTime"));
            String temp = "MH^" + map.get("Sta3n") + "^" + map.get("PatientIEN") + "^" + map.get("PatientName") + "^" + map.get("SSN")
                    + "^" + map.get("SurveyAdministrationIEN") + "^" + SurveyGivenDateTime + "^" + map.get("SurveyName");
            result.append(temp);
            for(java.util.Map m : rows.values()) {
                String SurveyQuestionSID = (String)m.get("SurveyQuestionIEN");
                String SurveyChoiceSID = (String)m.get("SurveyChoiceIEN");
                temp = "^" + SurveyQuestionSID + "^" + SurveyChoiceSID;
                result.append(temp);
            }
            return result.toString();
        }
    }
}
