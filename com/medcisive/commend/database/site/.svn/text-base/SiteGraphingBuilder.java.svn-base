package com.medcisive.commend.database.site;

import com.medcisive.utility.LogUtility;
import com.medcisive.utility.sql2.DBC;
import com.medcisive.utility.sql2.SQLTable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

/**
 *
 * @author vhapalchambj
 */
public class SiteGraphingBuilder extends Thread {
    private DBC _sql;
    private final static java.util.HashMap<String,String> colorMap = new java.util.HashMap();
    private static java.util.Map<String,java.util.Map<String,String>> _querys = Collections.synchronizedMap(new java.util.LinkedHashMap());
    private String _ien = null;
    private int _sta3n;

    static {
        colorMap.put("green","#063");
        colorMap.put("blue","#039");
        colorMap.put("orange","#F60");
        colorMap.put("red","#C00");
        colorMap.put("purple","#609");
    }

    public SiteGraphingBuilder(int sta3n, String ien, DBC sql) {
        _sql = sql;
        _ien = ien;
        _sta3n = sta3n;
    }

    @Override
    public void run() {
        String jsonData = _getPatientPTSDjavascriptData(_ien, "RM");
        java.util.Map<String,String> map = new java.util.HashMap();
        map.put("RM", jsonData);
        jsonData = _getPatientPTSDjavascriptData(_ien, "FYM0");
        map.put("FYM0", jsonData);
        jsonData = _getPatientPTSDjavascriptData(_ien, "FYM1");
        map.put("FYM1", jsonData);
        insert(_ien, map);
    }

    private static synchronized void insert(String ien, java.util.Map<String,String> map) {
        _querys.put(ien,map);
    }

    private String _getPatientPTSDjavascriptData(String ien,String type) {
        if (ien == null || type == null) {
            return null;
        }
        String result = " { ";
        java.util.HashMap<String,Object> patientInfo = _getPatientData(ien,type);
        String patientGraphData = _getPatientPTSDTherapyGraphData(ien,type);
        for(String key : patientInfo.keySet()) {
            result += key + ": \"" + patientInfo.get(key) + "\", ";
        }
        result += "graphData:" + patientGraphData;
        result += " }";
        return result;
    }

    private String _getPatientPTSDTherapyGraphData(String ien, String type) {
        String result = "[ ";
        if (ien == null || type == null) {
            return null;
        }
        java.util.HashMap<String, java.util.ArrayList<java.sql.Timestamp>> resultMap = new java.util.HashMap();
        resultMap.put("greendiamond", new java.util.ArrayList());
        resultMap.put("orangediamond", new java.util.ArrayList());
        resultMap.put("greencircle", new java.util.ArrayList());
        resultMap.put("orangecircle", new java.util.ArrayList());
        resultMap.put("orangetriangle", new java.util.ArrayList());
        resultMap.put("orangecross", new java.util.ArrayList());

        java.util.LinkedHashMap<java.sql.Timestamp, String> data = getPatientPTSDTimestampCategoryData(ien,type);
        for (java.sql.Timestamp ts : data.keySet()) {
            int category = -1;
            try {
                category = Integer.parseInt(data.get(ts));
            } catch (NumberFormatException e) { LogUtility.error(e);
                continue;
            }
            switch (category) {
                case 1:
                    resultMap.get("greendiamond").add(ts);
                    break;
                case 2:
                    resultMap.get("orangediamond").add(ts);
                    break;
                case 3:
                    resultMap.get("greencircle").add(ts);
                    break;
                case 4:
                    resultMap.get("orangecircle").add(ts);
                    break;
                case 5:
                    resultMap.get("orangetriangle").add(ts);
                    break;
                case 6:
                    resultMap.get("orangecross").add(ts);
                    break;
                default:
                    break;
            }
        }
        // revers order of prioity due to rendering pass
        result += _getGraphStringData(resultMap.get("orangecross"), "cross",colorMap.get("orange")) + ", ";
        result += _getGraphStringData(resultMap.get("orangetriangle"), "triangle",colorMap.get("orange")) + ", ";
        result += _getGraphStringData(resultMap.get("orangecircle"), "circle",colorMap.get("orange")) + ", ";
        result += _getGraphStringData(resultMap.get("greencircle"), "circle",colorMap.get("green")) + ", ";
        result += _getGraphStringData(resultMap.get("orangediamond"), "diamond",colorMap.get("blue")) + ", ";
        result += _getGraphStringData(resultMap.get("greendiamond"), "diamond",colorMap.get("green")) + " ]";

        return result;
    }

    private String _getGraphStringData(java.util.ArrayList<java.sql.Timestamp> data, String symbol, String color) {
        if (data == null || symbol == null) {
            return null;
        }
        String result = "{ data: [";
        for (java.sql.Timestamp ts : data) {
            result += "[" + ts.getTime() + ",0],";
        }
        int lastComma = result.lastIndexOf(',');
        if (lastComma > 0) {
            result = result.substring(0, lastComma);
        }
        result += "], points: { symbol: \"" + symbol + "\" }, color: \"" + color + "\", fillColor: \"" + color + "\" }";
        return result;
    }

    private java.util.HashMap<String, Object> _getPatientData(String ien, String type) {
        if (ien == null || type == null) {
            return null;
        }
        java.util.HashMap<String, Object> result = new java.util.HashMap();
        String query =
                "SELECT distinct \n"
                + "       pat.patientName \n"
                + "       ,pat.DateOfBirth \n"
                + "       ,pat.gender \n"
                + "       ,pat.OEFOIFService \n"
                + "       ,enc.status \n"
                + "       ,enc.windowStart \n"
                + "       ,enc.windowEnd \n"
                + "       ,enc.deadline \n"
                + "FROM Commend.dbo.CommendVISNPatient pat, \n"
                + "     Commend.dbo.CommendVISNEncSummary enc \n"
                + "WHERE pat.patientIEN = " + DBC.fixString(ien) + " \n"
                + "  AND pat.patientIEN = enc.patientIEN \n"
                + "  AND enc.measureType = " + DBC.fixString(type) + " \n"
                + "  AND pat.sta3n = " + _sta3n + " \n"
                + "  AND enc.sta3n = pat.sta3n";
        SQLTable tm = _sql.getTable(query);
        java.util.LinkedHashMap<String, Object> map = tm.getRow(0);
        if(map!=null) {
            result.put("ien", ien);
            result.put("name", map.get("patientName"));
            result.put("dob", map.get("DateOfBirth"));
            result.put("gender", map.get("gender"));
            result.put("oefoif", map.get("OEFOIFService"));
            result.put("status", map.get("status"));
            result.put("windowStart", convertTimestamp(map.get("windowStart")));
            result.put("windowEnd", convertTimestamp(map.get("windowEnd")));
            result.put("deadline", convertTimestamp(map.get("deadline")));
        }
        return result;
    }

    private String convertTimestamp(Object o) {
        if(o==null) { return null; }
        String result = null;
        if(o.getClass().equals(java.sql.Timestamp.class)) {
            java.sql.Timestamp ts = (java.sql.Timestamp)o;
            long ms = ts.getTime();
            result = "" + ms;
        }
        return result;
    }

    private java.util.LinkedHashMap<java.sql.Timestamp, String> getPatientPTSDTimestampCategoryData(String ien, final String type) {
        if (ien == null || type == null) {
            return null;
        }
        final java.util.LinkedHashMap<java.sql.Timestamp, String> result = new java.util.LinkedHashMap();
        String query =
                "SELECT encounterDateFloor \n"
                + "      ," + type + "CatID \n"
                + "FROM Commend.dbo.CommendVISNEncAnalysisView \n"
                + "WHERE patientIEN = " + DBC.fixString(ien) + " \n"
                + "ORDER BY encounterDateFloor";
        _sql.query(query, new com.medcisive.utility.sql2.SQLObject() {

            @Override
            public void row(ResultSet rs) throws SQLException {
                java.sql.Timestamp ts = rs.getTimestamp("encounterDateFloor");
                String category = rs.getString(type + "CatID");
                result.put(ts, category);
            }
        });
        return result;
    }

    public java.util.ArrayList<String> getPatients(String duz) {
        if(duz==null) { return null; }
        final java.util.ArrayList<String> result = new java.util.ArrayList();
        String query =
                "SELECT DISTINCT pat.patientIEN \n"
                + "FROM Commend.dbo.CommendVISNPatientPanel panel, \n"
                + "     Commend.dbo.CommendVISNPatient pat \n"
                + "WHERE pat.patientIEN = panel.patientIEN \n"
                + "  and panel.providerDUZ = " + DBC.fixString(duz);
        _sql.query(query, new com.medcisive.utility.sql2.SQLObject() {

            @Override
            public void row(ResultSet rs) throws SQLException {
                result.add(rs.getString("patientIEN"));
            }
        });
        return result;
    }

    public String getSSNbyIEN(String ien) {
        if(ien==null) { return null; }
        final StringBuilder result = new StringBuilder();
        String query = "SELECT DISTINCT patientSSN \n"
                + "FROM Commend.dbo.CommendVISNPatient \n"
                + "WHERE patientIEN = " + DBC.fixString(ien);
        _sql.query(query, new com.medcisive.utility.sql2.SQLObject() {

            @Override
            public void row(ResultSet rs) throws SQLException {
                result.append(rs.getString("patientSSN"));
            }
        });
        return result.toString();
    }

    public java.util.ArrayList<java.util.Map<String,Object>> getPatientEncounterFlatten(String ssn) {
        if(ssn==null) { return null; }
        final java.util.ArrayList<java.util.Map<String,Object>> result = new java.util.ArrayList();
        String query = "SELECT * \n"
                + "FROM Commend.dbo.CommendVISNEncounterFlatten \n"
                + "WHERE SSN = " + DBC.fixString(ssn) + " \n"
                + "ORDER BY encounterDate DESC";
        _sql.query(query, new com.medcisive.utility.sql2.SQLObject() {

            @Override
            public void row(ResultSet rs) throws SQLException {
                java.util.Map<String,Object> map = new java.util.HashMap();
                map.put("SSN", rs.getString("SSN"));
                map.put("sta3n", rs.getString("sta3n"));
                map.put("encounterDate", rs.getTimestamp("encounterDate"));
                map.put("cssClass", rs.getString("cssClass"));
                map.put("cptCodes", rs.getString("cptCodes"));
                map.put("primaryDiagnosis", rs.getString("primaryDiagnosis"));
                map.put("secondaryDiagnosis", rs.getString("secondaryDiagnosis"));
                result.add(map);
            }
        });
        return result;
    }

    public static synchronized void insertData(int sta3n, com.medcisive.utility.sql2.DBC sql) {
        StringBuilder result = new StringBuilder();
        for(String ien : _querys.keySet()) {
            java.util.Map<String,String> map = _querys.get(ien);
            StringBuilder curr = new StringBuilder();
            for(String type : map.keySet()) {
                curr.append(_insertQuery(sta3n, ien,map.get(type), type));
            }
            if(curr.length()>0) {
                result.append(curr.toString());
            }
        }
        _querys.clear();
        if(result.length()>0) {
            sql.update(result.toString());
        }
        
    }

    private static String _insertQuery(int sta3n, String ien, String data, String type) {
        if( (sta3n<=0) || (ien==null) ) { return ""; }
        return  "INSERT INTO Commend.dbo.CommendVISNGraphData (sta3n,ien,jsonObject,measureType) \n"
                + "VALUES (" + sta3n + "," + DBC.fixString(ien) + "," + DBC.fixString(data) + "," + DBC.fixString(type) +") \n";
    }
}
