package com.medcisive.commend.database.site;

import com.medcisive.utility.sql2.DBC;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author vhapalchambj
 */
public class EncounterAnalyzer extends Thread {
    private java.util.Map<String,java.sql.Timestamp> _qualifying;
    private java.sql.Timestamp _encounterDate = null;
    private java.util.ArrayList<java.util.Map<String,Object>> data = new java.util.ArrayList();
    private java.util.Map<String,Object> _resultMap = new java.util.HashMap();
    private int maxInsertSize = 1000;
    private static final java.util.Collection<String> _inserts = Collections.synchronizedCollection(new java.util.ArrayList());
    public static final java.util.Queue<String> _batches = new java.util.LinkedList();
    private static final java.util.ArrayList<String> _cptCodes = new java.util.ArrayList();
    private static final int[][] _categoryMatrix = new int[2][2];

    static {
        _cptCodes.add("90801");
        _cptCodes.add("90806");
        _cptCodes.add("90807");
        _cptCodes.add("90808");
        _cptCodes.add("90809");
        _cptCodes.add("90818");
        _cptCodes.add("90819");
        _cptCodes.add("90821");
        _cptCodes.add("90822");
        _cptCodes.add("90847");
        _cptCodes.add("90853");

        _categoryMatrix[0][0] = 3;
        _categoryMatrix[0][1] = 4;
        _categoryMatrix[1][0] = 5;
        _categoryMatrix[1][1] = 6;
    }

    public EncounterAnalyzer(java.util.Map<String,java.sql.Timestamp> qualifying) {
        _qualifying = qualifying;
    }

    public void push(ResultSet rs) throws java.sql.SQLException {
        if(rs==null) { return; }
        java.util.Map<String,Object> push = new java.util.HashMap();
        push.put("sta3n", rs.getString("sta3n"));
        push.put("patientSSN", rs.getString("patientSSN"));
        push.put("patientIEN", rs.getString("patientIEN"));
        push.put("patientSID", rs.getString("patientSID"));
        push.put("visitSID", rs.getString("visitSID"));
        push.put("encounterDate", rs.getTimestamp("encounterDate"));
        push.put("encounterDateFloor", rs.getTimestamp("encounterDateFloor"));
        push.put("CPTCode", rs.getString("CPTCode"));
        push.put("icd9", rs.getString("icd9"));
        push.put("primarySecondary", rs.getString("primarySecondary"));
        data.add(push);
    }

    @Override
    public void run() {
        int category = _catigorizeEncounter();
        _resultMap.put("RM", category);
        _resultMap.put("FYM0", category);
        _resultMap.put("FYM1", category);
        if(_qualifying!=null) {
            for(String type : _qualifying.keySet()) {
                java.sql.Timestamp ts = _qualifying.get(type);
                if(ts!=null && (_encounterDate.getTime()==ts.getTime())) {
                    _resultMap.put(type, getQualifyingCategory(category));
                }
            }
        }
        _saveInsert();
    }

    private int _catigorizeEncounter() {
        int min = 6;
        for(java.util.Map<String,Object> row : data) {
            int matrixX = 1;
            int matrixY = 1;
            if(_encounterDate==null) {
                _encounterDate = (java.sql.Timestamp)row.get("encounterDateFloor");
                _resultMap.put("sta3n", row.get("sta3n"));
                _resultMap.put("patientSSN", row.get("patientSSN"));
                _resultMap.put("patientIEN", row.get("patientIEN"));
                _resultMap.put("patientSID", row.get("patientSID"));
                _resultMap.put("visitSID", row.get("visitSID"));
                _resultMap.put("encounterDate", row.get("encounterDate"));
                _resultMap.put("encounterDateFloor", _encounterDate);
            }
            String cpt = row.get("CPTCode").toString();
            if(isTargetTherapy(cpt)) {
                matrixX = 0;
            }
            String diag = row.get("icd9").toString();
            if(isCorrectDiagnosis(diag)) {
                matrixY = 0;
            }
            int temp = _categoryMatrix[matrixX][matrixY];
            if(temp<min) {
                min = temp;
                _resultMap.put("visitSID", row.get("visitSID"));
                _resultMap.put("encounterDate", row.get("encounterDate"));
            }
        }
        return min;
    }

    private boolean isTargetTherapy(String cpt) {
        if(cpt==null) { return false; }
        for(String tt : _cptCodes) {
            if(cpt.equalsIgnoreCase(tt)) {
                return true;
            }
        }
        return false;
    }

    private boolean isCorrectDiagnosis(String diag) {
        if(diag==null) { return false; }
        if(diag.equalsIgnoreCase("309.81")) {
            return true;
        }
        return false;
    }

    private int getQualifyingCategory(int category) {
        if(category==3) {
            return 1;
        }
        if(category==5) {
            return 2;
        }
        return category;
    }

    private void _saveInsert() {
        if(_resultMap==null) { return; }
        String query =
                "INSERT INTO Commend.dbo.CommendVISNEncAnalysis (sta3n,SSN,patientIEN,patientSID,visitSID,encounterDate,encounterDateFloor,RMCatID,FYM0CatID,FYM1CatID) \n"
                + "VALUES ("
                + DBC.fixString(_resultMap.get("sta3n").toString()) + ","
                + DBC.fixString(_resultMap.get("patientSSN").toString()) + ","
                + DBC.fixString(_resultMap.get("patientIEN").toString()) + ","
                + DBC.fixString(_resultMap.get("patientSID").toString()) + ","
                + DBC.fixString(_resultMap.get("visitSID").toString()) + ","
                + DBC.fixTimestamp((java.sql.Timestamp)_resultMap.get("encounterDate")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp)_resultMap.get("encounterDateFloor")) + ","
                + DBC.fixString(_resultMap.get("RM").toString()) + ","
                + DBC.fixString(_resultMap.get("FYM0").toString()) + ","
                + DBC.fixString(_resultMap.get("FYM1").toString()) + ") \n";
        synchronized(_inserts) {
            _inserts.add(query);
        }
        if(_inserts.size()>maxInsertSize) {
            _updateBatch();
        }
    }

    public static void _updateBatch() {
        String result = "";
        synchronized(_inserts) {
            for(String s : _inserts) {
                result += s;
            }
            _inserts.clear();
            if(result.length()>0) {
                synchronized(_batches) {
                    _batches.add(result);
                }
            } else {
                System.err.println("result: " + result + " _inserts: " + _inserts);
            }
        }
    }

    public static void insert(SiteAnalysisDatabaseController dbc) {
        synchronized(_batches) {
            if(!_batches.isEmpty()) {
                String update = _batches.poll();
                //System.out.println("inserting batch!\n" + update);
                dbc.update(update);
            }
        }
    }
}