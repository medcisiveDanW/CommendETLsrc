package com.medcisive.commend.database.site;

import com.medcisive.commend.database.patient.graph.encounter.PaitentEncounterGraphDBC;
import com.medcisive.utility.LogUtility;
import com.medcisive.utility.Timer;
import com.medcisive.utility.sql2.DBC;
import com.medcisive.utility.sql2.SQLTable;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author vhapalchambj
 */
public class VISNEncounterFlattener extends com.medcisive.utility.sql2.DBCUtil {

    private static final java.util.Map<String, String> CPT_TO_PROCTYPE_MAP = new java.util.HashMap();
    private static final java.util.Map<String, Integer> CPT_PRIORITY = new java.util.HashMap();
    private static final java.util.Set<String> targetTherapies = new java.util.HashSet();
    private java.lang.StringBuilder _inserts = new java.lang.StringBuilder();
    private java.util.List<String> _patients;
    private int _sta3n;
    private float _totalCount;
    private float _currentCount = 0;
    private final int _bufferSize = 500000;
    private final static java.text.NumberFormat _nf = java.text.NumberFormat.getInstance();
    private final com.medcisive.utility.sql2.DBC _destSecondary;

    static {
        _nf.setMaximumFractionDigits(2);
        _nf.setMinimumFractionDigits(2);
        targetTherapies.add("90801");
        targetTherapies.add("90806");
        targetTherapies.add("90807");
        targetTherapies.add("90808");
        targetTherapies.add("90809");
        targetTherapies.add("90818");
        targetTherapies.add("90819");
        targetTherapies.add("90821");
        targetTherapies.add("90822");
        targetTherapies.add("90847");
        targetTherapies.add("90853");

        CPT_TO_PROCTYPE_MAP.put("90807", "PsyWithMM");

        CPT_TO_PROCTYPE_MAP.put("90806", "Psy60min");
        CPT_TO_PROCTYPE_MAP.put("90818", "Psy60min");
        CPT_TO_PROCTYPE_MAP.put("90819", "Psy60min");

        CPT_TO_PROCTYPE_MAP.put("90808", "Psy90min");
        CPT_TO_PROCTYPE_MAP.put("90809", "Psy90min");
        CPT_TO_PROCTYPE_MAP.put("90821", "Psy90min");
        CPT_TO_PROCTYPE_MAP.put("90822", "Psy90min");

        CPT_TO_PROCTYPE_MAP.put("90847", "FamThrpy");
        CPT_TO_PROCTYPE_MAP.put("90853", "GrpThrpy");
        CPT_TO_PROCTYPE_MAP.put("90801", "Eval");

        CPT_TO_PROCTYPE_MAP.put("90862", "MM");
        CPT_TO_PROCTYPE_MAP.put("M0064", "MM");
        CPT_TO_PROCTYPE_MAP.put("90805", "MM");

        CPT_TO_PROCTYPE_MAP.put("90815", "CareSupp");
        CPT_TO_PROCTYPE_MAP.put("90804", "CareSupp");
        CPT_TO_PROCTYPE_MAP.put("T1016", "CareSupp");

        CPT_TO_PROCTYPE_MAP.put("H0005", "Addictn");

        CPT_TO_PROCTYPE_MAP.put("99441", "Telephn");
        CPT_TO_PROCTYPE_MAP.put("99442", "Telephn");
        CPT_TO_PROCTYPE_MAP.put("99443", "Telephn");
        CPT_TO_PROCTYPE_MAP.put("98966", "Telephn");
        CPT_TO_PROCTYPE_MAP.put("98967", "Telephn");
        CPT_TO_PROCTYPE_MAP.put("98968", "Telephn");

        CPT_PRIORITY.put("PsyWithMM", 1);
        CPT_PRIORITY.put("Psy60min", 2);
        CPT_PRIORITY.put("Psy90min", 3);
        CPT_PRIORITY.put("FamThrpy", 4);
        CPT_PRIORITY.put("GrpThrpy", 5);
        CPT_PRIORITY.put("Eval", 6);
        CPT_PRIORITY.put("MM", 7);
        CPT_PRIORITY.put("CareSupp", 8);
        CPT_PRIORITY.put("Addictn", 9);
        CPT_PRIORITY.put("Telephn", 10);
    }

    public VISNEncounterFlattener(int sta3n) throws InterruptedException {
        _sta3n = sta3n;
        _patients = _getAllPatients();
        _totalCount = _patients.size();
        _destSecondary = _dest.clone();
    }

    public static Thread createThread(final int sta3n) {
        Thread result = new Thread() {
            @Override
            public void run() {
                try {
                    VISNEncounterFlattener visnApptSummaryBuilder = new VISNEncounterFlattener(sta3n);
                    visnApptSummaryBuilder.startVISNEncounterFlattener();
                } catch (InterruptedException ex) {
                    System.out.println("Could not start VISNEncounterFlattener Thread: " + ex);
                }
            }
        };
        result.start();
        return result;
    }

    public void startVISNEncounterFlattener() {
        if (!_isValid(_sta3n)) {
            return;
        }
        Timer t = Timer.start();
        String query =
                "DELETE FROM Commend.dbo.CommendVISNEncounterFlatten \n"
                + "WHERE sta3n = " + _sta3n;
        _dest.update(query);
        if (_patients == null || _patients.isEmpty()) {
            return;
        }
        query =
                "select \n"
                + "     prc.sta3n, \n"
                + "     prc.patientSSN, \n"
                + "     prc.visitDateTime, \n"
                + "     prc.cptCode, \n"
                + "     prc.cptName, \n"
                + "     dia.icd9, \n"
                + "     dia.primarySecondary \n"
                + "from dbo.CommendVISNProcedure prc, \n"
                + "     dbo.CommendVISNDiagnosis dia \n"
                + "where prc.patientSSN in " + DBC.javaListToSQLList(_patients) + " \n"
                + "  and prc.sta3n = dia.sta3n \n"
                + "  and dia.patientSSN = prc.patientSSN \n"
                + "  and dia.visitSID = prc.visitSID \n"
                + "  and dia.primaryStopCode BETWEEN 500 AND 599 \n"
                + "order by prc.patientSID, prc.visitDateTime";
        _dest.query(query, new com.medcisive.utility.sql2.SQLObject() {

            @Override
            public void row(ResultSet rs) throws SQLException {
                _processEncounter(rs);
            }
        });
        _pushData();
        t.print();
        PaitentEncounterGraphDBC peg = new PaitentEncounterGraphDBC();
        peg.fillPatentEncounterGraphTable(_sta3n);
    }

    public void setPatients(java.util.List list) {
        _patients = list;
    }

    private void _processEncounter(ResultSet rs) throws SQLException {
        java.util.Set<String> cptSet = new java.util.HashSet();
        java.util.Set<String> diagSet = new java.util.HashSet();
        String SSN = null;
        String sta3n = null;
        String diag = null;
        String diagPrimary = null;
        java.sql.Timestamp visitDateTime = null;
        try {
            do {
                //System.out.println("sta3n: " + rs.getString("sta3n") + " SSN: " + rs.getString("patientSSN") + " visitDateTime: " + rs.getString("visitDateTime") + " icd9: " + rs.getString("icd9") + " cptCode: " + rs.getString("cptCode"));
                if (SSN == null && visitDateTime == null) {
                    sta3n = rs.getString("sta3n");
                    SSN = rs.getString("patientSSN");
                    visitDateTime = rs.getTimestamp("visitDateTime");
                } else if (visitDateTime.getTime() != rs.getTimestamp("visitDateTime").getTime() || !SSN.equalsIgnoreCase(rs.getString("patientSSN"))) {
                    if(!SSN.equalsIgnoreCase(rs.getString("patientSSN"))){
                        _currentCount++;
                    }
                    rs.previous();
                    break;
                }
                diag = rs.getString("icd9");
                if (rs.getString("primarySecondary") != null) {
                    if (rs.getString("primarySecondary").equals("P")) {
                        diagPrimary = diag;
                    } else {
                        diagSet.add(diag);
                    }
                }
                cptSet.add(rs.getString("cptCode"));
            } while (rs.next());
        } catch (java.sql.SQLException e) {
            LogUtility.error(e);
            System.err.println("Error: VISNEncounterFlattener._processEncounter - " + e);
        }
        _insertFlatEncounter(SSN, sta3n, visitDateTime, cptSet, diagPrimary, diagSet);
    }

    private void _insertFlatEncounter(String ssn, String sta3n, java.sql.Timestamp ts, java.util.Set<String> cptSet, String primary, java.util.Set<String> diagSet) {
        if (!_isValid(ssn) || !_isValid(sta3n) || !_isValid(ts) || !_isValid(cptSet) || !_isValid(primary) || !_isValid(diagSet)) {
            return;
        }
        java.util.Map<String, Object> result = new java.util.HashMap();
        boolean isTargetCpt = false;
        for (String cpt : cptSet) {
            if (targetTherapies.contains(cpt)) {
                isTargetCpt = true;
                break;
            }
        }
        String css = "noTargetTpy";
        String type = _determineCPTType(cptSet);
        if (isTargetCpt) {
            css = "containsTargetTpy";
        }
        String cpts = "";
        for (String cpt : cptSet) {
            cpts += cpt + ",";
        }
        int index = cpts.lastIndexOf(',');
        if (index > 0) {
            cpts = cpts.substring(0, index);
        }
        String diags = "";
        for (String diag : diagSet) {
            diags += diag + ",";
        }
        index = diags.lastIndexOf(',');
        if (index > 0) {
            diags = diags.substring(0, index);
        }
        result.put("ssn", ssn);
        result.put("sta3n", sta3n);
        result.put("encounterDate", ts);
        result.put("cssClass", css);
        result.put("cptCodes", cpts);
        result.put("procType", type);
        result.put("primary", primary);
        result.put("secondary", diags);
        _inserts.append(_getInsert(result));
        if (_inserts.length() > _bufferSize) {
            _pushData();
        }
    }

    private String _determineCPTType(java.util.Set<String> set) {
        String procTypeHighestP = "OtherPrc";
        if (set == null || set.size() <= 0) {
            return "NoCPT";
        }
        int highestP = 11;
        for (String cpt : set) {
            String aProcType = CPT_TO_PROCTYPE_MAP.get(cpt);
            int aPriority;
            if (aProcType == null || aProcType.length() <= 0) {
                continue;
            }
            aPriority = CPT_PRIORITY.get(aProcType);
            if (aPriority < highestP) {
                highestP = aPriority;
                procTypeHighestP = aProcType;
            }
        }
        return procTypeHighestP;
    }

    private String _getInsert(java.util.Map<String, Object> map) {
        String result = "INSERT INTO Commend.dbo.CommendVISNEncounterFlatten (SSN,sta3n,encounterDate,cssClass,cptCodes,procedureType,primaryDiagnosis,secondaryDiagnosis) \n"
                + "VALUES ("
                + DBC.fixString((String) map.get("ssn")) + ","
                + DBC.fixString((String) map.get("sta3n")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp) map.get("encounterDate")) + ","
                + DBC.fixString((String) map.get("cssClass")) + ","
                + DBC.fixString((String) map.get("cptCodes")) + ","
                + DBC.fixString((String) map.get("procType")) + ","
                + DBC.fixString((String) map.get("primary")) + ","
                + DBC.fixString((String) map.get("secondary")) + ") \n";
        return result;
    }

    private void _pushData() {
        System.out.println(_nf.format((_currentCount / _totalCount) * 100) + "% complete");
        if(_inserts.length()>0) {
            _destSecondary.update(_inserts.toString());
            _inserts = null;
            _inserts = new java.lang.StringBuilder();
        }
    }

    private java.util.ArrayList<String> _getAllPatients() {
        Timer t = Timer.start();
        String query =
                "SELECT distinct patientSSN \n"
                + "FROM Commend.dbo.CommendVISNPatient \n"
                + "WHERE sta3n = " + _sta3n;
        SQLTable tm = _dest.getTable(query);
        t.print();
        return (java.util.ArrayList<String>) tm.getColumn("patientSSN");
    }

    private boolean _isValid(Object o) {
        if (o == null) {
            return false;
        }
        return true;
    }
}
