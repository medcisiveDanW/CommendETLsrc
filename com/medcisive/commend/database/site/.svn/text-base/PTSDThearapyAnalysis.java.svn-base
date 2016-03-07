package com.medcisive.commend.database.site;

import com.medcisive.utility.*;
import com.medcisive.utility.sql2.SQLTable;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author vhapalchambj
 */
public class PTSDThearapyAnalysis extends com.medcisive.utility.sql2.DBCUtil {

    private java.util.ArrayList<SiteAnalysisDatabaseController> _dbcs = new java.util.ArrayList();
    private String _processPatient = null;
    private int _sta3n;
    private final static java.text.NumberFormat _nf = java.text.NumberFormat.getInstance();
    private int _count;
    static {
        _nf.setMaximumFractionDigits(2);
        _nf.setMinimumFractionDigits(2);
    }

    public PTSDThearapyAnalysis(int sta3n) {
        _sta3n = sta3n;
        ThreadCyclicManager._triggerAt = 100; // Above 100 causes stabliltiy issues.
        for (int i = 0; i <= ThreadCyclicManager._triggerAt; i++) {
            _dbcs.add(new SiteAnalysisDatabaseController(_sta3n));
        }
    }

    public void processPatient(String SSN) {
        _processPatient = SSN;
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

    private java.util.ArrayList<String> _getAllPatientIENs() {
        Timer t = Timer.start();
        String query =
                "SELECT distinct pat.patientIEN \n"
                + "FROM Commend.dbo.CommendVISNPatient pat, \n"
                + "     Commend.dbo.CommendVISNEncSummary enc \n"
                + "WHERE pat.sta3n = " + _sta3n + " \n"
                + "  AND pat.patientIEN = enc.patientIEN";
        SQLTable tm = _dest.getTable(query);
        t.print();
        return (java.util.ArrayList<String>) tm.getColumn("patientIEN");
    }
    
    public static Thread createThread(final int sta3n) {
        Thread result = new Thread() {
            @Override
            public void run() {
                PTSDThearapyAnalysis visnApptSummaryBuilder = new PTSDThearapyAnalysis(sta3n);
                visnApptSummaryBuilder._processVISNPatientAnalysis();
            }
        };
        result.start();
        return result;
    }
    
    private void _processVISNPatientAnalysis() {
        Timer t = Timer.start();
        String query = "DELETE FROM Commend.dbo.CommendVISNEncSummary";
        _dest.update(query);
        _count = 0;
        java.util.ArrayList<String> patients = _getAllPatients();
        SitePatientAnalysis.totalPatients = patients.size();
        final ThreadCyclicManager cycle = new ThreadCyclicManager(new ThreadCyclicEvent() {
                @Override
                public void go() {
                    SitePatientAnalysis._progress();
                    SitePatientAnalysis.executeInsert(_dest);
                    _count = 0;
                }
            });
        if (_processPatient != null) {
            Thread thread = new SitePatientAnalysis(_processPatient, _dbcs.get(_count));
            cycle.add(thread);
            cycle.finalizeCycle();
            SitePatientAnalysis.executeInsert(_dest);
        } else if((patients!=null) && (!patients.isEmpty())) {
            for (String patient : patients) {
                Thread thread = new SitePatientAnalysis(patient, _dbcs.get(_count));
                cycle.add(thread);
                _count++;
            }
            cycle.finalizeCycle();
        }
        t.print();
    }

    private void _processVISNEncounterAnalysis(final int sta3n) {
        Timer t = Timer.start();
        String query = "DELETE FROM Commend.dbo.CommendVISNEncAnalysis \n";
        _dest.update(query);
        query =
                "SELECT dia.sta3n \n"
                + "     ,dia.patientSSN \n"
                + "     ,dia.patientIEN \n"
                + "     ,dia.patientSID \n"
                + "     ,dia.visitSID \n"
                + "     ,CONVERT(datetime, dia.visitDateTime) AS encounterDate \n"
                + "     ,cast(floor(cast(dia.visitDateTime as float)) as datetime) AS encounterDateFloor \n"
                + "     ,pro.CPTCode \n"
                + "     ,dia.icd9 \n"
                + "     ,dia.primarySecondary \n"
                + "FROM Commend.dbo.CommendVISNDiagnosis dia, \n"
                + "     Commend.dbo.CommendVISNProcedure pro \n"
                + "WHERE pro.visitSID = dia.visitSID \n"
                + "  and pro.patientSSN = dia.patientSSN \n"
                + "  and dia.sta3n = " + sta3n + " \n"
                + "  and dia.PrimaryStopCode < 600 \n"
                + "  and dia.PrimaryStopCode > 499 \n"
                + "ORDER BY dia.patientSSN, dia.visitSID";
        final ThreadCyclicManager cycle = new ThreadCyclicManager();
        ThreadCyclicManager._triggerAt = 1000;
        
        _dest.query(query, new com.medcisive.utility.sql2.SQLObject() {
            java.util.Map<String,java.util.Map<String,java.sql.Timestamp>> patientQualifyMap = getPatientQualifyMap(sta3n);
            int tempTriggerAt = ThreadCyclicManager._triggerAt;    
            String prevSID = null;
            String currSID = null;
            java.util.Map<String,java.sql.Timestamp> qualifying = null;
            EncounterAnalyzer encAnalyzer = null;
            @Override
            public void row(ResultSet rs) throws SQLException {
                currSID = rs.getString("visitSID");
                if(prevSID==null) {
                    prevSID = currSID;
                    qualifying = patientQualifyMap.get(rs.getString("patientSSN"));
                    encAnalyzer = new EncounterAnalyzer(qualifying);
                }
                if(prevSID.equalsIgnoreCase(currSID)) {
                    encAnalyzer.push(rs);
                }
                else {
                    cycle.add(encAnalyzer);
                    prevSID = currSID;
                    qualifying = patientQualifyMap.get(rs.getString("patientSSN"));
                    encAnalyzer = new EncounterAnalyzer(qualifying);
                    encAnalyzer.push(rs);
                }
                if(EncounterAnalyzer._batches.size()>=_dbcs.size()-1) {
                    insertEncounterAnalysis();
                }
            }
            
            @Override
            public void post(){
                if(encAnalyzer!=null) {
                    cycle.add(encAnalyzer);
                    cycle.finalizeCycle();
                    EncounterAnalyzer._updateBatch();
                    ThreadCyclicManager._triggerAt = tempTriggerAt;
                    insertEncounterAnalysis();
                }
            }
        });
        t.print();
    }

    private void insertEncounterAnalysis() {
        int counter = 0;
        while(!EncounterAnalyzer._batches.isEmpty()) {
            EncounterAnalyzer.insert(_dbcs.get(counter));
            counter++;
        }
    }

    private java.util.Map<String,java.util.Map<String,java.sql.Timestamp>> getPatientQualifyMap(int sta3n) {
        java.util.Map<String,java.util.Map<String,java.sql.Timestamp>> result = new java.util.HashMap();
        String query =
                "SELECT patientSSN \n"
                + "     ,measureType \n"
                + "     ,qualify2 \n"
                + "FROM Commend.dbo.CommendVISNEncSummary \n"
                + "WHERE sta3n = " + sta3n + " \n"
                + "ORDER BY patientSSN";
        SQLTable tm = _dest.getTable(query);
        for(Integer i : tm.keySet()) {
            java.util.Map map = tm.getRow(i);
            String ssn = map.get("patientSSN").toString();
            String type = map.get("measureType").toString();
            java.sql.Timestamp ts = (java.sql.Timestamp)map.get("qualify2");
            if(result.get(ssn)==null) {
                java.util.Map<String,java.sql.Timestamp> qualifyingMap = new java.util.HashMap();
                qualifyingMap.put(type, ts);
                result.put(ssn, qualifyingMap);
            }
            else {
                java.util.Map<String,java.sql.Timestamp> qualifyingMap = result.get(ssn);
                qualifyingMap.put(type, ts);
            }
        }
        return result;
    }
// Disabled because OEF4 data is no longer needed.
//    private void _processGraphData() { //Builds json objects for graphing system
//        Timer t = Timer.start();
//        String query = "DELETE FROM Commend.dbo.CommendVISNGraphData";
//        _dest.update(query);
//        java.util.ArrayList<String> patients = _getAllPatientIENs();
//        final float _totalCount = patients.size();
//        class Number { public float n; }
//        final Number _currentCount = new Number();
//        _currentCount.n = 0;
//        _count = 0;
//        final ThreadCyclicManager cycle = new ThreadCyclicManager(new ThreadCyclicEvent() {
//                @Override
//                public void go() {
//                    _count = 0;
//                    System.out.println(_nf.format((_currentCount.n/_totalCount)*100) + "% complete");
//                    SiteGraphingBuilder.insertData(_sta3n, _dest);
//                }
//            });
//        for (String patient : patients) {
//            Thread thread = new SiteGraphingBuilder(_sta3n, patient, _dest);
//            cycle.add(thread);
//            _count++;
//            _currentCount.n++;
//        }
//        cycle.finalizeCycle();
//        t.print();
//    }
// Disabled because OEF4 data is no longer needed.
//    private void _processInstitutionTable() {
//        Timer t = Timer.start();
//        String query = "DELETE FROM Commend.dbo.CommendVISNPatientInstitution";
//        _dest.update(query);
//        query =
//                "SELECT DISTINCT \n"
//                + "	v.Sta3n AS sta3n \n"
//                + "	,p.PatientIEN AS patientIEN \n"
//                + "	,v.PatientSID AS patientSID \n"
//                + "	,v.VisitDateTime AS visitDateTime \n"
//                + "	,i.InstitutionCode AS institutionCode \n"
//                + "	,i.InstitutionName AS institutionName \n"
//                + "FROM \n"
//                + "	VDWWork.Outpat.Visit v \n"
//                + "	,VDWWork.Dim.Institution i \n"
//                + "	,Commend.dbo.CommendVISNPatient p \n"
//                + "WHERE \n"
//                + "	v.VisitDateTime > DATEADD(MONTH,-15, GETDATE()) \n"
//                + "AND v.InstitutionSID = i.InstitutionSID \n"
//                + "AND p.PatientSID = v.PatientSID \n"
//                + "ORDER BY v.PatientSID,v.VisitDateTime DESC";
//        final java.util.List<String> inserts = new java.util.ArrayList();
//        _dest.query(query, new com.medcisive.utility.sql2.SQLObject() {
//            java.util.Map<String,String> current = new java.util.HashMap();
//            java.util.Map<String,String> institutionMap = new java.util.HashMap();
//            com.google.common.collect.Multiset<String> institutionMultiset = com.google.common.collect.HashMultiset.create();
//            int count = 0;
//            String sta3n = null;
//            String patientIEN = null;
//            String patientSID = null;
//            String institutionCode = null;
//            String institutionName = null;
//        
//            @Override
//            public void row(ResultSet rs) throws SQLException {
//                sta3n = rs.getString("sta3n");
//                patientIEN = rs.getString("patientIEN");
//                patientSID = rs.getString("patientSID");
//                institutionCode = rs.getString("institutionCode");
//                institutionName = rs.getString("institutionName");
//                if(current.get("patientSID")==null) {
//                    current.put("sta3n", sta3n);
//                    current.put("patientIEN", patientIEN);
//                    current.put("patientSID", patientSID);
//                    institutionMap.put(institutionCode, institutionName);
//                    institutionMultiset.add(institutionCode);
//                } else {
//                    if(current.get("patientSID").equalsIgnoreCase(patientSID)) {
//                        institutionMap.put(institutionCode, institutionName);
//                        if(count<3) {
//                            institutionMultiset.add(institutionCode);
//                            count++;
//                        }
//                    } else {
//                        int highCount = -1;
//                        String code = null;
//                        for(String s : institutionMultiset.elementSet()) {
//                            int temp = institutionMultiset.count(s);
//                            if(highCount<temp) {
//                                highCount = temp;
//                                code = s;
//                            }
//                        }
//                        String insert =
//                            "INSERT INTO Commend.dbo.CommendVISNPatientInstitution (sta3n,patientIEN,patientSID,institutionCode,institutionName) \n"
//                            + "VALUES ("
//                            + DBC.fixString(sta3n) + ","
//                            + DBC.fixString(patientIEN) + ","
//                            + DBC.fixString(patientSID) + ","
//                            + DBC.fixString(code) + ","
//                            + DBC.fixString(institutionMap.get(code)) + ") \n";
//                        inserts.add(insert);
//                        current.put("sta3n", sta3n);
//                        current.put("patientIEN", patientIEN);
//                        current.put("patientSID", patientSID);
//                        institutionMap.put(institutionCode, institutionName);
//                        institutionMultiset.clear();
//                        institutionMultiset.add(institutionCode);
//                        count = 0;
//                    }
//                }
//            }
//            
//            @Override
//            public void post() {
//                if(sta3n!=null) { // catch the last patient
//                    int highCount = -1;
//                    String code = null;
//                    for(String s : institutionMultiset.elementSet()) {
//                        int temp = institutionMultiset.count(s);
//                        if(highCount<temp) {
//                            highCount = temp;
//                            code = s;
//                        }
//                    }
//                    String insert =
//                        "INSERT INTO Commend.dbo.CommendVISNPatientInstitution (sta3n,patientIEN,patientSID,institutionCode,institutionName) \n"
//                        + "VALUES ("
//                        + DBC.fixString(sta3n) + ","
//                        + DBC.fixString(patientIEN) + ","
//                        + DBC.fixString(patientSID) + ","
//                        + DBC.fixString(code) + ","
//                        + DBC.fixString(institutionMap.get(code)) + ") \n";
//                    inserts.add(insert);
//                }
//            }
//        });
//        
//        java.lang.StringBuilder sb = new java.lang.StringBuilder();
//        for(String s : inserts) {
//            sb.append(s);
//        }
//        _dest.update(sb.toString());
//        t.print();
//    }
}