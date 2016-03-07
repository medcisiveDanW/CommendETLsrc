package com.medcisive.commend.database.site;

import com.medcisive.utility.LogUtility;
import com.medcisive.utility.sql2.DBC;
import com.medcisive.utility.sql2.SQLTable;
import java.util.Calendar;
import java.util.Collections;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author vhapalchambj
 */
public class SitePatientAnalysis extends SiteAnalysisUtility {

    private String _ssn;
    private String _sta3n;
    private String _ien;
    private String _sid;
    private String _name;
    private SiteAnalysisDatabaseController _dbc;
    private java.util.Map<String, java.sql.Timestamp> _qualifying;
    private SQLTable _qualifyingTableMap = null;
    private java.util.List<java.sql.Timestamp> _completedTherapyList;
    private SQLTable _therapyTableMap = null;
    private java.util.List<java.sql.Timestamp> _possibleTherapyList;
    public static java.util.Collection<String> _analysisUpdates = Collections.synchronizedCollection(new java.util.ArrayList());
    public static java.util.Collection<String> _inserts = Collections.synchronizedCollection(new java.util.ArrayList());
    public static java.util.Collection<String> _batch = Collections.synchronizedCollection(new java.util.ArrayList());
    public static float totalPatients = 0;
    private static float _completedPatients = 0;
    private final static java.text.NumberFormat _nf = java.text.NumberFormat.getInstance();
    private static final Lock _lock = new ReentrantLock(true);

    static {
        _nf.setMaximumFractionDigits(2);
        _nf.setMinimumFractionDigits(2);
    }

    public SitePatientAnalysis(String ssn, SiteAnalysisDatabaseController dbc) {
        _ssn = ssn;
        _dbc = dbc;
        _getPatientInformation();
    }

    @Override
    public void run() {
        go();
    }

    private void go() {
        _calculateMeasure(_today, "RM");
        _calculateMeasure(_fiscalYears.get(0), "FYM0");
        _calculateMeasure(_fiscalYears.get(1), "FYM1");
        _completedPatients++;
        _qualifyingTableMap = null;
        _therapyTableMap = null;
    }

    private void _calculateMeasure(java.sql.Timestamp today, String type) {
        if (today == null) {
            return;
        }
        _completedTherapyList = null;
        _possibleTherapyList = null;
        _qualifying = null;
        _getQualifying(new java.sql.Timestamp(today.getTime() - _yearms), today);
        if (_qualifying != null) {
            java.sql.Timestamp start = new java.sql.Timestamp(_qualifying.get("first").getTime() - (_nintyEightDays + (_yearms * 5)));
            java.sql.Timestamp end = new java.sql.Timestamp(_qualifying.get("second").getTime() + _yearms);
            _get8in14(start, end);
            _getPossible8in14(start, end);
        }
        _updateInsert(type);
    }

    private void _getQualifying(java.sql.Timestamp start, java.sql.Timestamp end) {
        if (_qualifyingTableMap == null) {
            _qualifyingTableMap = _dbc._getPatientDiagnosisTableMap(_ssn);
        }
        if (_qualifyingTableMap == null) { return; }
        java.util.List<java.sql.Timestamp> list = new java.util.ArrayList();
        for (Integer i : _qualifyingTableMap.keySet()) {
            java.util.LinkedHashMap<String, Object> map = _qualifyingTableMap.getRow(i);
            java.sql.Timestamp ts = (java.sql.Timestamp) map.get("encounterDateFloor");
            list.add(ts);
        }
        if ((list != null) && (!list.isEmpty())) {
            _qualifying = getQualifyingVisit(start, end, list);
        }
    }

    private void _get8in14(java.sql.Timestamp start, java.sql.Timestamp end) {
        if (_therapyTableMap == null) {
            _therapyTableMap = _dbc._getPTSDTherapyTableMap(_ssn);
        }
        if (_therapyTableMap == null) { return; }
        java.util.List<java.sql.Timestamp> list = new java.util.ArrayList();
        for (Integer i : _therapyTableMap.keySet()) {
            java.util.LinkedHashMap<String, Object> map = _therapyTableMap.getRow(i);
            java.sql.Timestamp ts = (java.sql.Timestamp) map.get("encounterDateFloor");

            if ((ts.getTime() < end.getTime()) && (ts.getTime() >= start.getTime())) {
                list.add(ts);
            }
        }

        if ((list != null) && (!list.isEmpty())) {
            _completedTherapyList = _get8in14(start, end, list);
        }
    }

    private void _getPossible8in14(java.sql.Timestamp start, java.sql.Timestamp end) {
        if ((_qualifying == null) || (_completedTherapyList != null)) {
            return;
        }
        if(_today.getTime()>end.getTime()) {
            return;
        }
        java.sql.Timestamp _14weeksAgo = new java.sql.Timestamp(_today.getTime() - _nintyEightDays);
        java.sql.Timestamp deadline = _today;
        java.util.List<java.sql.Timestamp> list = new java.util.ArrayList();
        for (Integer i : _therapyTableMap.keySet()) {
            java.util.LinkedHashMap<String, Object> map = _therapyTableMap.getRow(i);
            java.sql.Timestamp ts = (java.sql.Timestamp) map.get("encounterDateFloor");
            if ((ts.getTime() < deadline.getTime()) && (ts.getTime() >= _14weeksAgo.getTime())) {
                list.add(ts);
            }
        }
        if ((list != null) && (!list.isEmpty())) { // list at this point has removed non possilbe before 14 weeks ago
            _possibleTherapyList = _hasPossible8in14(list,deadline);
        }
    }

    public static void executeInsert(com.medcisive.utility.sql2.DBC _sql) {
        _lock.lock();
        try {
            String result =
                    "INSERT INTO Commend.dbo.CommendVISNEncSummary (sta3n,patientSID,patientIEN,patientSSN,patientName,"
                    + "measureType,status,qualify1,qualify2,windowStart,windowEnd,numCompleted,deadline,"
                    + "window1,window2,window3,window4,window5,window6,window7,window8) \n";
            StringBuilder sb = new StringBuilder(1000);
            for (String s : _inserts) {
                sb.append(s);
            }
            _inserts.clear();
            result += sb.toString();
            int index = result.lastIndexOf("UNION ALL");
            if(index>0) {
                result = result.substring(0, index);
                _sql.update(result);
            }
        } catch(java.lang.Exception e) {
            LogUtility.error(e);
        } finally {
            _lock.unlock();
        }
    }

    private void _updateInsert(String type) {
        java.util.Map<String, Object> map = new java.util.HashMap();
        java.util.List<java.sql.Timestamp> list = getList();
        map.put("sta3n", _sta3n);
        map.put("patientSID", _sid);
        map.put("patientIEN", _ien);
        map.put("patientSSN", _ssn);
        map.put("patientName", _name);
        map.put("measureType", type);
        java.sql.Timestamp first = null;
        java.sql.Timestamp last = null;
        java.sql.Timestamp deadline = null;

        if (_qualifying != null) {
            // if complete list = 8 && list.last() >= qualify1
            // else if excluded list = 8 && list.last() < qualify1
            // else if failed deadline < today
            // else incomplete
            deadline = new java.sql.Timestamp(_qualifying.get("second").getTime() + _yearms);
            map.put("qualify1", _qualifying.get("first"));
            map.put("qualify2", _qualifying.get("second"));
            map.put("deadline", deadline);

            long windowStart = _today.getTime();
            long windowEnd = windowStart + _nintyEightDays;
            java.sql.Timestamp windowStartTS = null;
            java.sql.Timestamp windowEndTS = null;
            if (list != null && (!list.isEmpty())) {
                first = list.get(0);
                last = list.get(list.size() - 1);
                windowStart = first.getTime();
                windowEnd = first.getTime() + _nintyEightDays;
            }
            if (windowEnd > deadline.getTime()) {
                windowEnd = deadline.getTime();
            }
            if (windowStart < deadline.getTime()) {
                windowStartTS = new java.sql.Timestamp(windowStart);
                windowEndTS = new java.sql.Timestamp(windowEnd);
            }
            map.put("windowStart", windowStartTS);
            map.put("windowEnd", windowEndTS);

            if ((list != null) && (list.size() == 8) && (last.getTime() >= _qualifying.get("first").getTime())) {
                map.put("status", "Complete");
                for (int i = 0; i < list.size(); i++) {
                    map.put("window" + (i + 1), list.get(i));
                }
                map.put("numCompleted", "" + list.size());
            } else if ((list != null) && (list.size() == 8) && (last.getTime() < _qualifying.get("first").getTime())) {
                map.put("status", "Exclude");
                for (int i = 0; i < list.size(); i++) {
                    map.put("window" + (i + 1), list.get(i));
                }
                map.put("numCompleted", "" + list.size());
            } else if (deadline.getTime() < _today.getTime()) {
                map.put("status", "Failed");
                map.put("numCompleted", "0");
            } else {
                map.put("status", "Incomplete");
                if (list != null) {
                    for (int i = 0; i < list.size(); i++) {
                        map.put("window" + (i + 1), list.get(i));
                    }
                    map.put("numCompleted", "" + list.size());
                } else {
                    map.put("numCompleted", "0");
                }
            }
        } else {
            map.put("status", "NoQualifyingDate");
            map.put("numCompleted", "" + 0);
        }

        String result = getInsertSummaryString(map);
        _lock.lock();
        try {
            _inserts.add(result);
        } finally {
            _lock.unlock();
        }
    }

    private java.util.List<java.sql.Timestamp> getList() {
        java.util.List<java.sql.Timestamp> result = null;
        if (_completedTherapyList != null && !_completedTherapyList.isEmpty()) {
            if (_completedTherapyList.size() > 8) {
                System.out.println(_ssn + " : " + _completedTherapyList + " (completed)Greater than 8!!!");
            }
            result = _completedTherapyList;
        } else if (_possibleTherapyList != null && !_possibleTherapyList.isEmpty()) {
            if (_possibleTherapyList.size() > 8) {
                System.out.println(_ssn + " : " + _possibleTherapyList + " (possible)Greater than 8!!!");
            }
            result = _possibleTherapyList;
        }
        return result;
    }

    private void _updateDetail(String fy) {
        if (_qualifying == null) {
            return;
        }
        java.sql.Timestamp dx1 = _qualifying.get("first");
        java.sql.Timestamp dx2 = _qualifying.get("second");
        java.sql.Timestamp first = null;
        java.sql.Timestamp last = null;
        String completed = "0";
        java.sql.Timestamp week14 = null;
        java.sql.Timestamp deadline = new java.sql.Timestamp(dx2.getTime() + _yearms);
        if (_completedTherapyList != null) {
            first = _completedTherapyList.get(0);
            last = _completedTherapyList.get(_completedTherapyList.size() - 1);
            completed = "" + _completedTherapyList.size();
        } else if (_possibleTherapyList != null) {
            first = _possibleTherapyList.get(0);
            last = _possibleTherapyList.get(_possibleTherapyList.size() - 1);
            completed = "" + _possibleTherapyList.size();
        }
        if ((first != null) && (last != null)) {
            week14 = new java.sql.Timestamp(first.getTime() + _nintyEightDays);
            if ((deadline.getTime() < last.getTime()) || (last.getTime() < dx1.getTime())) {
                return;
            }
        }
        _dbc.insertDetailBatch(_sta3n, _ssn, _ien, _name, fy, dx1, dx2, first, last, completed, week14, deadline);
    }

    private void _getPatientInformation() {
        SQLTable tm = _dbc.getPatientInformation(_ssn);
        java.util.Map<String, Object> map = tm.getRow(0);
        _sta3n = map.get("sta3n").toString();
        _ien = (String) map.get("patientIEN");
        _name = (String) map.get("patientName");
        _sid = "" + (Integer) map.get("patientSID");
    }

    public static void _progress() {
        float precentageComplete = (_completedPatients / totalPatients) * 100;
        System.out.println("Patient processing:  " + _nf.format(precentageComplete) + "% complete");
    }

    private String getInsertSummaryString(java.util.Map<String, Object> map) {
        String result =
                "SELECT "
                + DBC.fixString((String) map.get("sta3n")) + ","
                + DBC.fixString((String) map.get("patientSID")) + ","
                + DBC.fixString((String) map.get("patientIEN")) + ","
                + DBC.fixString((String) map.get("patientSSN")) + ","
                + DBC.fixString((String) map.get("patientName")) + ","
                + DBC.fixString((String) map.get("measureType")) + ","
                + DBC.fixString((String) map.get("status")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp) map.get("qualify1")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp) map.get("qualify2")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp) map.get("windowStart")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp) map.get("windowEnd")) + ","
                + DBC.fixString((String) map.get("numCompleted")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp) map.get("deadline")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp) map.get("window1")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp) map.get("window2")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp) map.get("window3")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp) map.get("window4")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp) map.get("window5")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp) map.get("window6")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp) map.get("window7")) + ","
                + DBC.fixTimestamp((java.sql.Timestamp) map.get("window8")) + " \n"
                + "UNION ALL \n";
        return result;
    }

    private void _sleep(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (java.lang.InterruptedException e) {
            LogUtility.error(e);
            System.err.println("sleep failed(DatabaseController):" + e);
        }
    }
}
