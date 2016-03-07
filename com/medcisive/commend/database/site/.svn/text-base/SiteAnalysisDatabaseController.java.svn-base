package com.medcisive.commend.database.site;

import com.medcisive.utility.Timer;
import com.medcisive.utility.sql2.DBC;
import com.medcisive.utility.sql2.SQLTable;

/**
 *
 * @author vhapalchambj
 */
public class SiteAnalysisDatabaseController extends com.medcisive.utility.sql2.DBCUtil {

    private static String _PTSDcptCodes = "'90801', '90806', '90807', '90808', '90809', '90818', '90819', '90821', '90822', '90847', '90853'";
    private int _sta3n;
    private StringBuilder buffer = new StringBuilder();
    private int _count = 0;
    private int _bufferSize = 1000;

    public SiteAnalysisDatabaseController(int sta3n) {
        _sta3n = sta3n;
    }

    public SQLTable _getPatientDiagnosisTableMap(String ssn) {
        if (ssn == null) {
            return null;
        }
        String query =
                "SELECT distinct \n"
                + "     diag.patientSSN AS SSN \n"
                + "     ,cast(floor(cast(diag.visitDateTime as float)) as datetime) AS encounterDateFloor \n"
                + "FROM Commend.dbo.CommendVISNDiagnosis diag, \n"
                + "     Commend.dbo.CommendVISNProcedure pro \n"
                + "WHERE diag.patientSSN = '" + ssn + "' \n"
                + "  AND pro.patientSSN = diag.patientSSN \n"
                + "  AND diag.icd9 = '309.81' \n"
                + "  AND diag.primarySecondary = 'P' \n"
                + "  AND diag.sta3n = '" + _sta3n + "' \n"
                + "  AND diag.PrimaryStopCode < 600 \n"
                + "  AND diag.PrimaryStopCode > 499 \n"
                + "ORDER BY encounterDateFloor";
        SQLTable tm = _dest.getTable(query);
        return tm;
    }
    
    public void update(String update){
        _dest.update(update);
    }

    public SQLTable _getPTSDTherapyTableMap(String ssn) {
        if (ssn == null) {
            return null;
        }
        String query =
                "SELECT \n"
                + "     pro.patientSSN AS SSN \n"
                + "     ,cast(floor(cast(diag.visitDateTime as float)) as datetime) AS encounterDateFloor \n"
                + "     ,max(pro.cptCode) AS cptCode \n"
                + "FROM Commend.dbo.CommendVISNProcedure pro, \n"
                + "     Commend.dbo.CommendVISNDiagnosis diag \n"
                + "WHERE cptCode in (" + _PTSDcptCodes + ") \n"
                + "  AND pro.visitSID = diag.visitSID \n"
                + "  AND diag.icd9 = '309.81' \n"
                + "  AND pro.patientSSN = '" + ssn + "' \n"
                + "  AND diag.patientSSN = pro.patientSSN \n"
                + "  AND pro.sta3n = '" + _sta3n + "' \n"
                + "  AND diag.sta3n = pro.sta3n \n"
                + "GROUP BY cast(floor(cast(diag.visitDateTime as float)) as datetime), pro.patientSSN \n"
                + "ORDER BY SSN, encounterDateFloor";
        SQLTable tm = _dest.getTable(query);
        return tm;
    }

    public void updateAnalysis(String ssn, java.sql.Timestamp ts, String cpt, String id, String column) {
        if ((ssn == null) || (ts == null) || (cpt == null) || (id == null) || (column == null)) {
            return;
        }
        String update =
                "UPDATE Commend.dbo.CommendVISNEncAnalysis \n"
                + "SET " + column + " = '" + id + "' \n"
                + "WHERE SSN = '" + ssn + "' \n"
                + " AND encounterDateFloor = '" + ts + "' \n"
                + " AND cptCode = '" + cpt + "' \n"
                + " AND " + column + " is NULL \n"
                + " AND sta3n = '" + _sta3n + "'";
        insertIntoBuffer(update);
    }

    public void insertDetailBatch(String sta3n, String ssn, String ien, String name,
                             String fy, java.sql.Timestamp dx1, java.sql.Timestamp dx2, java.sql.Timestamp first,
                             java.sql.Timestamp last, String completed, java.sql.Timestamp week14, java.sql.Timestamp deadline) {
        if ((sta3n == null) || (ssn == null) ||(ien == null) ||(name == null) ||(fy == null) ||
                (dx1 == null) ||(dx2 == null) || (completed == null) || (deadline == null)) {
            return;
        }
        sta3n = DBC.fixString(sta3n);
        ssn = DBC.fixString(ssn);
        ien = DBC.fixString(ien);
        name = DBC.fixString(name);
        fy = DBC.fixString(fy);
        String sdx1 = DBC.fixTimestamp(dx1);
        String sdx2 = DBC.fixTimestamp(dx2);
        String sfirst = DBC.fixTimestamp(first);
        String slast = DBC.fixTimestamp(last);
        completed = DBC.fixString(completed);
        String sweek14 = DBC.fixTimestamp(week14);
        String sdeadline = DBC.fixTimestamp(deadline);

        String update =
                "INSERT INTO Commend.dbo.CommendVISNDetail(sta3n,ssn,ien,name,fy,dx1,dx2,first,last,completed,week14,deadline) \n"
                + "VALUES ("+ sta3n + ","+ ssn + ","+ ien + ","+ name + ","+ fy + ","+ sdx1 + ","+ sdx2 + ","
                + ""+ sfirst + ","+ slast + ","+ completed + ","+ sweek14 + ","+ sdeadline + ")";
        insertIntoBuffer(update);
    }

    public SQLTable getPatientInformation(String ssn) {
        if(ssn == null) { return null; }
        String query =
                "SELECT * \n"
                + "FROM Commend.dbo.CommendVISNPatient \n"
                + "WHERE patientSSN = '" + ssn + "'";
        return _dest.getTable(query);
    }

    public void updateAnalysisBatch(String ssn, java.sql.Timestamp ts, String cpt, String id, String column) {
        if ((ssn == null) || (ts == null) || (cpt == null) || (id == null) || (column == null)) {
            return;
        }
        String update =
                "UPDATE Commend.dbo.CommendVISNEncAnalysis \n"
                + "SET " + column + " = '" + id + "' \n"
                + "WHERE SSN = '" + ssn + "' \n"
                + " AND encounterDateFloor = '" + ts + "' \n"
                + " AND cptCode = '" + cpt + "' \n"
                + " AND " + column + " is NULL \n"
                + " AND sta3n = '" + _sta3n + "'";
        insertIntoBuffer(update);
    }

    public void insertAnalysisBatch(String ssn, java.sql.Timestamp ts, String cpt, String id, String column) {
        if ((ssn == null) || (ts == null) || (cpt == null) || (id == null) || (column == null)) {
            return;
        }
        String insert =
                "INSERT INTO Commend.dbo.CommendVISNEncAnalysisMapper(SSN,encounterDateFloor,cptCode,sta3n) \n"
                + "VALUES (" + DBC.fixString(ssn) + "," + DBC.fixTimestamp(ts) + "," + DBC.fixString(cpt) + "," + _sta3n + ")";
        insertIntoBuffer(insert);
    }

    public void clearBatch() {
        finializeBuffer();
    }

    public void classifyOtherTargetTherapies() {
        Timer t = Timer.start();
        String update =
                "UPDATE Commend.dbo.CommendVISNEncAnalysis \n"
                + "SET RMCatID = 4 \n"
                + "WHERE cptCode in ('90801', '90806', '90807', '90808', '90809', '90818', '90819', '90821', '90822', '90847', '90853') \n"
                + "  AND RMCatID is NULL \n"
                + "  AND sta3n = '" + _sta3n + "'";
        _dest.update(update);
        t.print();
    }

    public void classifyOtherNonTargetTherapies() {
        Timer t = Timer.start();
        String update =
                "UPDATE Commend.dbo.CommendVISNEncAnalysis \n"
                + "SET RMCatID = 5 \n"
                + "WHERE RMCatID is NULL \n"
                + "  AND sta3n = '" + _sta3n + "'";
        _dest.update(update);
        t.print();
    }
    
    private void insertIntoBuffer(String query) {
        buffer.append(query);
        _count++;
        if (_count > _bufferSize) {
            _count = 0;
            _dest.update(buffer.toString());
            buffer.setLength(0);
        }
    }
    private void finializeBuffer() {
        _dest.update(buffer.toString());
        buffer.setLength(0);
    }
}
