package com.medcisive.commend.database.site;

import com.medcisive.utility.*;
import com.medcisive.utility.sql2.DBC;
import com.medcisive.utility.sql2.SQLObject;
import java.sql.ResultSet;
import java.sql.SQLException;
/**
 *
 * @author vhapalchambj
 */
public class SiteDatabaseController extends com.medcisive.utility.sql2.DBCUtil {

    private int _sta3n;
    private SiteAnalysisUtility sau = new SiteAnalysisUtility();

    /**
     * Default constructor. SQLUtility must be initialized first, but maybe
     * created anytime after.
     * @param sta3n Station number which to work on.
     */
    public SiteDatabaseController(int sta3n) {
        _sta3n = sta3n;
    }

    public void setupPatientProviderTables() {
        Timer t = Timer.start();
        _startSetupPatientProviderTables();
        _buildTempPatientTable(new java.sql.Timestamp(sau._today.getTime() - sau._yearms), sau._today);
        _markAndCopyTempTable("Y", "N", "N");
        _buildTempPatientTable(sau._fiscalYears.get(1), sau._fiscalYears.get(0));
        _markAndCopyTempTable("N", "Y", "N");
        _buildTempPatientTable(sau._fiscalYears.get(2), sau._fiscalYears.get(1));
        _markAndCopyTempTable("N", "N", "Y");
        _updateOEFOIFStatuses();
        _endSetupPatientProviderTables();
        t.print();
    }

    private void _updateOEFOIFStatuses() {
        /*
         * We do 2 updates:
         *   1. set all OEFOIFService flags to N
         *   2. set OEFOIFService flags to Y when the patient is found in
         *      table CommendVISNOEFOIFService
         */
        Timer t = Timer.start();
        String query =
                "UPDATE Commend.dbo.CommendVISNPatient \n"
                + "SET OEFOIFService = 'Y' \n"
                + "WHERE patientSID IN (select oef.patientSID from VDWWork.PatSub.OEFOIFService oef) ";
        _dest.update(query);
        t.print();
    }

    private void _startSetupPatientProviderTables() {
        String query =
                "DELETE FROM dbo.CommendVISNPatient \n"
                + "DELETE FROM dbo.CommendVISNProvider \n"
                + "DELETE FROM dbo.CommendVISNPatientPanel \n"
                + "IF OBJECT_ID('Commend.dbo.CommendVISNPatientTemp') is not null \n"
                + "DROP TABLE Commend.dbo.CommendVISNPatientTemp \n"
                + "IF OBJECT_ID('Commend.dbo.CommendVISNProviderTemp') is not null \n"
                + "DROP TABLE Commend.dbo.CommendVISNProviderTemp \n"
                + "IF OBJECT_ID('Commend.dbo.CommendVISNPatientPanelTemp') is not null \n"
                + "DROP TABLE Commend.dbo.CommendVISNPatientPanelTemp \n";
        _dest.update(query);
    }

    private void _endSetupPatientProviderTables() {
        String delete =
                "IF OBJECT_ID('Commend.dbo.CommendVISNPatientTemp') is not null \n"
                + "DROP TABLE Commend.dbo.CommendVISNPatientTemp \n"
                + "IF OBJECT_ID('Commend.dbo.CommendVISNProviderTemp') is not null \n"
                + "DROP TABLE Commend.dbo.CommendVISNProviderTemp \n"
                + "IF OBJECT_ID('Commend.dbo.CommendVISNPatientPanelTemp') is not null \n"
                + "DROP TABLE Commend.dbo.CommendVISNPatientPanelTemp \n";
        _dest.update(delete);
        String query =
                "SELECT DISTINCT * \n"
                + "INTO Commend.dbo.CommendVISNPatientTemp \n"
                + "FROM Commend.dbo.CommendVISNPatient \n"
                + "ORDER BY patientName \n"
                + ""
                + "DELETE FROM Commend.dbo.CommendVISNPatient \n"
                + ""
                + "INSERT INTO Commend.dbo.CommendVISNPatient \n"
                + "SELECT * FROM Commend.dbo.CommendVISNPatientTemp \n"
                + ""
                + "SELECT DISTINCT * \n"
                + "INTO Commend.dbo.CommendVISNProviderTemp \n"
                + "FROM Commend.dbo.CommendVISNProvider \n"
                + "ORDER BY providerName \n"
                + ""
                + "DELETE FROM Commend.dbo.CommendVISNProvider \n"
                + ""
                + "INSERT INTO Commend.dbo.CommendVISNProvider \n"
                + "SELECT * FROM Commend.dbo.CommendVISNProviderTemp \n"
                + ""
                + "SELECT * \n"
                + "INTO Commend.dbo.CommendVISNPatientPanelTemp \n"
                + "FROM Commend.dbo.CommendVISNPatientPanel \n"
                + ""
                + "DELETE FROM Commend.dbo.CommendVISNPatientPanel \n";
        _dest.update(query);
        class Provider {
            private java.util.Map<String,java.util.Map<String,String>> patients = new java.util.HashMap();
            public Provider() {}
            public void add(java.sql.ResultSet rs) throws java.sql.SQLException {
                String currSID = rs.getString("patientSID");
                if(patients.containsKey(currSID)) {
                    String flag = "";
                    if(rs.getString("RMFlag").equalsIgnoreCase("Y")) {
                        flag = "RMFlag";
                    } else if(rs.getString("FYM0Flag").equalsIgnoreCase("Y")) {
                        flag = "FYM0Flag";
                    } else if(rs.getString("FYM1Flag").equalsIgnoreCase("Y")) {
                        flag = "FYM1Flag";
                    }
                    if(!flag.isEmpty()) {
                        java.util.Map<String,String> map = patients.get(currSID);
                        map.put(flag, "Y");
                    }
                } else {
                    java.util.Map<String,String> map = new java.util.HashMap();
                    map.put("sta3n", rs.getString("sta3n"));
                    map.put("staffSID", rs.getString("staffSID"));
                    map.put("providerDUZ", rs.getString("providerDUZ"));
                    map.put("providerName", rs.getString("providerName"));
                    map.put("patientSID", rs.getString("patientSID"));
                    map.put("patientIEN", rs.getString("patientIEN"));
                    map.put("patientName", rs.getString("patientName"));
                    map.put("RMFlag", rs.getString("RMFlag"));
                    map.put("FYM0Flag", rs.getString("FYM0Flag"));
                    map.put("FYM1Flag", rs.getString("FYM1Flag"));
                    patients.put(currSID,map);
                }
            }
            public String insert() {
                String result = "";
                for( String key : patients.keySet()) {
                    java.util.Map<String,String> map = patients.get(key);
                    result += "INSERT INTO Commend.dbo.CommendVISNPatientPanel \n"
                            + "VALUES ("
                            + DBC.fixString(map.get("sta3n")) + ","
                            + DBC.fixString(map.get("staffSID")) + ","
                            + DBC.fixString(map.get("providerDUZ")) + ","
                            + DBC.fixString(map.get("providerName")) + ","
                            + DBC.fixString(map.get("patientSID")) + ","
                            + DBC.fixString(map.get("patientIEN")) + ","
                            + DBC.fixString(map.get("patientName")) + ","
                            + DBC.fixString(map.get("RMFlag")) + ","
                            + DBC.fixString(map.get("FYM0Flag")) + ","
                            + DBC.fixString(map.get("FYM1Flag")) + ") \n";
                }
                return result;
            }
        }
        
        final java.util.Map<String,Provider> providers = new java.util.HashMap();
        query =
                "SELECT * \n"
                + "FROM Commend.dbo.CommendVISNPatientPanelTemp \n"
                + "ORDER BY staffSID, patientSID";
        _dest.query(query, new SQLObject() {
            String currSID = null;
            public void row(ResultSet rs) throws SQLException {
                currSID = rs.getString("staffSID");
                if(providers.containsKey(currSID)) {
                    Provider p = providers.get(currSID);
                    p.add(rs);
                } else {
                    Provider p = new Provider();
                    p.add(rs);
                    providers.put(currSID, p);
                }
            }
        });
        for( String key : providers.keySet()) {
            _dest.update(providers.get(key).insert());
        }
        _dest.update(delete);
    }

    private void _buildTempPatientTable(java.sql.Timestamp startDate, java.sql.Timestamp stopDate) {
        Timer t = Timer.start();
        String query =
                "IF OBJECT_ID('Commend.dbo.CommendVISNBuildTempPatientTable') is not null \n"
                + "DROP TABLE Commend.dbo.CommendVISNBuildTempPatientTable \n"
                + "SELECT COUNT(*) AS visitCount, \n"
                + "       pt.sta3n, \n"
                + "       pt.PatientSID AS patientSID, \n"
                + "       pt.PatientIEN AS patientIEN, \n"
                + "       pt.patientSSN AS patientSSN, \n"
                + "       pt.PatientName AS patientName, \n"
                + "       pt.DateOfBirth, \n"
                + "       pt.Gender AS gender, \n"
                + "       staff.staffSID, \n"
                + "       staff.staffIEN, \n"
                + "       staff.staffName \n"
                + "INTO Commend.dbo.CommendVISNBuildTempPatientTable \n"
                + "FROM \n"
                + "     VDWWork.Outpat.Visit         vis, \n"
                + "     VDWWork.Outpat.VProvider     prv, \n"
                + "     VDWWork.SStaff.SStaff        staff, \n"
                + "     VDWWork.SPatient.SPatient    pt \n"
                + "WHERE vis.sta3n = " + _sta3n + " \n"
                + "  AND staff.sta3n = vis.sta3n \n"
                + "  AND prv.sta3n = staff.sta3n \n"
                + "  AND pt.sta3n = prv.sta3n \n"
                + "  AND vis.PrimaryStopCode < 600 \n"
                + "  AND vis.PrimaryStopCode > 499 \n"
                + "  AND vis.visitDateTime > " + DBC.fixTimestamp(startDate) + " \n"
                + "  AND vis.VisitDateTime < " + DBC.fixTimestamp(stopDate) + " \n"
                + "  AND prv.visitSID = vis.visitSID \n"
                + "  AND staff.StaffSID =  prv.ProviderSID \n"
                + "  AND pt.PatientSID =   vis.PatientSID \n"
                + "GROUP BY pt.sta3n, pt.PatientSID, pt.PatientIEN, pt.patientSSN, pt.patientName, pt.DateOfBirth, \n"
                + "         pt.Gender, staff.StaffSID, staff.staffIEN, staff.staffName \n"
                + "ORDER BY pt.patientSSN, pt.patientName, staff.staffName";
        _dest.update(query);
        t.print();
    }

    private void _markAndCopyTempTable(String rollingMeasure, String currentFiscalYear, String lastFiscalYear) {
        Timer t = Timer.start();
        String query =
                "INSERT INTO Commend.dbo.CommendVISNPatient \n"
                + "SELECT \n"
                + "     sta3n \n"
                + "	,patientSID \n"
                + "     ,patientIEN \n"
                + "     ,patientSSN \n"
                + "     ,patientName \n"
                + "     ,DateOfBirth \n"
                + "     ,gender \n"
                + "     ,'N' as OEFOIFService \n"
                + "FROM dbo.CommendVISNBuildTempPatientTable \n"
                + "GROUP BY sta3n,patientSID,patientIEN,patientSSN,patientName,DateOfBirth,gender \n"
                + "HAVING SUM(visitCount) > 1 \n"
                + "ORDER BY PatientName";
        _dest.update(query);
        query =
                "INSERT INTO Commend.dbo.CommendVISNProvider \n"
                + "SELECT DISTINCT \n"
                + "     sta3n \n"
                + "     ,staffSID \n"
                + "     ,staffIEN \n"
                + "     ,staffName \n"
                + "FROM dbo.CommendVISNBuildTempPatientTable";
        _dest.update(query);
        query =
                "INSERT INTO Commend.dbo.CommendVISNPatientPanel \n"
                + "SELECT DISTINCT \n"
                + "     sta3n \n"
                + "     ,staffSID \n"
                + "     ,staffIEN \n"
                + "     ,staffName \n"
                + "     ,patientSID \n"
                + "     ,patientIEN \n"
                + "     ,patientName \n"
                + "     ,'" + rollingMeasure    + "' AS RMFlag \n"
                + "     ,'" + currentFiscalYear + "' AS FYM0Flag \n"
                + "     ,'" + lastFiscalYear    + "' AS FYM1Flag \n"
                + "FROM Commend.dbo.CommendVISNBuildTempPatientTable \n"
                + "GROUP BY sta3n,staffSID,staffIEN,staffName,patientSID,patientIEN,patientSSN,patientName \n"
                + "HAVING SUM(visitCount) > 1 \n"
                + "ORDER BY PatientName";
        _dest.update(query);
        t.print();
    }

    private void setupTestTables() {
        String query =
                "IF OBJECT_ID('Commend.dbo.CommendTempVISNProviderPatientTable') is not null \n"
                + "DROP TABLE Commend.dbo.CommendTempVISNProviderPatientTable \n"
                + "SELECT COUNT(*) AS encounterCount, \n"
                + "       enc.sta3n, \n"
                + "       staff.staffIEN, \n"
                + "       staff.staffName, \n"
                + "       pt.PatientIEN, \n"
                + "       pt.patientName, \n"
                + "       pt.patientSSN, \n"
                + "       pt.DateOfBirth, \n"
                + "       race.Race, \n"
                + "       pt.Gender \n"
                + "INTO Commend.dbo.CommendTempVISNProviderPatientTable  \n"
                + "FROM Enc.FactAmbEnc enc,  \n"
                + "     Enc.FactAmbEncProvider eProv, \n"
                + "     SStaff.SStaff staff, \n"
                + "     SPatient.SPatient pt, \n"
                + "     Dim.Race race \n"
                + "WHERE  \n"
                + "        enc.sta3n = " + _sta3n + " \n"
                + "    AND eProv.sta3n = enc.sta3n \n"
                + "    AND staff.sta3n = eProv.sta3n \n"
                + "    AND pt.sta3n = staff.sta3n \n"
                + "    AND enc.clinicStop in (502,509,510,513,516, \n"
                + "                           532,540,550,552,557, \n"
                + "                           558,559,560,561,562, \n"
                + "                           567,571,572,582,583) \n"
                + "    AND enc.EncounterDateTime > DATEADD(year,-1, GETDATE()) \n"
                + "    AND eProv.EncounterID = enc.EncounterID \n"
                + "    AND staff.staffIEN = eProv.providerIEN \n"
                + "    AND pt.PatientIEN = enc.patientIEN \n"
                + "    AND race.RaceSID = pt.RaceSID \n"
                + "GROUP BY staffName, staffIEN, patientName, patientSSN, enc.sta3n, pt.PatientIEN, pt.DateOfBirth, pt.Race, pt.Gender \n"
                + "HAVING COUNT(*) > 1 \n"
                + "ORDER BY staff.staffname, pt.patientName";
        Timer t = Timer.start();
        _dest.update(query);
        t.print();
    }

    public void breakdownTestTables() {
        String query =
                "IF OBJECT_ID('Commend.dbo.CommendTempVISNProviderPatientTable') is not null \n"
                + "DROP TABLE Commend.dbo.CommendTempVISNProviderPatientTable";
        Timer t = Timer.start();
        _dest.update(query);
        t.print();
    }

    public void insertProviders() {
        String query =
                "INSERT INTO Commend.dbo.CommendVISNProvider (sta3n, providerDUZ, providerName) \n"
                + "SELECT DISTINCT sta3n \n"
                + "      ,staffIEN \n"
                + "      ,staffName \n"
                + "FROM Commend.dbo.CommendTempVISNProviderPatientTable";
        Timer t = Timer.start();
        _dest.update("DELETE FROM Commend.dbo.CommendVISNProvider");
        _dest.update(query);
        t.print();
    }

    public void insertPatients() {
        String query =
                "INSERT INTO Commend.dbo.CommendVISNPatients (sta3n,patientIEN,patientSSN,patientName,DateOfBirth,race,gender) \n"
                + "SELECT \n"
                + "   sta3n, \n"
                + "   PatientIEN, \n"
                + "   patientSSN, \n"
                + "   patientName, \n"
                + "   DateOfBirth, \n"
                + "   Race, \n"
                + "   Gender \n"
                + "FROM Commend.dbo.CommendTempVISNProviderPatientTable";
        Timer t = Timer.start();
        _dest.update("DELETE FROM Commend.dbo.CommendVISNPatients");
        _dest.update(query);
        t.print();
    }

    public Thread insertDiagnosis() {
        if (_sta3n<=0) {
            return null;
        }
        class InsertDiagnosisThread extends Thread {
            @Override
            public void run() {
                insertDiagnosis();
            }
            private void insertDiagnosis() {
                Timer t = Timer.start();
                com.medcisive.utility.sql2.DBC dbc = _dest.clone();
                java.sql.Timestamp ts = new java.sql.Timestamp(sau._fiscalYears.get(1).getTime() - (sau._yearms * 7));
                String query = "DELETE FROM Commend.dbo.CommendVISNDiagnosis WHERE sta3n = " + _sta3n;
                dbc.update(query);
                query =
                        "INSERT INTO Commend.dbo.CommendVISNDiagnosis \n"
                        + "SELECT DISTINCT \n"
                        + "     " + _sta3n + ", \n"
                        + "     pt.patientSID, \n"
                        + "     pt.patientIEN, \n"
                        + "     pt.patientSSN, \n"
                        + "     vis.visitSID, \n"
                        + "     vis.visitDateTime, \n"
                        + "     vis.primaryStopCode, \n"
                        + "     vis.SecondaryStopCode, \n"
                        + "     icdmap.ICDCode, \n"
                        + "     dia.primarySecondary \n"
                        + "FROM VDWWork.Outpat.Visit vis, \n"
                        + "     VDWWork.Outpat.VDiagnosis dia, \n"
                        + "     Commend.dbo.CommendVISNPatient pt, \n"
                        + "     VDWWork.Dim.ICD icdmap \n"
                        + "WHERE vis.sta3n = " + _sta3n + " \n"
                        + "  AND pt.sta3n = vis.sta3n \n"
                        + "  AND dia.sta3n = pt.sta3n \n"
                        + "  AND icdmap.sta3n = dia.sta3n \n"
                        + "  AND vis.patientSID = pt.patientSID \n"
                        + "  AND vis.visitDateTime > " + DBC.fixTimestamp(ts) + " \n"
                        + "  AND dia.VisitSID = vis.VisitSID \n"
                        + "  AND dia.patientSID = pt.patientSID \n"
                        + "  AND icdmap.ICDSID = dia.ICDSID \n"
                        + "ORDER BY patientSSN, vis.visitDateTime";
                dbc.update(query);
                t.print();
            }
        }
        InsertDiagnosisThread idt = new InsertDiagnosisThread();
        idt.start();
        return idt;
    }

    public Thread insertProcedure() {
        if (_sta3n<=0) {
            return null;
        }
        class InsertProcedureThread extends Thread {
            @Override
            public void run() {
                insertProcedure();
            }
            private void insertProcedure() {
                Timer t = Timer.start();
                com.medcisive.utility.sql2.DBC dbc = _dest.clone();
                java.sql.Timestamp ts = new java.sql.Timestamp(sau._fiscalYears.get(1).getTime() - (sau._yearms * 7));
                String query =
                        "DELETE FROM Commend.dbo.CommendVISNProcedure \n"
                        + ""
                        + "INSERT INTO Commend.dbo.CommendVISNProcedure \n"
                        + "SELECT distinct " + _sta3n + ", \n"
                        + "       pt.patientSID, \n"
                        + "       pt.patientIEN, \n"
                        + "       pt.patientSSN, \n"
                        + "       vis.visitSID, \n"
                        + "       vis.visitDateTime, \n"
                        + "       cptmap.CPTCode, \n"
                        + "       cptmap.CPTName \n"
                        + "FROM VDWWork.Outpat.Visit vis, \n"
                        + "     VDWWork.Outpat.VProcedure pro, \n"
                        + "     Commend.dbo.CommendVISNPatient pt, \n"
                        + "     VDWWork.Dim.CPT cptmap \n"
                        + "WHERE vis.sta3n = " + _sta3n + " \n"
                        + "  AND pt.sta3n = vis.sta3n \n"
                        + "  AND pro.sta3n = pt.sta3n \n"
                        + "  AND cptmap.sta3n = pro.sta3n \n"
                        + "  AND vis.patientSID = pt.patientSID \n"
                        + "  AND vis.visitDateTime > " + DBC.fixTimestamp(ts) + " \n"
                        + "  AND pro.VisitSID = vis.VisitSID \n"
                        + "  AND pro.patientSID = pt.patientSID \n"
                        + "  AND cptmap.CPTSID = pro.CPTSID \n"
                        + "ORDER BY patientSSN, vis.visitDateTime";
                dbc.update(query);
                t.print();
            }
        }
        InsertProcedureThread ipt = new InsertProcedureThread();
        ipt.start();
        return ipt;
    }

    public Thread insertCommendVISNPrvdrEncounters() {
        if (_sta3n<=0) {
            return null;
        }
        class InsertCommendVISNPrvdrEncounters extends Thread {
            @Override
            public void run() {
                _insertCommendVISNPrvdrEncounters();
            }
            private void _insertCommendVISNPrvdrEncounters() {
                Timer t = Timer.start();
                com.medcisive.utility.sql2.DBC dbc = _dest.clone();
                String query =
                        "DELETE FROM Commend.dbo.CommendVISNPrvdrEncounters \n"
                        + "INSERT INTO Commend.dbo.CommendVISNPrvdrEncounters \n"
                        + "select distinct \n"
                        + "	vprov.sta3n AS staffSta3n, \n"
                        + "	stf.staffSID, \n"
                        + "	stf.staffIEN, \n"
                        + "	stf.staffName, \n"
                        + "	vprov.visitSID, \n"
                        + "	vprov.visitDateTime, \n"
                        + "	pt.sta3n AS patientSta3n, \n"
                        + "	vprov.patientSID, \n"
                        + "     pan.patientIEN, \n"
                        + "	pt.patientName, \n"
                        + "	vst.primaryStopCode, \n"
                        + "	inst.institutionCode, \n"
                        + "	inst.institutionName, \n"
                        + "	vprov.primarySecondary \n"
                        + "from \n"
                        + "	Commend.dbo.CommendVISNPatientPanel pan, \n"
                        + "	VDWWork.Outpat.VProvider vprov, \n"
                        + "	VDWWork.Outpat.Visit vst, \n"
                        + "	VDWWork.SStaff.SStaff stf, \n"
                        + "	VDWWork.SPatient.SPatient pt, \n"
                        + "	VDWWork.Dim.institution inst \n"
                        + "where \n"
                        + "	vprov.sta3n = " + _sta3n + " \n"
                        + "	and pan.sta3n = vprov.sta3n \n"
                        + "	and stf.sta3n = pan.sta3n \n"
                        + "	and pt.sta3n = pt.sta3n \n"
                        + "	and pan.staffSID = vprov.providerSID \n"
                        + "	and stf.staffSID = pan.staffSID \n"
                        + "	and pt.patientSID = vprov.patientSID \n"
                        + "	and vst.visitSID = vprov.visitSID \n"
                        + "     and pan.patientSID = vprov.patientSID \n"
                        + "	and inst.institutionSID = vst.InstitutionSID \n"
                        + "	and vprov.visitDateTime > DATEADD(MONTH,-15, GETDATE()) \n"
                        + "order by \n"
                        + "	vprov.VisitDateTime, stf.staffName, pt.patientName";
                dbc.update(query);
                t.print();
            }
        }
        InsertCommendVISNPrvdrEncounters icvpe = new InsertCommendVISNPrvdrEncounters();
        icvpe.start();
        return icvpe;
    }

    public void logEvent(String event, int eventId) {
        logEvent(null, null, null, null, null, event, eventId);
    }

    public void logEvent(java.sql.Timestamp eventtime, java.sql.Timestamp logtime, String patientSSN, String providerDUZ, String clientComputer, String event, int eventId) {
        if (eventtime == null) {
            eventtime = new java.sql.Timestamp(System.currentTimeMillis());
        }
        if (logtime == null) {
            logtime = new java.sql.Timestamp(System.currentTimeMillis());
        }
        String q = "INSERT INTO Commend.dbo.CommendLogTrace (eventtime,logtime,patientSSN,providerDUZ,clientComputer,event,eventID) \n"
                + "VALUES(" +  DBC.fixTimestamp(eventtime) + "," + DBC.fixTimestamp(logtime) + ","
                + DBC.fixString(patientSSN) + "," + DBC.fixString(providerDUZ) + ","
                + DBC.fixString(clientComputer) + "," + DBC.fixString(event) + "," + eventId + ");";
        _dest.update(q);
    }
}
