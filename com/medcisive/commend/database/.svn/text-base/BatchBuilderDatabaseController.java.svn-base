package com.medcisive.commend.database;

import com.medcisive.commend.database.encounter.CommendEncSummaryData;
import com.medcisive.commend.database.encounter.CommendEncounterData;
import com.medcisive.utility.*;
import com.medcisive.utility.sql2.DBC;
import com.medcisive.utility.sql2.SQLObject;
import java.sql.*;
import java.util.ArrayList;

/**
 *
 * @author vhapalchambj
 */
public class BatchBuilderDatabaseController extends com.medcisive.utility.sql2.DBCUtil {

    private int _startDay = Integer.parseInt(_properties.getProperty("START_DAY"));
    private int _endDay = Integer.parseInt(_properties.getProperty("END_DAY"));
    private int _bufferSize = 2000;
    private boolean _showErrors = false;
    private long _oneDay = 1 * 24 * 60 * 60 * 1000;// ms in one day for timestamp calculations
    private java.sql.Timestamp _today = new java.sql.Timestamp(System.currentTimeMillis());
    private long _clockStart;
    private boolean _completeRebuild = false;

    public BatchBuilderDatabaseController() {}

    public void clearDatabase(int sta3n, boolean completeRebuild) {
        _completeRebuild = completeRebuild;
        clearDatabase(sta3n);
    }

    public void clearDatabase(int sta3n) {
        System.out.println("*Clearing Database...");
        _dest.update("DELETE FROM Commend.dbo.CommendAccess");
        _dest.update("DELETE FROM Commend.dbo.CommendMedicationManagement WHERE Sta3n = " + sta3n);

        _dest.update("DELETE FROM Commend.dbo.CommendEncounters");
        _dest.update("DELETE FROM Commend.dbo.CommendEncSummary");

        if(_completeRebuild) {
            _dest.update("DELETE FROM Commend.dbo.CommendProgressNote WHERE Sta3n = " + sta3n);
            _dest.update("DELETE FROM Commend.dbo.CommendNoteEncounters WHERE Sta3n = " + sta3n);
            _dest.update("DELETE FROM Commend.dbo.CommendOutcomes");
            _dest.update("DELETE FROM Commend.dbo.CommendSideEffects");
            _dest.update("DELETE FROM Commend.dbo.CommendTrtModes");
        } else {
            _dest.update("DELETE FROM Commend.dbo.CommendOutcomes WHERE IsCommendNote = 'false'");
        }
    }

    public ArrayList<CommendEncounterData> getCommendEncounters() {
        final java.util.ArrayList<CommendEncounterData> list = new java.util.ArrayList();
        String query =
                "SELECT SSN,TrtLocIEN,apptDateTime,CancelNoShowCode,CancellationReason \n"
                + "FROM Commend.dbo.CommendEncounters \n"
                + "ORDER BY SSN, apptDateTime desc";

        _dest.query(query, new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                String ssn = rs.getString("SSN");
                String trtLocIEN = rs.getString("TrtLocIEN");
                Timestamp apptDateTime = rs.getTimestamp("apptDateTime");
                String apptStatus = rs.getString("CancelNoShowCode");
                String CancellationReason = rs.getString("CancellationReason");
                CommendEncounterData anEnc = new CommendEncounterData(ssn, trtLocIEN, apptDateTime, apptStatus, CancellationReason);
                list.add(anEnc);
            }
        });
        return list;
    }

    public void saveEncSummaries(ArrayList<CommendEncSummaryData> encSummaries) {
        if (encSummaries == null || encSummaries.size() <= 0) {
            return;
        }
        int nSummaries = encSummaries.size();
        String insertRoot = "insert into CommendEncSummary (ssn, countTotAppts, countNoShows, nextAppt, lastAppt) ";
        String unionAll = " union all ";
        StringBuffer insStat = new StringBuffer(insertRoot);
        int countRows = 0;
        for (int j = 0; j < nSummaries; j++) {
            CommendEncSummaryData aSum = encSummaries.get(j);
            String selStr = createSelectString(aSum);
            if (selStr != null) {
                countRows += 1;
            } else {
                continue;
            }
            if (countRows < _bufferSize) {
                insStat.append(selStr).append(unionAll);
            } else if (countRows == _bufferSize) {
                insStat.append(selStr);   // for last select,no need to have "union all"
                _dest.update(insStat.toString());
                int lenStat = insStat.length();
                insStat.delete(0, lenStat);
                insStat.append(insertRoot);
                countRows = 0;
            }
        }
        // Here if we still have rows to save
        if (insStat != null && insStat.length() > insertRoot.length()) {
            // we must take out the last unionAll which has 11 characters
            int lenDesired = insStat.length() - unionAll.length();
            String lastInsert = insStat.substring(0, lenDesired);
            _dest.update(lastInsert);
        }
    }

    private String createSelectString(CommendEncSummaryData anEncSum) {
        if (anEncSum == null) {
            return null;
        }
        String sQuote = "'";
        String comma = ",";
        String nullStr = "NULL";

        StringBuffer resStr = new StringBuffer("select ");
        resStr.append(sQuote).append(anEncSum.m_SSN).append(sQuote).append(comma);
        resStr.append(anEncSum.m_numTotAppts).append(comma);
        resStr.append(anEncSum.m_numTotNoShows).append(comma);

        if (anEncSum.m_nextAppt == null) {
            resStr.append(nullStr).append(comma);
        } else {
            resStr.append(sQuote).append(anEncSum.m_nextAppt).append(sQuote).append(comma);
        }
        if (anEncSum.m_lastAppt == null) {
            resStr.append(nullStr);
        } else {
            resStr.append(sQuote).append(anEncSum.m_lastAppt).append(sQuote);
        }
        return resStr.toString();
    }

    public java.util.HashMap<String, String> getPatientsHash() {
        String q =
                "SELECT distinct SSN, PatientIEN \n"
                + "FROM Commend.dbo.CommendDemographics";
        final java.util.HashMap<String, String> hash = new java.util.HashMap();
        _dest.query(q, new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                hash.put(rs.getString("SSN"), rs.getString("PatientIEN"));
            }
        });
        return hash;
    }

    public java.util.List<java.util.Map<String, Object>> getPatients(final int sta3n) {
        java.util.List<java.util.Map<String, Object>> result = new java.util.ArrayList();
        java.util.List<java.util.Map<String, Object>> unprocessed = getUnprocessedPatients(sta3n);
        java.util.List<java.util.Map<String, Object>> missingNote = getMissingNotePatients(sta3n);
        result.addAll(unprocessed);
        result.addAll(missingNote);
        System.out.println("Number of Patiens MDWS process: " + result.size() + " unprocessed: " + unprocessed.size() + " missingNote: " + missingNote.size());
        return result;
    }

    public java.util.List<java.util.Map<String, Object>> getUnprocessedPatients(final int sta3n) {
        String q =
                "SELECT demo.PatientSID,demo.PatientIEN,demo.SSN \n"
                + "FROM Commend.dbo.CommendDemographics demo FULL OUTER JOIN Commend.dbo.CommendProgressNote cpn ON \n"
                + "     demo.PatientSID = cpn.PatientSID WHERE cpn.PatientSID IS NULL AND demo.Sta3n = " + sta3n;
        final java.util.List<java.util.Map<String, Object>> result = new java.util.ArrayList();
        _dest.query(q, new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                java.util.Map<String, Object> map = new java.util.HashMap();
                map.put("Sta3n", sta3n);
                map.put("PatientSID", rs.getInt("PatientSID"));
                map.put("PatientIEN", rs.getInt("PatientIEN"));
                map.put("SSN", rs.getString("SSN"));
                result.add(map);
            }
        });
        return result;
    }

    public java.util.List<java.util.Map<String, Object>> getMissingNotePatients(final int sta3n) {
        String q =
                "SELECT \n"
                + "     cln.Sta3n \n"
                + "     ,cln.PatientSID \n"
                + "     ,demo.PatientIEN \n"
                + "     ,demo.SSN \n"
                + "     ,CAST(cln.Date AS DATE) AS LastCPRSNoteDate"
                + "     ,CAST(lnp.NoteDate AS DATE) AS LastCommendNoteDate \n"
                + "FROM Commend.dbo.CommendLastNote cln, \n"
                + "     Commend.dbo.CommendLastNotePulled lnp, \n"
                + "     Commend.dbo.CommendDemographics demo \n"
                + "WHERE \n"
                + "     cln.Sta3n = " + sta3n + " \n"
                + " AND lnp.Sta3n = cln.Sta3n \n"
                + " AND demo.Sta3n = lnp.Sta3n \n"
                + " AND cln.PatientSID = lnp.PatientSID \n"
                + " AND lnp.PatientSID = demo.PatientSID \n"
                + " AND CAST(cln.Date AS DATE) > CAST(lnp.NoteDate AS DATE)";
        final java.util.List<java.util.Map<String, Object>> result = new java.util.ArrayList();
        _dest.query(q, new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                java.sql.Timestamp com = rs.getTimestamp("LastCommendNoteDate");
                java.sql.Timestamp cprs = rs.getTimestamp("LastCPRSNoteDate");
                java.sql.Timestamp pullFromDate = new java.sql.Timestamp(com.getTime() + (1000*60*60*24));
                java.util.Map<String, Object> map = new java.util.HashMap();
                map.put("Sta3n", sta3n);
                map.put("PatientSID", rs.getInt("PatientSID"));
                map.put("PatientIEN", rs.getInt("PatientIEN"));
                map.put("SSN", rs.getString("SSN"));
                map.put("Date", pullFromDate);
                result.add(map);
            }
        });
        return result;
    }

    public void insertCommendDemographics(int sta3n) {
        System.out.println("*Insert Commend Demographics...");
        startTimmer();
        String q = "DELETE FROM Commend.dbo.CommendDemographics WHERE Sta3n=" + sta3n;
        _dest.update(q);
        q =
                "INSERT INTO Commend.dbo.CommendDemographics (Sta3n,PatientSID,PatientIEN,SSN,Name,DOB,Race,Sex) \n"
                + "SELECT distinct \n"
                + "   pt.Sta3n, \n"
                + "   pt.PatientSID, \n"
                + "   pt.PatientIEN, \n"
                + "   pt.PatientSSN, \n"
                + "   pt.PatientName, \n"
                + "   pt.DateOfBirth, \n"
                + "   CASE WHEN pr.race is null THEN '#Unknown' ELSE pr.race END, \n"
                + "   pt.Gender \n"
                + "FROM \n"
                + "  Commend.dbo.CommendProviderClinics cl \n"
                + "  join VDWWork.Appt.Appointment app on \n"
                + "        app.LocationSID = cl.LocationSID \n"
                + "    and app.AppointmentDateTime " + getRange() + " \n"
                + "  join VDWWork.SPatient.SPatient pt \n"
                + "    on pt.patientSID = app.patientSID \n"
                + "    and pt.sta3n = " + sta3n + " \n"
                + "  left outer join VDWWork.PatSub.PatientRace pr on \n"
                + "        pr.patientSID = pt.patientSID \n"
                + "    and	pr.raceSID in (SELECT TOP 1 rp.RaceSID \n"
                + "    		       FROM VDWWork.PatSub.PatientRace rp \n"
                + "    		       where rp.PatientSID = pr.PatientSID \n"
                + "    		       ORDER BY PatientRaceSID DESC) \n"
                + "  where cl.disable = 'N' \n"
                + "  order by pt.patientName \n";
        _dest.update(q);
        logTimmer(1000011);
    }

    public void insertCommendDemographicsTESTING(int sta3n) {
        System.out.println("*Insert Commend Demographics Testing...");
        String q1 =
                "INSERT INTO Commend.dbo.CommendDemographics (Sta3n, PatientSID, patientIEN, SSN, Name, DOB, Race, Sex) \n"
                + "SELECT DISTINCT \n"
                + "   pt.Sta3n, \n"
                + "   pt.PatientSID, \n"
                + "   pt.PatientIEN, \n"
                + "   pt.PatientSSN, \n"
                + "   pt.PatientName, \n"
                + "   pt.DateOfBirth, \n"
                + "   pr.Race, \n"
                + "   pt.Gender \n"
                + "FROM \n"
                + "   VDWWork.SPatient.SPatient pt, \n"
                + "   VDWWork.PatSub.PatientRace pr, \n"
                + "   VDWWork.Appt.Appointment app, \n"
                + "   Commend.dbo.CommendProviderClinics cl \n"
                + "WHERE pt.sta3n= " + sta3n + " \n"
                + "   AND app.sta3n=pt.sta3n \n"
                + "   AND app.LocationSID=cl.LocationSID \n"
                + "   AND app.PatientSID=pt.PatientSID \n"
                + "   AND pr.PatientSID = app.PatientSID \n"
                + "   AND cl.disable = 'N' \n"
                + "   AND pr.RaceSID in ( \n"
                + "		SELECT TOP 1 rp.RaceSID \n"
                + "		FROM VDWWork.PatSub.PatientRace rp \n"
                + "		where rp.PatientSID = pr.PatientSID \n"
                + "		ORDER BY PatientRaceSID DESC) \n"
                + "   AND pt.PatientLastName like '%zztestpatient%' \n"
                + "ORDER BY pt.PatientName";
        _dest.update(q1);
    }

    public void insertCommendPrescriptionsRaw(int sta3n) {
        System.out.println("*Insert Commend Prescriptions Raw...");
        startTimmer();
//        "insert into Commend.dbo.CommendPrescriptionsRaw \n"
//                + "   (SSN, localDrugIEN, LocalDrugNameWithDose, DrugNameWithoutDose, NationalFormularyFlag, \n"
//                + "   NationalFormularyName, IssueDate, FillDateTime, ReleaseDateTime, ExpirationDate, \n"
//                + "   Qty, DaysSupply, Units, DosageForm, Strength, \n"
//                + "   PrimaryDrugClassIEN, PrimaryDrugClassCode) \n";
        String q = "DELETE FROM Commend.dbo.CommendPrescriptionsRaw WHERE Sta3n=" + sta3n;
        _dest.update(q);
        q = "INSERT INTO Commend.dbo.CommendPrescriptionsRaw (Sta3n,PatientSID,LocalDrugSID,LocalDrugNameWithDose,DrugNameWithoutDose,NationalFormularyFlag,NationalFormularyName,IssueDate,FillDateTime,ReleaseDateTime,ExpirationDate,Qty,DaysSupply,Units,DosageForm,Strength,PrimaryDrugClassSID,PrimaryDrugClassCode) "
                + "SELECT \n"
                + "   pt.Sta3n, \n"
                + "   pt.PatientSID, \n"
                + "   prscr.LocalDrugSID, \n"
                + "   prscr.LocalDrugNameWithDose, \n"
                + "   prscr.DrugNameWithoutDose, \n"
                + "   nat_dr.NationalFormularyFlag, \n"
                + "   nat_dr.NationalFormularyName, \n"
                + "   prscr.IssueDate, \n"
                + "   prscr.FillDateTime, \n"
                + "   prscr.ReleaseDateTime, \n"
                + "   prscr.ExpirationDate, \n"
                + "   prscr.Qty, \n"
                + "   prscr.DaysSupply, \n"
                + "   nat_dr.Units, \n"
                + "   nat_dr.DosageForm, \n"
                + "   nat_dr.Strength, \n"
                + "   nat_dr.PrimaryDrugClassSID, \n"
                + "   nat_dr.PrimaryDrugClassCode \n"
                + "FROM Commend.dbo.CommendDemographics pt, \n"
                + "     VDWWork.RxOut.RxOutpatFill prscr, \n"
                + "     VDWWork.Dim.LocalDrug loc_dr, \n"
                + "     VDWWork.Dim.NationalDrug nat_dr \n"
                + "WHERE prscr.sta3n = " + sta3n + " \n"
                + "  and loc_dr.sta3n = prscr.sta3n \n"
                + "  and nat_dr.sta3n = loc_dr.sta3n \n"
                + "  and pt.PatientSID = prscr.PatientSID \n"
                + "  and prscr.LocalDrugSID = loc_dr.LocalDrugSID \n"
                + "  and nat_dr.NationalDrugSID = prscr.NationalDrugSID \n"
                + "ORDER BY pt.patientSID, prscr.localDrugNameWithDose, prscr.FillDateTime";
        _dest.update(q);
        logTimmer(1000013);
    }

    public void insertCommendPrescriptions(int sta3n) {
        System.out.println("*Insert Commend Prescriptions...");
        _dest.update("DELETE FROM Commend.dbo.CommendPrescriptions WHERE Sta3n=" + sta3n);
        startTimmer();
        _dest.query("SELECT * FROM Commend.dbo.CommendPrescriptionsRaw WHERE Sta3n=" + sta3n, new SQLObject() {

            int counter = 0;
            StringBuilder buff = new StringBuilder();
            DBC dbc = _dest.clone();

            public void row(ResultSet rs) throws SQLException {
                buff.append(bufferInsertPrescription(rs, 3, 30 * 6));
                counter++;
                if (counter > _bufferSize) {
                    counter = 0;
                    dbc.update(buff.toString());
                    buff.setLength(0);
                }
            }

            public void post() {
                if (buff.length() > 0) {
                    dbc.update(buff.toString());
                }
            }
        });
        logTimmer(1000014);
    }

    public void insertCommendMedicationManagementVAonly(int sta3n) {
        System.out.println("*Insert Commend Medication Management (Raw VA only)...");
        startTimmer();
        String q = "INSERT INTO Commend.dbo.CommendMedicationManagement (Sta3n,PatientSID,LocalDrugNameWithDose,Sig,RxStatus,VistaCreateDate,isVAMed) \n"
                + "SELECT rx.Sta3n, rx.PatientSID, ld.LocalDrugNameWithDose, sig.Sig, rx.RxStatus, rx.IssueDate, 1 \n"
                + "FROM VDWWork.RxOut.RxOutpat rx, \n"
                + "	VDWWork.RxOut.RxOutpatSig sig, \n"
                + "	VDWWork.Dim.LocalDrug ld, \n"
                + "	Commend.dbo.CommendDemographics cd \n"
                + "WHERE rx.PatientSID = cd.PatientSID \n"
                + "  AND rx.Sta3n = " + sta3n + " \n"
                + "  AND cd.Sta3n = rx.Sta3n \n"
                + "  AND sig.Sta3n = cd.Sta3n \n"
                + "  AND ld.Sta3n = sig.Sta3n \n"
                + "  AND ld.LocalDrugSID = rx.LocalDrugSID \n"
                + "  AND rx.RxStatus in ('HOLD','ACTIVE','PENDING') \n"
                + "  AND sig.RxOutpatSID = rx.RxOutpatSID \n"
                + "ORDER BY rx.IssueDate DESC";
        _dest.update(q);
        logTimmer(1000016);
    }

    public void insertCommendMedicationManagementNonVA(int Sta3n, int PatientSID, String LocalDrugNameWithDose, String Sig, String RxStatus, java.sql.Timestamp VistaCreateDate, int isVAMed) {
        if ((isVAMed > 1) || (isVAMed < 0)) {
            isVAMed = 0;
        }
        String q = "INSERT INTO Commend.dbo.CommendMedicationManagement (Sta3n,PatientSID,LocalDrugNameWithDose,Sig,RxStatus,VistaCreateDate,isVAMed) \n"
                + "VALUES (" + Sta3n + "," + PatientSID + "," + DBC.fixString(LocalDrugNameWithDose) + ",'" + Sig + "','" + RxStatus + "','" + VistaCreateDate + "','" + isVAMed + "') \n";
        _dest.update(q);
    }

    public void insertCommendEncounters(int sta3n) {
        System.out.println("*Insert Commend Encounters...");
        startTimmer();
        String q =
                "INSERT INTO Commend.dbo.CommendEncounters (SSN, TrtLocIEN, apptDateTime, CancelNoShowCode, CancellationReason) \n"
                + "SELECT DISTINCT \n"
                + "       pt.SSN, \n"
                + "       cl.TrtLocIEN, \n"
                + "       appt.AppointmentDateTime, \n"
                + "       appt.CancelNoShowCode, \n"
                + "       cr.CancellationReason \n"
                + "FROM   Commend.dbo.CommendDemographics pt, \n"
                + "       Commend.dbo.CommendProviderClinics cl, \n"
                + "       VDWWork.Appt.Appointment appt, \n"
                + "       VDWWork.Dim.CancellationReason cr \n"
                + "WHERE  appt.sta3n = " + sta3n + " \n"
                + "       and appt.PatientSID = pt.PatientSID \n"
                + "       and cl.LocationSID = appt.LocationSID \n"
                + "       and appt.CancellationReasonSID = cr.CancellationReasonSID \n"
                + "       and appt.AppointmentDateTime BETWEEN DATEADD(YEAR,-1,GETDATE()) AND DATEADD(DAY,7,GETDATE()) \n"
                + "ORDER BY pt.SSN, appt.AppointmentDateTime DESC";
        _dest.update(q);
        logTimmer(1000010);
    }

    public void insertCommendEncountersTESTING(int sta3n) {
        System.out.println("*Insert Commend Encounters Testing...");
        String q =
                "insert into Commend.dbo.CommendEncounters (SSN, TrtLocIEN, apptDateTime) \n"
                + "select distinct pt.PatientSSN, \n"
                + "       cl.TrtLocIEN, \n"
                + "       apt.AppointmentDateTime \n"
                + "from   VDWWork.SPatient.SPatient pt, \n"
                + "       VDWWork.Appt.Appointment apt, \n"
                + "       Commend.dbo.CommendProviderClinics cl \n"
                + "where  pt.sta3n = " + sta3n + " \n"
                + "       and apt.sta3n=pt.sta3n \n"
                + "       and apt.LocationSID=cl.LocationSID \n"
                + "       and apt.PatientIEN=pt.PatientIEN \n"
                + "       and pt.PatientLastName like '%zztestpatient%' \n"
                + "       and apt.AppointmentDateTime BETWEEN DATEADD(YEAR,-2,GETDATE()) AND DATEADD(DAY,7,GETDATE()) \n"
                + "order by pt.PatientSSN";
        _dest.update(q);
    }

    public void insertCommendConditions() {
        System.out.println("*Insert Commend Conditions...");
        startTimmer();
        String q =
                "SELECT demo.SSN, pl.diagnosis, dis.ICDDescription, pl.DateEntered, pl.DateOfOnset \n"
                + "FROM \n"
                + "     Commend.dbo.CommendDemographics demo, \n"
                + "     VDWCubeStaging.Prob.ProblemList pl, \n"
                + "     VDWWork.Dim.ICD dis \n"
                + "WHERE \n"
                + "     demo.PatientIEN = pl.PatientIEN \n"
                + "     AND pl.sta3n = '640' \n"
                + "     AND pl.status = 'ACTIVE' \n"
                + "     AND pl.diagnosis = dis.ICDCode \n"
                + "ORDER BY pl.patientIEN, pl.DateEntered DESC";

        _dest.query(q, new SQLObject() {

            DBC dbc = _dest.clone();
            int counter = 0;
            String insertQuery = "";

            public void row(ResultSet rs) throws SQLException {
                String DateOfOnset = rs.getString("DateOfOnset");
                String Begin_date = null;
                if (DateOfOnset != null) {
                    Begin_date = DateOfOnset;
                } else {
                    Begin_date = rs.getString("DateEntered");
                }

                insertQuery += "insert into Commend.dbo.CommendConditions (SSN, ICD9_1, Condition, last_date, Begin_date) "
                        + "values ('" + rs.getString("SSN") + "','" + rs.getString("diagnosis") + "','" + fixApostropheForSQLInsertion(rs.getString("ICDDescription")) + "','"
                        + rs.getString("DateEntered") + "','" + Begin_date + "') \n";
                counter++;
                if (counter > 100) {
                    dbc.update(insertQuery);
                    insertQuery = "";
                    counter = 0;
                }
            }

            public void post() {
                if (insertQuery.length() > 1) {
                    dbc.update(insertQuery);
                }
            }
        });
        logTimmer(1000015);
    }

    public void insertCommendStudies() {
        System.out.println("*Insert Commend Studies...");
        startTimmer();
        String q = "INSERT INTO Commend.dbo.CommendStudies (SSN, LabChemTestName, Value, units, date) \n"
                + "SELECT demo.SSN, chem.LabChemTestName, chem.LabChemResultValue, map.target_unit, chem.LabChemSpecimenDateTime \n"
                + "FROM Commend.dbo.CommendDemographics demo \n"
                + "  INNER JOIN VDWWork.Chem.LabChem chem \n"
                + "     ON demo.PatientIEN = chem.PatientIEN \n"
                + "  INNER JOIN Commend.dbo.LabMapping map \n"
                + "     ON chem.LabChemTestName = map.source_name \n"
                + "WHERE chem.Sta3n = '640' \n"
                + "ORDER BY demo.PatientIEN, chem.LabChemSpecimenDateTime DESC";
        _dest.update(q);
        logTimmer(1000012);
    }

    public void insertCommendTreatmentMode(String SSN, java.sql.Timestamp date, String primaryMode, String secondaryMode, String duration, String serviceID, String contactID) {
        //System.out.println("*Inserting TreatmentMode...");
        String q = "insert into Commend.dbo.CommendTrtModes (SSN, date, primaryMode, secondaryMode, duration, serviceID, contactID) \n"
                + "values (" + DBC.fixString(SSN) + "," + DBC.fixTimestamp(date) + "," + DBC.fixString(primaryMode) + ","
                + DBC.fixString(secondaryMode) + "," + DBC.fixString(duration) + "," + DBC.fixString(serviceID) + "," + DBC.fixString(contactID) + ")";
        _dest.update(q);
    }

    public void insertCommendOutcome(String outcomeID, String SSN, java.sql.Timestamp date, String value, boolean isCommendNote) {
        String q = "insert into Commend.dbo.CommendOutcomes (outcomeID,SSN,date,value,IsCommendNote) \n"
                + "values (" + DBC.fixString(outcomeID) + "," + DBC.fixString(SSN) + "," + DBC.fixTimestamp(date) + "," + DBC.fixString(value) + ",'" + isCommendNote + "')";
        _dest.update(q);
    }

    public void insertCommendSideEffects(String SSN, java.sql.Timestamp date, String listOfSideEffects, String daysPerWeek) {
        //System.out.println("*Inserting Side Effects...");
        String q = "insert into Commend.dbo.CommendSideEffects (SSN, date, listSideEffs, daysPerWeek) \n"
                + "values (" + DBC.fixString(SSN) + "," + DBC.fixTimestamp(date) + "," + DBC.fixString(listOfSideEffects) + "," + DBC.fixString(daysPerWeek) + ")";
        _dest.update(q);
    }

    public void insertCommendProviders(java.util.Collection<String> providers) {
        System.out.println("*Insert Commend Providers...");
        java.util.HashMap<String, String> duzProvider = getProviderDUZ(providers);
        for (String duz : duzProvider.keySet()) {
            String q = "insert into Commend.dbo.CommendProviders (DUZ, name) \n"
                    + "values ( '" + duz + "','" + duzProvider.get(duz) + "')";
            _dest.update(q);
        }
    }

    public void insertCommendProviderClinics() {
        System.out.println("*Insert Commend Provider Clinics...");
        String q = "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('16360', '9459', 'PCT-KASCH(MPD)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('16360', '9460', 'PCT-KASCH-PM(MPD)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('175683', '8935', 'PCT-BRANDOM-GROUP(SJC)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('175683', '8934', 'PCT-BRANDOM(SJC)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('31583', '8350', 'PCT-DILANDRO(LD)');\n"
                + "\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('31583', '8628', 'PCT-DILANDRO-GROUP(LD)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('31583', '9247', 'PCT-CPT-DILANDRO-GROUP(LD)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('31583', '9601', 'PCT-DILANDRO-GROUP-PM(LD)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('73500', '9531', 'PCT-HUGO(MPD)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('73500', '8479', 'PCT-HUGO(SJC)');\n"
                + "\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('73500', '9530', 'PCT-HUGO-GROUP(MPD)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('73500', '8479', 'PCT-HUGO-GROUP(SJC)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('143518', '8408', 'PCT-SALZMAN(MONT)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('143518', '8452', 'PCT-SALZMAN-GROUP(MONT)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('143518', '9245', 'PCT-SALZMAN-PM(MONT)');\n"
                + "\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('14362', '8538', 'PCT-SMITH(LD)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('14362', '9286', 'PCT-SMITH(SJC)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('14362', '8925', 'PCT-SMITH(STC)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('14362', '8539', 'PCT-SMITH-GROUP(LD)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('14362', '8687', 'PCT-SMITH-GROUP(STC)');\n"
                + "\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('130730', '9447', 'PCT-WEATHERBY(SJC)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('130730', '9448', 'PCT-WEATHERBY-GROUP(SJC)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('15733', '7910', 'PCT-YOUNG(MONT)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('15733', '7945', 'PCT-YOUNG-GROUP(MONT)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('15733', '9228', 'PCT-YOUNG-PM(MONT) ');\n"
                + "\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('13926', '7019', 'MHC-KOPELL(MPD)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('36701', '7917', 'MHC-SASTRY(MPD)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('147230', '8993', 'PCT-TOWNE(MPD)');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('147230', '9231', 'PCT-TOWNE-PM(MPD)');\n"
                + "\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('70479', '5116', 'TEST CLINIC');\n"
                + "insert into CommendProviderClinics \n"
                + "    values ('70479', '5116', 'TEST CLINIC');\n"
                + "insert into CommendProviderClinics \n"
                + "    values ('70479', '7019', 'MHC-KOPELL(MPD)');\n"
                + "insert into CommendProviderClinics \n"
                + "    values ('70479', '7917', 'MHC-SASTRY(MPD)');\n"
                + "insert into CommendProviderClinics \n"
                + "    values ('70479', '7910', 'PCT-YOUNG(MONT)');\n"
                + "insert into CommendProviderClinics \n"
                + "    values ('70479', '7945', 'PCT-YOUNG-GROUP(MONT)');\n"
                + "insert into CommendProviderClinics \n"
                + "    values ('70479', '9228', 'PCT-YOUNG-PM(MONT) ');\n"
                + "insert into CommendProviderClinics \n"
                + "    values ('182462', '5116', 'TEST CLINIC');\n"
                + "insert into CommendProviderClinics \n"
                + "    values ('182462', '7019', 'MHC-KOPELL(MPD)');\n"
                + "insert into CommendProviderClinics \n"
                + "    values ('182462', '7917', 'MHC-SASTRY(MPD)');\n"
                + "insert into CommendProviderClinics \n"
                + "    values ('182462', '7910', 'PCT-YOUNG(MONT)');\n"
                + "insert into CommendProviderClinics \n"
                + "    values ('182462', '7945', 'PCT-YOUNG-GROUP(MONT)');\n"
                + "insert into CommendProviderClinics \n"
                + "    values ('182462', '9228', 'PCT-YOUNG-PM(MONT) ');\n"
                + "insert into Commend.dbo.CommendProviderClinics \n"
                + "    values ('182462', '5116', 'TEST CLINIC');";
        _dest.update(q);
    }

    public void insertCommendPatientSummary() {
        System.out.println("*Insert Commend Patient Summary...");
        String q =
                "DELETE FROM [Commend].[dbo].[CommendPatientSummary] \n"
                + "IF OBJECT_ID('[Commend].[dbo].[CommendPatientSummarySideEffectLast]') is not null \n"
                + "DROP TABLE [Commend].[dbo].[CommendPatientSummarySideEffectLast] \n"
                + "IF OBJECT_ID('[Commend].[dbo].[CommendPatientSummarySideEffectFirst]') is not null \n"
                + "DROP TABLE [Commend].[dbo].[CommendPatientSummarySideEffectFirst] \n"
                + "\n"
                + "SELECT [SSN], MAX(date) AS lastDate \n"
                + "INTO [Commend].[dbo].[CommendPatientSummarySideEffectLast] \n"
                + "FROM [Commend].[dbo].[CommendSideEffects] \n"
                + "GROUP BY [SSN] \n"
                + "\n"
                + "ALTER TABLE [Commend].[dbo].[CommendPatientSummarySideEffectLast] \n"
                + "ADD lastValue decimal(9,2) \n"
                + "\n"
                + "ALTER TABLE [Commend].[dbo].[CommendPatientSummarySideEffectLast] \n"
                + "ADD details varchar(50) \n"
                + "\n"
                + "UPDATE [Commend].[dbo].[CommendPatientSummarySideEffectLast] \n"
                + "SET [Commend].[dbo].[CommendPatientSummarySideEffectLast].lastValue = [Commend].[dbo].[CommendSideEffects].daysPerWeek, \n"
                + "    [Commend].[dbo].[CommendPatientSummarySideEffectLast].details = [Commend].[dbo].[CommendSideEffects].listSideEffs \n"
                + "FROM [Commend].[dbo].[CommendPatientSummarySideEffectLast] INNER JOIN [Commend].[dbo].[CommendSideEffects] ON [Commend].[dbo].[CommendPatientSummarySideEffectLast].SSN = [Commend].[dbo].[CommendSideEffects].SSN \n"
                + "WHERE [Commend].[dbo].[CommendSideEffects].[date] = [Commend].[dbo].[CommendPatientSummarySideEffectLast].lastDate \n"
                + "SELECT [SSN], MIN(date) AS firstDate \n"
                + "INTO [Commend].[dbo].[CommendPatientSummarySideEffectFirst] \n"
                + "FROM [Commend].[dbo].[CommendSideEffects] \n"
                + "GROUP BY [SSN] \n"
                + "\n"
                + "ALTER TABLE [Commend].[dbo].[CommendPatientSummarySideEffectFirst] \n"
                + "ADD firstValue decimal(9,2) \n"
                + "\n"
                + "UPDATE [Commend].[dbo].[CommendPatientSummarySideEffectFirst] \n"
                + "SET [Commend].[dbo].[CommendPatientSummarySideEffectFirst].firstValue = [Commend].[dbo].[CommendSideEffects].daysPerWeek \n"
                + "FROM [Commend].[dbo].[CommendPatientSummarySideEffectFirst] INNER JOIN [Commend].[dbo].[CommendSideEffects] ON [Commend].[dbo].[CommendPatientSummarySideEffectFirst].SSN = [Commend].[dbo].[CommendSideEffects].SSN \n"
                + "WHERE [Commend].[dbo].[CommendSideEffects].[date] = [Commend].[dbo].[CommendPatientSummarySideEffectFirst].firstDate \n"
                + "\n"
                + "INSERT INTO [Commend].[dbo].[CommendPatientSummary] (SSN,type,details,firstDate,firstValue,lastDate,lastValue) \n"
                + "SELECT seFirst.SSN, 'S', seLast.details, seFirst.firstDate, seFirst.firstValue, seLast.lastDate, seLast.lastValue \n"
                + "FROM  \n"
                + "     [Commend].[dbo].[CommendPatientSummarySideEffectFirst] seFirst, \n"
                + "     [Commend].[dbo].[CommendPatientSummarySideEffectLast] seLast \n"
                + "WHERE \n"
                + "     seLast.SSN = seFirst.SSN \n"
                + "ORDER BY seLast.SSN \n"
                + "\n"
                + "IF OBJECT_ID('[Commend].[dbo].[CommendPatientSummaryOutcomeLast]') is not null \n"
                + "DROP TABLE [Commend].[dbo].[CommendPatientSummaryOutcomeLast] \n"
                + "IF OBJECT_ID('[Commend].[dbo].[CommendPatientSummaryOutcomeFirst]') is not null \n"
                + "DROP TABLE [Commend].[dbo].[CommendPatientSummaryOutcomeFirst] \n"
                + "\n"
                + "SELECT [SSN], outcomeID AS details, MAX(date) AS lastDate \n"
                + "INTO [Commend].[dbo].[CommendPatientSummaryOutcomeLast] \n"
                + "FROM [Commend].[dbo].[CommendOutcomes] \n"
                + "GROUP BY [SSN], outcomeID \n"
                + "\n"
                + "ALTER TABLE [Commend].[dbo].[CommendPatientSummaryOutcomeLast] \n"
                + "ADD lastValue decimal(9,2) \n"
                + "\n"
                + "UPDATE [Commend].[dbo].[CommendPatientSummaryOutcomeLast] \n"
                + "SET [Commend].[dbo].[CommendPatientSummaryOutcomeLast].lastValue = [Commend].[dbo].[CommendOutcomes].value \n"
                + "FROM [Commend].[dbo].[CommendPatientSummaryOutcomeLast] INNER JOIN [Commend].[dbo].[CommendOutcomes] ON [Commend].[dbo].[CommendPatientSummaryOutcomeLast].SSN = [Commend].[dbo].[CommendOutcomes].SSN \n"
                + "WHERE [Commend].[dbo].[CommendPatientSummaryOutcomeLast].details = [Commend].[dbo].[CommendOutcomes].outcomeID AND [Commend].[dbo].[CommendOutcomes].[date] = [Commend].[dbo].[CommendPatientSummaryOutcomeLast].lastDate \n"
                + "\n"
                + "SELECT [SSN], outcomeID AS details, MIN(date) AS firstDate \n"
                + "INTO [Commend].[dbo].[CommendPatientSummaryOutcomeFirst] \n"
                + "FROM [Commend].[dbo].[CommendOutcomes] \n"
                + "GROUP BY [SSN], outcomeID \n"
                + "\n"
                + "ALTER TABLE [Commend].[dbo].[CommendPatientSummaryOutcomeFirst] \n"
                + "ADD firstValue decimal(9,2) \n"
                + "\n"
                + "UPDATE [Commend].[dbo].[CommendPatientSummaryOutcomeFirst] \n"
                + "SET [Commend].[dbo].[CommendPatientSummaryOutcomeFirst].firstValue = [Commend].[dbo].[CommendOutcomes].value \n"
                + "FROM [Commend].[dbo].[CommendPatientSummaryOutcomeFirst] INNER JOIN [Commend].[dbo].[CommendOutcomes] ON [Commend].[dbo].[CommendPatientSummaryOutcomeFirst].SSN = [Commend].[dbo].[CommendOutcomes].SSN \n"
                + "WHERE [Commend].[dbo].[CommendPatientSummaryOutcomeFirst].details = [Commend].[dbo].[CommendOutcomes].outcomeID AND [Commend].[dbo].[CommendOutcomes].[date] = [Commend].[dbo].[CommendPatientSummaryOutcomeFirst].firstDate \n"
                + "\n"
                + "INSERT INTO [Commend].[dbo].[CommendPatientSummary] (SSN,type,details,firstDate,firstValue,lastDate,lastValue) \n"
                + "SELECT outFirst.SSN, 'O', defn.name, outFirst.firstDate, outFirst.firstValue, outLast.lastDate, outLast.lastValue \n"
                + "FROM  \n"
                + "     [Commend].[dbo].[CommendPatientSummaryOutcomeFirst] outFirst, \n"
                + "     [Commend].[dbo].[CommendPatientSummaryOutcomeLast] outLast, \n"
                + "     [Commend].[dbo].[CommendOutcomeDefn] defn \n"
                + "WHERE \n"
                + "     outLast.SSN = outFirst.SSN \n"
                + " and outLast.details = outFirst.details \n"
                + " and defn.outcomeID = outLast.details \n"
                + "ORDER BY outLast.SSN \n"
                + "\n"
                + "IF OBJECT_ID('[Commend].[dbo].[CommendPatientSummaryTherapyLast]') is not null \n"
                + "DROP TABLE [Commend].[dbo].[CommendPatientSummaryTherapyLast] \n"
                + "IF OBJECT_ID('[Commend].[dbo].[CommendPatientSummaryTherapyFirst]') is not null \n"
                + "DROP TABLE [Commend].[dbo].[CommendPatientSummaryTherapyFirst] \n"
                + "\n"
                + "SELECT [SSN], MAX(date) AS lastDate \n"
                + "INTO [Commend].[dbo].[CommendPatientSummaryTherapyLast] \n"
                + "FROM [Commend].[dbo].[CommendTrtModes] \n"
                + "GROUP BY [SSN] \n"
                + "\n"
                + "ALTER TABLE [Commend].[dbo].[CommendPatientSummaryTherapyLast] \n"
                + "ADD lastValue varchar(50) \n"
                + "UPDATE [Commend].[dbo].[CommendPatientSummaryTherapyLast] \n"
                + "SET [Commend].[dbo].[CommendPatientSummaryTherapyLast].lastValue = [Commend].[dbo].[CommendTrtModesDefn].name \n"
                + "FROM [Commend].[dbo].[CommendTrtModesDefn], \n"
                + "     [Commend].[dbo].[CommendPatientSummaryTherapyLast] INNER JOIN [Commend].[dbo].[CommendTrtModes] ON [Commend].[dbo].[CommendPatientSummaryTherapyLast].SSN = [Commend].[dbo].[CommendTrtModes].SSN \n"
                + "WHERE [Commend].[dbo].[CommendTrtModes].[date] = [Commend].[dbo].[CommendPatientSummaryTherapyLast].lastDate \n"
                + "  AND [Commend].[dbo].[CommendTrtModesDefn].modeID = [Commend].[dbo].[CommendTrtModes].primaryMode \n"
                + "\n"
                + "SELECT [SSN], MIN(date) AS firstDate \n"
                + "INTO [Commend].[dbo].[CommendPatientSummaryTherapyFirst] \n"
                + "FROM [Commend].[dbo].[CommendTrtModes] \n"
                + "GROUP BY [SSN] \n"
                + "\n"
                + "ALTER TABLE [Commend].[dbo].[CommendPatientSummaryTherapyFirst] \n"
                + "ADD firstValue varchar(50) \n"
                + "\n"
                + "UPDATE [Commend].[dbo].[CommendPatientSummaryTherapyFirst] \n"
                + "SET [Commend].[dbo].[CommendPatientSummaryTherapyFirst].firstValue = [Commend].[dbo].[CommendTrtModesDefn].name \n"
                + "FROM [Commend].[dbo].[CommendTrtModesDefn], \n"
                + "     [Commend].[dbo].[CommendPatientSummaryTherapyFirst] INNER JOIN [Commend].[dbo].[CommendTrtModes] ON [Commend].[dbo].[CommendPatientSummaryTherapyFirst].SSN = [Commend].[dbo].[CommendTrtModes].SSN \n"
                + "WHERE [Commend].[dbo].[CommendTrtModes].[date] = [Commend].[dbo].[CommendPatientSummaryTherapyFirst].firstDate \n"
                + "  AND [Commend].[dbo].[CommendTrtModesDefn].modeID = [Commend].[dbo].[CommendTrtModes].primaryMode \n"
                + "\n"
                + "INSERT INTO [Commend].[dbo].[CommendPatientSummary] (SSN,type,details,firstDate,firstValue,lastDate,lastValue) \n"
                + "SELECT therapyFirst.SSN, 'T', 'Therapy Modes', therapyFirst.firstDate, therapyFirst.firstValue, therapyLast.lastDate, therapyLast.lastValue \n"
                + "FROM  \n"
                + "     [Commend].[dbo].[CommendPatientSummaryTherapyFirst] therapyFirst, \n"
                + "     [Commend].[dbo].[CommendPatientSummaryTherapyLast] therapyLast \n"
                + "WHERE therapyLast.SSN = therapyFirst.SSN \n"
                + "ORDER BY therapyLast.SSN \n"
                + "\n"
                + "IF OBJECT_ID('[Commend].[dbo].[CommendPatientSummarySideEffectLast]') is not null \n"
                + "drop table [Commend].[dbo].[CommendPatientSummarySideEffectLast] \n"
                + "IF OBJECT_ID('[Commend].[dbo].[CommendPatientSummarySideEffectFirst]') is not null \n"
                + "drop table [Commend].[dbo].[CommendPatientSummarySideEffectFirst] \n"
                + "IF OBJECT_ID('[Commend].[dbo].[CommendPatientSummaryOutcomeLast]') is not null \n"
                + "drop table [Commend].[dbo].[CommendPatientSummaryOutcomeLast] \n"
                + "IF OBJECT_ID('[Commend].[dbo].[CommendPatientSummaryOutcomeFirst]') is not null \n"
                + "drop table [Commend].[dbo].[CommendPatientSummaryOutcomeFirst] \n"
                + "IF OBJECT_ID('[Commend].[dbo].[CommendPatientSummaryTherapyFirst]') is not null \n"
                + "drop table [Commend].[dbo].[CommendPatientSummaryTherapyFirst] \n"
                + "IF OBJECT_ID('[Commend].[dbo].[CommendPatientSummaryTherapyLast]') is not null \n"
                + "drop table [Commend].[dbo].[CommendPatientSummaryTherapyLast]";
        _dest.update(q);
    }

    public void insertNotes(int Sta3n, int PatientSID, int ProviderSID, java.sql.Timestamp encounterDate, java.sql.Timestamp noteDate, String noteText, String titleIEN, String title, boolean isCommendNote) {
        String q = "insert into Commend.dbo.CommendProgressNote (Sta3n,PatientSID,ProviderSID,EncounterDate,NoteDate,NoteText,TitleIEN,Title,IsCommendNote) \n"
                + "values (" + Sta3n + "," + PatientSID + "," + ProviderSID + "," + DBC.fixTimestamp(encounterDate) + "," + DBC.fixTimestamp(noteDate) + "," + DBC.fixString(noteText) + "," + DBC.fixString(titleIEN) + "," + DBC.fixString(title) + ",'" + isCommendNote + "')";
        _dest.update(q);
    }

    public void insertAssessmentComponentAnswers(String q) {
        if (q == null || q.length() <= 0) {
            return;
        }
        _dest.update(q);
    }

    public void insertCommendNoteEncounters(java.util.Map<String, Object> patientInfoMap, java.util.List<java.util.Map<String, Object>> array) {
        if (patientInfoMap == null || array == null) {
            return;
        }
        StringBuilder query = new StringBuilder();
        int count = 0;
        int sta3n = (Integer)patientInfoMap.get("Sta3n");
        int patientSID = (Integer)patientInfoMap.get("PatientSID");
        int patientIEN = (Integer)patientInfoMap.get("PatientIEN");
        String q =
                "DELETE FROM Commend.dbo.CommendNoteEncounters \n"
                + "WHERE Sta3n = " + sta3n + " AND PatientSID = " + patientSID;
        _dest.update(q);

        for (java.util.Map<String, Object> map : array) {
            count++;
            String encounter = (String) map.get("encounter");
            String display = (String) map.get("display");
            java.sql.Timestamp date = (java.sql.Timestamp) map.get("date");
            q = "INSERT INTO Commend.dbo.CommendNoteEncounters (Sta3n,PatientSID,PatientIEN,EncounterValue,EncounterDisplay,EncounterDate) \n"
                    + "VALUES (" + sta3n + ","
                    + patientSID + ","
                    + patientIEN + ","
                    + DBC.fixString(encounter) + ","
                    + DBC.fixString(display) + ","
                    + DBC.fixTimestamp(date) + ")";
            query.append(q);
            if (count > 2000) {
                count = 0;
                _dest.update(query.toString());
                query.setLength(0);
            }
        }
        if (query.length() > 0) {
            _dest.update(query.toString());
        }
    }

    public void insertCommendLastNote(int sta3n) {
        System.out.println("*Insert Commend Last Note...");
        String q = "DELETE FROM Commend.dbo.CommendLastNote \n"
                + "WHERE Sta3n = " + sta3n;
        _dest.update(q);
        q = "INSERT INTO Commend.dbo.CommendLastNote \n"
                + "SELECT tiu.Sta3n, \n"
                + "       tiu.PatientSID, \n"
                + "       MAX(tiu.EntryDateTime) AS Date \n"
                + "FROM   VDWWork.TIU.TIUDocument tiu, \n"
                + "       Commend.dbo.CommendDemographics demo \n"
                + "WHERE  demo.Sta3n = " + sta3n + " \n"
                + "   AND tiu.Sta3n = demo.Sta3n \n"
                + "   AND tiu.PatientSID = demo.PatientSID \n"
                + "   AND tiu.EntryDateTime > DATEADD(YEAR,-3,GETDATE()) \n"
                + "GROUP BY tiu.Sta3n,tiu.PatientSID";
        _dest.update(q);
    }

    public String bufferInsertPrescription(ResultSet rs, long activeDrugInterval, long recentDrugInterval) {
        String result = "";
        String sta3nFORMATTED, patientSIDFORMATTED, localDrugSIDFORMATTED, localDrugNameWithDoseFORMATTED, drugNameWithoutDoseFORMATTED, nationalFormularyFlagFORMATTED,
                nationalFormularyNameFORMATTED, issueDateFORMATTED, fillDateTimeFORMATTED, releaseDateTimeFORMATTED, expirationDateFORMATTED, qtyFORMATTED,
                daysSupplyFORMATTED, unitsFORMATTED, dosageFormFORMATTED, strengthFORMATTED, primaryDrugClassSIDFORMATTED, primaryDrugClassCodeFORMATTED, endDateTimeFORMATTED;
        java.sql.Timestamp endDateTime = _today;
        String daysSupplyStr;
        int daysSupplyInt;
        long adi = _today.getTime() - activeDrugInterval * _oneDay;
        long rdi = _today.getTime() - recentDrugInterval * _oneDay;
        float qty = 0;
        float strength = 0;
        float dailyDose = 0;
        String isActive = "'F'";
        String isRecent = "'F'";

        try {
            daysSupplyInt = rs.getInt("DaysSupply");
            daysSupplyStr = Integer.toString(daysSupplyInt);
            String qtyStr = rs.getString("Qty");
            String strengthStr = rs.getString("Strength"); //problem strenth sometimes two numbers. i.e. 350-425
            java.sql.Timestamp issueDateTS = rs.getTimestamp("IssueDate");
            java.sql.Timestamp fillDateTS = rs.getTimestamp("FillDateTime");

            if (qtyStr != null) {
                qty = tryParseFloat(qtyStr);
            }
            if (strengthStr != null) {
                strength = preproessStrengthString(strengthStr);
            }
            if (daysSupplyInt != 0) {
                dailyDose = (qty / daysSupplyInt) * strength;
            } else {
                dailyDose = 0;
            }

            if (fillDateTS != null) {
                endDateTime = new java.sql.Timestamp(fillDateTS.getTime() + (_oneDay * daysSupplyInt));
            } else if (issueDateTS != null) {
                endDateTime = new java.sql.Timestamp(issueDateTS.getTime() + (_oneDay * daysSupplyInt));
                fillDateTS = issueDateTS;
            }

            long edt = endDateTime.getTime();
            if (edt > adi) {
                isActive = "'T'";
            }
            if (edt > rdi) {
                isRecent = "'T'";
            }

            //Prepare the remanding precomputed data that is required
            sta3nFORMATTED = DBC.fixString(rs.getString("Sta3n"));
            patientSIDFORMATTED = DBC.fixString(rs.getString("PatientSID"));
            localDrugSIDFORMATTED = DBC.fixString(rs.getString("LocalDrugSID"));
            localDrugNameWithDoseFORMATTED = DBC.fixString(rs.getString("LocalDrugNameWithDose"));

            drugNameWithoutDoseFORMATTED = DBC.fixString(rs.getString("DrugNameWithoutDose"));
            nationalFormularyFlagFORMATTED = DBC.fixString(rs.getString("NationalFormularyFlag"));
            nationalFormularyNameFORMATTED = DBC.fixString(rs.getString("NationalFormularyName"));

            issueDateFORMATTED = DBC.fixTimestamp(issueDateTS);
            fillDateTimeFORMATTED = DBC.fixTimestamp(fillDateTS);
            releaseDateTimeFORMATTED = DBC.fixTimestamp(rs.getTimestamp("ReleaseDateTime"));

            expirationDateFORMATTED = DBC.fixTimestamp(rs.getTimestamp("ExpirationDate"));
            qtyFORMATTED = DBC.fixString(qtyStr);
            daysSupplyFORMATTED = DBC.fixString(daysSupplyStr);

            unitsFORMATTED = DBC.fixString(rs.getString("Units"));
            dosageFormFORMATTED = DBC.fixString(rs.getString("DosageForm"));
            strengthFORMATTED = DBC.fixString(strengthStr);

            primaryDrugClassSIDFORMATTED = DBC.fixString(rs.getString("PrimaryDrugClassSID"));
            primaryDrugClassCodeFORMATTED = DBC.fixString(rs.getString("PrimaryDrugClassCode"));

            endDateTimeFORMATTED = DBC.fixTimestamp(endDateTime);
//Sta3n,PatientSID,LocalDrugSID,LocalDrugNameWithDose,
//PrimaryDrugClassCode,FillDateTime,Qty,DaysSupply,Strength,
//Units,isActive,isRecent,DailyDose,DailyDoseUnits,
//EndDateTime,DrugNameWithoutDose,NationalFormularyFlag,
//NationalFormularyName,IssueDate,ReleaseDateTime,ExpirationDate,DosageForm,PrimaryDrugClassSID
            result =
                    "INSERT INTO Commend.dbo.CommendPrescriptions \n"
                    + "   (Sta3n,PatientSID,LocalDrugSID,LocalDrugNameWithDose,PrimaryDrugClassCode,FillDateTime,Qty,DaysSupply,Strength,Units,isActive,isRecent,DailyDose,DailyDoseUnits"
                    + ",EndDateTime,DrugNameWithoutDose,NationalFormularyFlag,NationalFormularyName,IssueDate,ReleaseDateTime,ExpirationDate,DosageForm,PrimaryDrugClassSID) \n";
            result +=
                    "VALUES (" + sta3nFORMATTED + "," + patientSIDFORMATTED + "," + localDrugSIDFORMATTED + "," + localDrugNameWithDoseFORMATTED + ","
                    + primaryDrugClassCodeFORMATTED + "," + fillDateTimeFORMATTED + "," + qtyFORMATTED + "," + daysSupplyFORMATTED + "," + strengthFORMATTED + ","
                    + unitsFORMATTED + "," + isActive + "," + isRecent + "," + dailyDose + "," + unitsFORMATTED + ","
                    + endDateTimeFORMATTED + "," + drugNameWithoutDoseFORMATTED + "," + nationalFormularyFlagFORMATTED + ","
                    + nationalFormularyNameFORMATTED + "," + issueDateFORMATTED + "," + releaseDateTimeFORMATTED + "," + expirationDateFORMATTED + ","
                    + dosageFormFORMATTED + "," + primaryDrugClassSIDFORMATTED + ") \n";
        } catch (Exception e) {
            LogUtility.error(e);
            result = "";
        }
        return result;
    }

    private java.util.HashMap getProviderDUZ(java.util.Collection<String> providers) {
        final java.util.HashMap<String, String> duzProvider = new java.util.HashMap();
        String providersList = "(";
        for (String str : providers) {
            providersList += "'" + str + "',";
        }
        providersList = providersList.substring(0, providersList.lastIndexOf(',')) + ")";
        String q = "SELECT StaffIEN, StaffName \n"
                + "FROM VDWWork.SStaff.SStaff \n"
                + "WHERE StaffName in " + providersList + " \n"
                + "  and sta3n = '640' \n"
                + "ORDER BY staffName";
        _dest.query(q, new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                duzProvider.put(rs.getString("staffIEN"), rs.getString("staffName"));
            }
        });
        return duzProvider;
    }

    private String pullLocationName(String str) {
        int index = str.indexOf('(');
        String name = "";
        if (index >= 0) {
            name = str.substring(index + 1, str.length());
            index = name.indexOf(')');
            if (index >= 0) {
                name = name.substring(0, index);
            }
        }
        return name;
    }

    public java.util.ArrayList<String> getProviderDUZ() {
        String q = "SELECT distinct DUZ \n"
                + "FROM Commend.dbo.CommendProviderClinics \n"
                + "WHERE disable = 'N'";
        final java.util.ArrayList<String> result = new java.util.ArrayList();
        _dest.query(q, new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                result.add(rs.getString("DUZ"));
            }
        });
        return result;
    }

    public java.util.LinkedHashMap<String, String> getOutcomeHashToLower() {
        java.util.LinkedHashMap<String, String> hash = getOutcomeHash();
        java.util.LinkedHashMap<String, String> result = new java.util.LinkedHashMap();
        for (String key : hash.keySet()) {
            String lowerKey = key.toLowerCase();
            result.put(lowerKey, hash.get(key));
        }
        return result;
    }

    public java.util.LinkedHashMap<String, String> getOutcomeHash() {
        final java.util.LinkedHashMap<String, String> outcomeHash = new java.util.LinkedHashMap();
        _dest.query("SELECT outcomeID, name FROM Commend.dbo.CommendOutcomeDefn", new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                outcomeHash.put(rs.getString("name"), rs.getString("outcomeID"));
            }
        });
        return outcomeHash;
    }

    public java.util.LinkedHashMap<String, String> getSideEffectsHash() {
        final java.util.LinkedHashMap<String, String> sideEffectsHash = new java.util.LinkedHashMap();
        _dest.query("SELECT SideEffID, name FROM Commend.dbo.CommendSideEffectsDefn", new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                sideEffectsHash.put(rs.getString("name"), rs.getString("SideEffID"));
            }
        });
        return sideEffectsHash;
    }

    public java.util.LinkedHashMap<String, String> getContactTypesHash() {
        final java.util.LinkedHashMap<String, String> sideEffectsHash = new java.util.LinkedHashMap();
        _dest.query("SELECT * FROM [Commend].[dbo].[CommendTrtModeContactDefn]", new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                sideEffectsHash.put(rs.getString("name"), rs.getString("contactID"));
            }
        });
        return sideEffectsHash;
    }

    public java.util.LinkedHashMap<String, String> getServiceModesHash() {
        final java.util.LinkedHashMap<String, String> serviceHash = new java.util.LinkedHashMap();
        _dest.query("SELECT * FROM [Commend].[dbo].[CommendTrtModeServiceDefn]", new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                serviceHash.put(rs.getString("name"), rs.getString("serviceID"));
            }
        });
        return serviceHash;
    }

    public java.util.LinkedHashMap<String, String> getTreatmentModeHash() {
        final java.util.LinkedHashMap<String, String> modesHash = new java.util.LinkedHashMap();
        _dest.query("SELECT * FROM [Commend].[dbo].[CommendTrtModesDefn]", new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                modesHash.put(rs.getString("name"), rs.getString("modeID"));
            }
        });
        return modesHash;
    }

    public java.util.Map<Integer,Integer> getPatientIENtoSIDMap() {
        final java.util.Map<Integer,Integer> result = new java.util.LinkedHashMap();
        _dest.query("SELECT PatientSID,PatientIEN FROM Commend.dbo.CommendDemographics", new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                result.put(rs.getInt("PatientIEN"), rs.getInt("PatientSID"));
            }
        });
        return result;
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
                + "VALUES(" + DBC.fixTimestamp(eventtime) + "," + DBC.fixTimestamp(logtime) + ","
                + DBC.fixString(patientSSN) + "," + DBC.fixString(providerDUZ) + ","
                + DBC.fixString(clientComputer) + "," + DBC.fixString(event) + "," + eventId + ");";
        _dest.update(q);
    }

    public String getProviderDUZByName(String name) {
        String q = "SELECT TOP 1 * \n"
                + "FROM Commend.dbo.CommendProviders \n"
                + "WHERE name = " + DBC.fixString(name);
        final java.util.List<String> duzList = new java.util.ArrayList();
        _dest.query("SELECT * FROM Commend.dbo.CommendTrtModesDefn", new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                duzList.add(rs.getString("DUZ"));
            }
        });
        return duzList.get(0);
    }

    public int getProviderSIDByName(String name) {
        String q = "SELECT TOP 1 SID \n"
                + "FROM Commend.dbo.CommendProviders \n"
                + "WHERE name = " + DBC.fixString(name);
        final java.util.List<Integer> SIDList = new java.util.ArrayList();
        _dest.query(q, new SQLObject() {

            public void row(ResultSet rs) throws SQLException {
                SIDList.add(rs.getInt("SID"));
            }
        });
        if(!SIDList.isEmpty()) {
            return SIDList.get(0);
        }
        return -1;
    }

    private String getRange() {
        return "BETWEEN DATEADD(day," + _startDay + ",GETDATE()) AND DATEADD(day," + _endDay + ",GETDATE())";
    }

    private long tryParseLong(String s) {
        if (s != null) {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException e) {
                LogUtility.error(e);
                System.out.println("ERROR (parseLong:BatchBuilderDatabaseControler): " + e);
            }
        }
        return -1;
    }

    private Float tryParseFloat(String s) {
        if (s == null) {
            return (float) -1;
        }
        String pulledFloat = pullFirstFloat(s);
        if (pulledFloat != null) {
            try {
                return Float.parseFloat(pulledFloat);
            } catch (NumberFormatException e) {
                LogUtility.error(e);
                System.out.println("ERROR (tryParseFloat:BatchBuilderDatabaseControler): " + e);
            }
        }
        return (float) -1;
    }

    private Float preproessStrengthString(String s) {
        int index;
        String sub = s;
        if (s.contains("*")) {
            if (_showErrors) {
                System.out.println("Bad string: " + s + " returning 0.0f");
            }
            return 0.0f;
        }
        if (s.contains("-")) {
            if (_showErrors) {
                System.out.println("Bad string: " + s + " attempting to fix issue.");
            }
            index = s.indexOf('-');
            sub = s.substring(0, index);
        }
        return tryParseFloat(sub);
    }

    public String fixApostropheForSQLInsertion(String str) {
        if (str == null) {
            return null;
        }
        int index = str.indexOf('\'');
        String temp = str;
        if (index > 0) {
            String before = str.substring(0, index + 1) + "'";
            String after = str.substring(index + 1);
            temp = before + fixApostropheForSQLInsertion(after);
        }
        return temp;
    }

    private String pullFirstFloat(String str) {
        if (str == null) {
            System.err.println("Error (pullFirstFloat:BatchBuilderDBC) NULL input.");
            return null;
        }
        String beforeDecimal = parseFirstNumber(str);
        if (beforeDecimal == null) {
            System.err.println("Error (pullFirstFloat:BatchBuilderDBC) cannot parse:" + str);
            return null;
        }
        str = str.replaceFirst(beforeDecimal, "");
        int index = str.indexOf('.');
        if (index == 0) {
            str = str.substring(1, str.length());
            String afterDecimal = parseFirstNumber(str);
            if (afterDecimal == null) {
                afterDecimal = "0";
            }
            str = beforeDecimal + "." + afterDecimal;
            return str;
        }
        return beforeDecimal;
    }

    private String parseFirstNumber(String str) {
        if (str == null) {
            return null;
        }
        boolean start = false;
        boolean end = false;
        String number = "";
        for (char c : str.toCharArray()) {
            if (isNumberChar(c)) {
                if (!start && !end) {
                    number += c;
                    start = true;
                } else if (!end) {
                    number += c;
                }
            } else {
                if (start) {
                    end = true;
                }
            }
        }
        if (!number.equalsIgnoreCase("")) {
            return number;
        }
        return null;
    }

    private boolean isNumberChar(char c) {
        if (c == '\u0000') {
            return false;
        }
        if ((c == '0') || (c == '1') || (c == '2')
                || (c == '3') || (c == '4') || (c == '5')
                || (c == '6') || (c == '7') || (c == '8') || (c == '9')) {
            return true;
        }
        return false;
    }

    public void logTimmer(int eventId) {
        logEvent(getTimmer(), eventId);
    }

    public void startTimmer() {
        _clockStart = System.currentTimeMillis();
    }

    private String getTimmer() {
        return durationToString(System.currentTimeMillis() - _clockStart);
    }

    public String durationToString(long duration) {
        duration /= 1000;
        return String.format("%d:%02d:%02d", duration / 3600, (duration % 3600) / 60, (duration % 60));
    }

    public long durationToMillisecond(String duration) {
        int index = duration.indexOf(':');
        long result = -1;
        String hourStr = null;
        String minuteStr = null;
        String secondStr = null;
        if (index >= 0) {
            hourStr = duration.substring(0, index);
            duration = duration.substring(index + 1);
            index = duration.indexOf(':');
            if (index >= 0) {
                minuteStr = duration.substring(0, index);
                secondStr = duration.substring(index + 1);
                long hour = tryParseLong(hourStr);
                long minute = tryParseLong(minuteStr);
                long second = tryParseLong(secondStr);
                if ((hour >= 0) && (minute >= 0) && (second >= 0)) {
                    hour *= 3600;
                    minute *= 60;
                    result = (hour + minute + second) * 1000;
                }
            }
        }
        return result;
    }
}