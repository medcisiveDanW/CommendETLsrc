package com.medcisive.commend.database.site;

import com.medcisive.utility.LogUtility;
import com.medcisive.utility.Timer;
import com.medcisive.utility.sql2.DBC;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 *
 * @author vhapalchambj
 */
public class EncounterProcessingBucket extends com.medcisive.utility.sql2.DBCUtil {

    private String _sid;
    private String _sta3n;
    private String _wgId = null;
    private java.sql.Timestamp _startDate = null;
    private java.sql.Timestamp _endDate = null;
    private EncounterStatisticsBuilder.TemporalClass _temporalClass;
    private java.util.Map<EncounterClassifier.EncounterClass, java.util.Set<String>> _patients = new java.util.HashMap();
    private com.google.common.collect.Multiset<EncounterClassifier.EncounterClass> _encounters = com.google.common.collect.HashMultiset.create();
    private static java.util.Map<String, Double> _wRVUMap = new java.util.HashMap();
    private float _wRVU = 0;
    private float _multiplier = 0;
    private float _wRVUfilter = 0;
    private final static long _oneDay = 1000 * 60 * 60 * 24;
    private static long _maxDayRange = _oneDay * 365L * 4L;
    private ArrayList<FlatEncounter> listFlatEncounters = new ArrayList<FlatEncounter>();
    private static String EMPTY_STRING = "";
    private static String COMMA = ",";
    private java.util.Set<EncounterClassifier.EncounterClass> _individualSet = new java.util.HashSet();


    /* The int CPTPatterns array is used to count the number of occurrences of 
     * certain CPTPatterns in all enounters
     * The patterns and corresponding valueTypeID and brief descriptions are as follows:
    
     valueTypeID      CPTPattern      Description/Label
     100:                  0          Too many E&M 
     101:                  1          Prescribing 
     102:                  2          Intake
     103:                  3          AddOn With E&M
     104:   		   4          AddOn Only
     105:   		   5          Crisis
     106:   		   6          High Complexity
     107:   		   7          Medium Complexity
     108:   		   8          Low Complexity
     109:   		   9          Group Therapy
     110:   		  10          Prolonged Service
     111:   		  11          Interactive Complexity
     */
    private int[] CPTPatterns = new int[EncounterClassifier.NUM_CPT_PATTERNS];
    private static int[] valueTypeIDOfPatterns = new int[EncounterClassifier.NUM_CPT_PATTERNS];

    static {
        // This array is for convenience.  It allows mapping from the kth CPTPattern to 
        // the corresponding valueTypeID according to the listing above
        for (int k = 0; k < EncounterClassifier.NUM_CPT_PATTERNS; k++) {
            valueTypeIDOfPatterns[k] = 1000 + k;
        }
    }

    public EncounterProcessingBucket(Object sid, String sta3n, EncounterStatisticsBuilder.TemporalClass aTemporalClass, java.sql.Timestamp start, java.sql.Timestamp end) {
        if (sid.getClass().equals(String.class)) {
            _sid = "'" + (String) sid + "'";
        } else if (isInterface(sid, java.util.Set.class)) {
            _sid = _convertSetToSQLinStatment((java.util.Set<String>) sid);
        } else {
            _sid = null;
        }
        _sta3n = sta3n;
        _startDate = start;
        _endDate = end;
        _temporalClass = aTemporalClass;

        _individualSet.add(EncounterClassifier.EncounterClass.EVALUATION);
        _individualSet.add(EncounterClassifier.EncounterClass.PSYCHOTHERAPY_60MINUTE);
        _individualSet.add(EncounterClassifier.EncounterClass.PSYCHOTHERAPY_90MINUTE);
        _individualSet.add(EncounterClassifier.EncounterClass.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT);
        _individualSet.add(EncounterClassifier.EncounterClass.MEDICATION_MANAGEMENT);
        _individualSet.add(EncounterClassifier.EncounterClass.CASE_MANAGEMENT);

        _initWRVU();
        for (EncounterClassifier.EncounterClass ec : EncounterClassifier.EncounterClass.values()) {
            _patients.put(ec, new java.util.HashSet());
        }
        // initialize the CPTPatterns
        for (int k = 0; k < EncounterClassifier.NUM_CPT_PATTERNS; k++) {
            CPTPatterns[k] = 0;
        }
        
    }

    private void _initWRVU() {
        synchronized (_wRVUMap) {
            if (_wRVUMap.isEmpty()) {
                _wRVUMap = _getWRVUMap();
            }
        }
    }

    public void startProcessingEncounterBucket() {
        final DBC aDBCConn = _dest.clone();
        if (_sid != null && _startDate != null && _endDate != null) {
            String query
                    = "select distinct \n"
                    + "  	   stf.staffSID, \n"
                    + "  	   stf.staffIEN, \n"
                    + "  	   stf.staffName, \n"
                    + "  	   prv.sta3n, \n"
                    + "  	   prv.visitSID, \n"
                    + "  	   prv.PatientSID, \n"
                    + "  	   prv.visitDateTime, \n"
                    + "  	   vst.primaryStopCode, \n"
                    + "  	   prv.primarySecondary, \n"
                    + "  	   cptref.CPTCODE \n"
                    + "from VDWWork.Outpat.VProvider prv \n"
                    + " join VDWWork.SStaff.SStaff stf on \n"
                    + "      stf.staffSID = prv.providerSID \n"
                    + "  and stf.sta3n = prv.sta3n \n"
                    + " join VDWWork.Outpat.Visit vst on \n"
                    + "      vst.visitSID = prv.visitSID \n"
                    + "  and vst.sta3n = prv.sta3n \n"
                    + "  and vst.primaryStopCode >= 500 \n"
                    + "  and vst.primaryStopCode < 600 \n"
                    + " join VDWWork.Outpat.VProcedure vproc on \n"
                    + "      vproc.visitSID = prv.visitSID \n"
                    + "  and vproc.sta3n = prv.sta3n \n"
                    + " join VDWWork.Dim.CPT cptref on \n"
                    + "      cptref.CPTSID = vproc.CPTSID \n"
                    + "  and cptref.sta3n = vproc.sta3n \n"
                    + "where prv.sta3n = " + DBC.fixString(_sta3n) + " \n"
                    + "  and prv.providerSID in (" + _sid + ") \n"
                    + "  AND prv.visitDateTime BETWEEN " + DBC.fixTimestamp(_startDate) + " AND " + DBC.fixTimestamp(_endDate) + " \n"
                    + "order by prv.visitDateTime, prv.patientSID";
            _dest.query(query, new com.medcisive.utility.sql2.SQLObject() {
                EncounterClassifier.EncounterClass ec;
                java.util.TreeSet<String> cptSet;
                int errCount = 0;

                @Override
                public void row(ResultSet rs) throws SQLException {
                    //String sta3n = rs.getString("sta3n");
                    //String staffIEN = rs.getString("staffIEN");
                    //int staffSID = rs.getInt("staffSID");
                    String pSID = rs.getString("patientSID");
                    int[] thisEncPatterns;
                    try {
                        //LogUtility.warn("Processing, before getCptCodesFromRS");
                        // The getCptCodesFromRS method advances the rs cursor until
                        // all CPT codes from the same encounter are found, then the cursor
                        // is moved back to the last row of the encounter which contains the 
                        // set of CPT codes: cptSet
                        cptSet = EncounterClassifier.getCptCodesFromRS(rs);

                        float wRVUThisEncounter = EncounterClassifier.getWRVUTotalFromCPTSet(cptSet, _wRVUMap);
                        _wRVU += wRVUThisEncounter;

                        // We are NOT calculating the uncorrected productivity any more: 20140209
                        _wRVUfilter += 5.0;
                        // We are just filling in a number to avoid division by 0.0 errors

                        thisEncPatterns = EncounterClassifier.determinePatterns(cptSet);
                        for (int k = 0; k < EncounterClassifier.NUM_CPT_PATTERNS; k++) {
                            CPTPatterns[k] = CPTPatterns[k] + thisEncPatterns[k];
                        }

                        ec = EncounterClassifier.getHighestPriorityClass(cptSet);
                        
                        // Ensure that the last month's period DOES NOT overlap with the year to ensure
                        // that COMMENDVISNFlatEncounters does not have duplicate rows over the last
                        // month
                        boolean toDoForLastMonth = false;
                        if (_temporalClass == EncounterStatisticsBuilder.TemporalClass.MONTH) {
                           if (_startDate.after(EncounterStatisticsBuilder._rollingQuarterStart.get(0)) )   
                              toDoForLastMonth = true;                            
                        }
                        // Using rs, save data fields corresponding to this encounter
                        if ( (_temporalClass == EncounterStatisticsBuilder.TemporalClass.YEAR || toDoForLastMonth) && !_sid.contains(",")) {
                            // skip this processing for 4 quarters because it is covered by year                            
                            FlatEncounter aFlatEnc = obtainFlatEncounter(rs, cptSet, ec, wRVUThisEncounter);
                            if (aFlatEnc != null) {
                                listFlatEncounters.add(aFlatEnc);
                            }
                            saveListFlatEncountersToDB(aDBCConn, false);
                        }
                        if (_individualSet.contains(ec)) {
                            _encounters.add(EncounterClassifier.EncounterClass.INDIVIDUAL);
                            _patients.get(EncounterClassifier.EncounterClass.INDIVIDUAL).add(pSID);
                        }
                        _encounters.add(ec);
                        _patients.get(ec).add(pSID);
                        _encounters.add(EncounterClassifier.EncounterClass.TOTAL);
                        _patients.get(EncounterClassifier.EncounterClass.TOTAL).add(pSID);

                    } catch (Exception e) {
                        errCount++;
                        LogUtility.error(" ERROR processing --- sta3n: " + rs.getString("sta3n") + " prvdr: " + rs.getString("StaffSID") + " pt: " + pSID + " visitSID: " + rs.getString("visitSID"));
                        if (errCount < 3) {
                            e.printStackTrace();
                        }
                    }
                }

                public void post() {
                    // post processing of all rows of data.  Check to see that   
                    // the last remaining rows of flat encounters are saved
                    saveListFlatEncountersToDB(aDBCConn, true);
                }
            });
            _multiplier = _getClinicHours();
        }
    }

    private FlatEncounter obtainFlatEncounter(ResultSet rs, java.util.TreeSet cptSet, EncounterClassifier.EncounterClass ec, float aWRVUValue) {
        FlatEncounter resFlatEnc = null;
        String sta3n = null;
        String staffIEN, staffName, primarySecondary, encClass;
        java.sql.Timestamp visitDateTime = null;
        int staffSID = 0;
        int patientSID = 0;
        int primaryStopCode = 0;
        try {
            sta3n = rs.getString("sta3n");
            staffSID = rs.getInt("staffSID");
            staffIEN = rs.getString("staffIEN");
            staffName = rs.getString("staffName");
            patientSID = rs.getInt("PatientSID");
            visitDateTime = rs.getTimestamp("visitDateTime");
            primarySecondary = rs.getString("primarySecondary");
            primaryStopCode = rs.getInt("primaryStopCode");
            ArrayList<String> listCPTCodes = new ArrayList<String>();
            for (int i = 0; i < 8; i++) {
                listCPTCodes.add(EMPTY_STRING);
            }
            if (cptSet == null || cptSet.size() <= 0) { // do nothing, so CPT codes are empty strings
            } else {
                int index = 0;
                Iterator it = cptSet.iterator();
                while (it.hasNext()) {
                    if (index > 7) {
                        break;
                    }      // only allow for a maximum of 8 CPTCodes;
                    String aCPTCode = (String) it.next();
                    listCPTCodes.add(index, aCPTCode);
                    index++;
                }
            }
            if (ec.equals(EncounterClassifier.EncounterClass.EVALUATION)) {
                encClass = "EVALUATION";
            } else if (ec.equals(EncounterClassifier.EncounterClass.PSYCHOTHERAPY_60MINUTE)) {
                encClass = "PSYCHOTHERAPY_60MINUTE";
            } else if (ec.equals(EncounterClassifier.EncounterClass.PSYCHOTHERAPY_90MINUTE)) {
                encClass = "PSYCHOTHERAPY_90MINUTE";
            } else if (ec.equals(EncounterClassifier.EncounterClass.PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT)) {
                encClass = "PSYCHOTHERAPY_WITH_MEDICATION_MANAGEMENT";
            } else if (ec.equals(EncounterClassifier.EncounterClass.CASE_MANAGEMENT)) {
                encClass = "CASE_MANAGEMENT";
            } else if (ec.equals(EncounterClassifier.EncounterClass.MEDICATION_MANAGEMENT)) {
                encClass = "MEDICATION_MANAGEMENT";
            } else if (ec.equals(EncounterClassifier.EncounterClass.GROUP)) {
                encClass = "GROUP";
            } else if (ec.equals(EncounterClassifier.EncounterClass.TELEPHONE)) {
                encClass = "TELEPHONE";
            }else {
                encClass = "OTHER";
            }
                 
            resFlatEnc = new FlatEncounter(sta3n,
                    staffIEN, staffSID, staffName,
                    EMPTY_STRING, patientSID, EMPTY_STRING,
                    visitDateTime,
                    primarySecondary, primaryStopCode,
                    listCPTCodes.get(0), listCPTCodes.get(1), listCPTCodes.get(2), listCPTCodes.get(3),
                    listCPTCodes.get(4), listCPTCodes.get(5), listCPTCodes.get(6), listCPTCodes.get(7),
                    encClass, aWRVUValue);

            return resFlatEnc;
        } catch (Exception e) {
            LogUtility.error("Exception obtaining FlatEncounter for  " + sta3n + " prov. " + staffSID + " pt. " + patientSID + " at time: " + visitDateTime);
            resFlatEnc = null;
            return resFlatEnc;
        }
    }

    /*
     * This method serves to save the FlatEncounters we have obtained into the 
     * database table COMMENDVISNFlatEncounters.
     * We shall do a save whenever the number in listFlatEncounters exceeds a 
     * define number, NUM_TO_SAVE_IN_BATCH, which is set to 50 or more or 
     * if we force a save by using the flag bForceSave (assuming that
     * listFlatEncounters is not empty)
     */
    public void saveListFlatEncountersToDB(DBC aDBCConn, boolean bForceSave) {
        int NUM_TO_SAVE_IN_BATCH = 100;
        if (listFlatEncounters == null || listFlatEncounters.size() <= 0) {
            return;
        }
        // Here when listFlatEncounters contains some elements
        if (!bForceSave) {
            // Here when not doing a forceSave. Check to see if the list size is 
            // is big enough
            if (listFlatEncounters.size() < NUM_TO_SAVE_IN_BATCH) {
                return;
            }
        }
        StringBuffer batchInserts = new StringBuffer();
        for (int j = 0; j < listFlatEncounters.size(); j++) {
            FlatEncounter aFlatEnc = listFlatEncounters.get(j);
            String insertForFlatEnc = obtainInsertForFlatEnc(aFlatEnc);
            batchInserts.append(insertForFlatEnc);
        }

        // save into Database
        aDBCConn.update(batchInserts.toString());
        listFlatEncounters.clear();
    }

    private String obtainInsertForFlatEnc(FlatEncounter aFlatEnc) {
        if (aFlatEnc == null) {
            return EMPTY_STRING;
        }
        StringBuffer insertStmt = new StringBuffer();
        insertStmt.append("insert into Commend.dbo.CommendVISNFlatEncounters ");
        insertStmt.append(" (sta3n, providerSID, providerName, patientSID, visitDateTime,");
        insertStmt.append("primarySecondary,primaryStopCode,");
        insertStmt.append("cptCode0,cptCode1,cptCode2,cptCode3,cptCode4,cptCode5,cptCode6,cptCode7,");
        insertStmt.append("encounterClass,wRVUValue) VALUES ");

        insertStmt.append("(").append(DBC.fix(aFlatEnc.sta3n)).append(COMMA);
        insertStmt.append(aFlatEnc.providerSID).append(COMMA);
        insertStmt.append(DBC.fix(aFlatEnc.providerName)).append(COMMA);
        insertStmt.append(aFlatEnc.patientSID).append(COMMA);
        insertStmt.append(DBC.fix(aFlatEnc.visitDateTime)).append(COMMA);
        insertStmt.append(DBC.fix(aFlatEnc.primarySecondary)).append(COMMA);
        insertStmt.append(DBC.fix(aFlatEnc.primaryStopCode)).append(COMMA);

        insertStmt.append(DBC.fix(aFlatEnc.cptCode0)).append(COMMA);
        insertStmt.append(DBC.fix(aFlatEnc.cptCode1)).append(COMMA);
        insertStmt.append(DBC.fix(aFlatEnc.cptCode2)).append(COMMA);
        insertStmt.append(DBC.fix(aFlatEnc.cptCode3)).append(COMMA);
        insertStmt.append(DBC.fix(aFlatEnc.cptCode4)).append(COMMA);
        insertStmt.append(DBC.fix(aFlatEnc.cptCode5)).append(COMMA);
        insertStmt.append(DBC.fix(aFlatEnc.cptCode6)).append(COMMA);
        insertStmt.append(DBC.fix(aFlatEnc.cptCode7)).append(COMMA);

        insertStmt.append(DBC.fix(aFlatEnc.encounterClass)).append(COMMA);
        insertStmt.append(aFlatEnc.wRVUValue).append(")");

        return insertStmt.toString();
    }

    public StringBuilder insertStatment(int offset, int offsetId) {
        StringBuilder result = new StringBuilder();
        if (_sid == null) {
            return result;
        }
        String insert = "INSERT INTO Commend.dbo.CommendVISNPrvdrAnalysis (staffSta3n,staffSID,staffIEN,wgID,temporalOffset,tempOffsetTypeID,valueTypeID,value) \n";
        String update;
        for (EncounterClassifier.EncounterClass ec : EncounterClassifier.EncounterClass.values()) {
            if (_sid != null && _sid.contains(",")) {
                _sid = null;
            }
            update  = insert
                    + "VALUES ("
                    + DBC.fixString(_sta3n) + ","
                    + _sid + ","
                    + "null,"
                    + DBC.fixString(_wgId) + ","
                    + offset + ","
                    + offsetId + ","
                    + "1" + ec.ordinal() + ","
                    + _encounters.count(ec) + ") \n";
            result.append(update);
            update  = insert
                    + "VALUES ("
                    + DBC.fixString(_sta3n) + ","
                    + _sid + ","
                    + "null,"
                    + DBC.fixString(_wgId) + ","
                    + offset + ","
                    + offsetId + ","
                    + "2" + ec.ordinal() + ","
                    + _patients.get(ec).size() + ") \n";
            result.append(update);
        }
        update  = insert
                + "VALUES ("
                + DBC.fixString(_sta3n) + ","
                + _sid + ","
                + "null,"
                + DBC.fixString(_wgId) + ","
                + offset + ","
                + offsetId + ","
                + "3,"
                + _wRVU + ") \n";
        result.append(update);
        update  = insert
                + "VALUES ("
                + DBC.fixString(_sta3n) + ","
                + _sid + ","
                + "null,"
                + DBC.fixString(_wgId) + ","
                + offset + ","
                + offsetId + ","
                + "4,"
                + _multiplier + ") \n";
        result.append(update);
        update  = insert
                + "VALUES ("
                + DBC.fixString(_sta3n) + ","
                + _sid + ","
                + "null,"
                + DBC.fixString(_wgId) + ","
                + offset + ","
                + offsetId + ","
                + "5,"
                + _wRVUfilter + ") \n";
        result.append(update);

        for (int j = 0; j < EncounterClassifier.NUM_CPT_PATTERNS; j++) {
            update  = insert
                    + "VALUES ("
                    + DBC.fixString(_sta3n) + ","
                    + _sid + ","
                    + "null,"
                    + DBC.fixString(_wgId) + ","
                    + offset + ","
                    + offsetId + ","
                    + valueTypeIDOfPatterns[j] + ","
                    + CPTPatterns[j] + ") \n";
            result.append(update);
        }
        return result;
    }

    public void print() {
        System.out.println("SID: " + _sid);
        for (EncounterClassifier.EncounterClass ec : EncounterClassifier.EncounterClass.values()) {
            System.out.println(ec + " " + _patients.get(ec).size() + " / " + _encounters.count(ec));
        }
    }

    public void setWgId(String wgId) {
        _wgId = wgId;
    }

    class ProviderClinicTime {

        public int sta3n;
        public int providerSID;
        public float clinicTimeRate = 0.0f;
        public java.sql.Timestamp start;
        public java.sql.Timestamp finish;
        public float multiplier = 0.0f;

        public ProviderClinicTime() {
            start = new java.sql.Timestamp(0);
            finish = new java.sql.Timestamp(System.currentTimeMillis() + _oneDay);
        }

        public ProviderClinicTime(java.sql.ResultSet rs) throws SQLException {
            sta3n = rs.getInt("Sta3n");
            providerSID = rs.getInt("ProviderSID");
            clinicTimeRate = rs.getFloat("ClinicTimeRate");
            start = rs.getTimestamp("Start");
            finish = new java.sql.Timestamp(System.currentTimeMillis() + _maxDayRange);
        }

        public boolean isInRange(java.sql.Timestamp begin, java.sql.Timestamp end) {
            boolean result = false;
            if (isBetween(begin) || isBetween(end)) {
                result = true;
            }
            if (result) {
                java.sql.Timestamp s, e;
                if (begin.getTime() > start.getTime()) {
                    s = begin;
                } else {
                    s = start;
                }
                if (end.getTime() < finish.getTime()) {
                    e = end;
                } else {
                    e = finish;
                }
                double daysInRange = (double) (e.getTime() - s.getTime()) / _oneDay;
                double totalRange = (double) (end.getTime() - begin.getTime()) / _oneDay;
                double fractionOfDays = daysInRange / totalRange;
                multiplier = (float) (fractionOfDays * clinicTimeRate);
            }
            return result;
        }

        public void print() {
            System.out.println("Clinic Hour Object:");
            System.out.println("    Sta3n           : " + sta3n);
            System.out.println("    providerSID     : " + providerSID);
            System.out.println("    clinicTimeRate  : " + clinicTimeRate);
            System.out.println("    start           : " + start);
            System.out.println("    finish          : " + finish);
            System.out.println("    multiplier      : " + multiplier);
        }

        public void truncate(ProviderClinicTime next) {
            if (start.getTime() < next.start.getTime() && finish.getTime() > next.start.getTime()) {
                finish = next.start;
            }
        }

        private boolean isBetween(java.sql.Timestamp ts) {
            return ts != null && ts.getTime() > start.getTime() && ts.getTime() < finish.getTime();
        }
    }

    private float _getClinicHours() {
        String query
                = "SELECT Sta3n \n"
                + "      ,ProviderSID \n"
                + "      ,ClinicTimeRate \n"
                + "      ,Start \n"
                + "FROM Commend.dbo.CommendVISNProviderClinicTime \n"
                + "WHERE ProviderSID in (" + _sid + ") \n"
                + "ORDER BY Start ASC";
        final java.util.List<ProviderClinicTime> list = new java.util.ArrayList();

        _dest.query(query, new com.medcisive.utility.sql2.SQLObject() {
            ProviderClinicTime current;

            @Override
            public void row(ResultSet rs) throws SQLException {
                current = new ProviderClinicTime(rs);
                list.add(current);
            }

            @Override
            public void post() {
//                ProviderClinicTime historicSpan = new ProviderClinicTime();
//                historicSpan.start = new java.sql.Timestamp(System.currentTimeMillis() - _maxDayRange);
//                if (earliestTS != null) {
//                    historicSpan.finish = earliestTS;
//                } else {
//                    historicSpan.finish = new java.sql.Timestamp(System.currentTimeMillis());
//                }
//                list.add(historicSpan);
            }
        });
        ProviderClinicTime prevClinic = null;
        for (ProviderClinicTime pct : list) {
            if (prevClinic == null) {
                prevClinic = pct;
            } else {
                prevClinic.truncate(pct);
                prevClinic = pct;
            }
        }
        java.util.List<ProviderClinicTime> remander = _removeOutlierProviderClinicTime(list);
        if (remander.isEmpty()) {
            remander.add(new ProviderClinicTime());
        }
        float result = 0;
        //System.out.println("_startDate:" + _startDate + " _endDate:" + _endDate);
        for (ProviderClinicTime pct : remander) {
            //System.out.println("    pct.multiplier:" + pct.multiplier);
            result += pct.multiplier;
        }
        //System.out.println("result:" + result);
        return result;
    }

    private java.util.List<ProviderClinicTime> _removeOutlierProviderClinicTime(java.util.List<ProviderClinicTime> list) {
        java.util.List<ProviderClinicTime> result = new java.util.ArrayList();
        for (ProviderClinicTime pct : list) {
            if (pct.isInRange(_startDate, _endDate)) {
                result.add(pct);
            }
        }
        return result;
    }

    private boolean isInterface(Object a, Class b) {
        if (a == null || b == null) {
            return false;
        }
        Class[] aInterfaceArray = a.getClass().getInterfaces();
        for (Class c : aInterfaceArray) {
            if (b.equals(c)) {
                return true;
            }
        }
        return false;
    }

    private String _convertSetToSQLinStatment(java.util.Set<String> set) {
        String result = "'";
        for (String s : set) {
            result += s + "','";
        }
        result = result.substring(0, result.lastIndexOf(",'"));
        return result;
    }

    private java.util.Map<String, Double> _getWRVUMap() {
        final java.util.Map<String, Double> result = new java.util.HashMap();
        Timer t = Timer.start();
        String query
                = "SELECT CPTCode \n"
                + "     ,wRVU \n"
                + "FROM Commend.dbo.CommendVISNwRVUMap";
        _dest.query(query, new com.medcisive.utility.sql2.SQLObject() {

            @Override
            public void row(ResultSet rs) throws SQLException {
                String CPTCode = rs.getString("CPTCode");
                float wRVU = rs.getFloat("wRVU");
                result.put(CPTCode, new Double(wRVU));
            }
        });
        t.print();
        return result;
    }
}

class FlatEncounter {

    public String sta3n;
    public String providerDUZ;
    public int providerSID;
    public String providerName;
    public String patientIEN;
    public int patientSID;
    public String patientName;
    public java.sql.Timestamp visitDateTime;
    public String primarySecondary;
    public int primaryStopCode;
    public String cptCode0;
    public String cptCode1;
    public String cptCode2;
    public String cptCode3;
    public String cptCode4;
    public String cptCode5;
    public String cptCode6;
    public String cptCode7;
    public String encounterClass;
    public float wRVUValue;

    public FlatEncounter(String aSta3n,
            String aProvDUZ,
            int aProvSID,
            String aProvName,
            String aPtIEN,
            int aPtSID,
            String aPtName,
            java.sql.Timestamp aVisitDateTime,
            String aPrimSec,
            int aPrimaryStopCode,
            String code0,
            String code1,
            String code2,
            String code3,
            String code4,
            String code5,
            String code6,
            String code7,
            String anEncClass,
            float aWRVUValue) {

        sta3n = aSta3n;
        providerDUZ = aProvDUZ;
        providerSID = aProvSID;
        providerName = aProvName;
        patientIEN = aPtIEN;
        patientSID = aPtSID;
        patientName = aPtName;
        visitDateTime = aVisitDateTime;
        primarySecondary = aPrimSec;
        primaryStopCode = aPrimaryStopCode;
        cptCode0 = code0;
        cptCode1 = code1;
        cptCode2 = code2;
        cptCode3 = code3;
        cptCode4 = code4;
        cptCode5 = code5;
        cptCode6 = code6;
        cptCode7 = code7;
        encounterClass = anEncClass;
        wRVUValue = aWRVUValue;
    }

}
