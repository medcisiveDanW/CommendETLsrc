package com.medcisive.commend.database.site;

import com.medcisive.utility.LogUtility;
import com.medcisive.utility.Timer;
import com.medcisive.utility.sql2.DBC;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author vhapalchambj
 */
public class VISNApptSummaryBuilder extends com.medcisive.utility.sql2.DBCUtil {
    private static final java.util.ArrayList<String> _inserts = new java.util.ArrayList();
    private static final java.util.Date _todayDate = new java.util.Date();
    private static final java.sql.Timestamp _today = new java.sql.Timestamp(_todayDate.getTime());
    private DBC _destSecondary;
    private int _sta3n;
    private float _totalCount;
    private float _currentCount = 0;
    private final static java.text.NumberFormat _nf = java.text.NumberFormat.getInstance();

    public VISNApptSummaryBuilder(int sta3n) {
        _sta3n = sta3n;
        _destSecondary = _dest.clone();
    }

    public static Thread createThread(final int sta3n) {
        Thread result = new Thread() {
            VISNApptSummaryBuilder visnApptSummaryBuilder = new VISNApptSummaryBuilder(sta3n);
            
            @Override
            public void run() {
                visnApptSummaryBuilder.executingVISNApptSummaryBuilder();
            }
        };
        result.start();
        return result;
    }
    
    private void executingVISNApptSummaryBuilder() {
        Timer t = Timer.start();
        String query =
                "DELETE FROM Commend.dbo.CommendVISNApptSummary \n"
                + "WHERE sta3n = " + _sta3n;
        this.
        _dest.update(query);
        query =
                "SELECT count(distinct patientSID) AS patientCount \n"
                + "FROM Commend.dbo.VISNApptSummaryRawView \n"
                + "WHERE sta3n = " + _sta3n;
        _dest.query(query, new com.medcisive.utility.sql2.SQLObject() {

            @Override
            public void row(ResultSet rs) throws SQLException {
                _totalCount = rs.getInt("patientCount");
            }
        });
        query =
                "SELECT sta3n \n"
                + "     ,patientSID \n"
                + "     ,AppointmentDateTime \n"
                + "     ,CancelNoShowCode \n"
                + "     ,PrimaryStopCode \n"
                + "FROM Commend.dbo.VISNApptSummaryRawView \n"
                + "WHERE sta3n = " + _sta3n + " \n"
                + "ORDER BY patientSID, AppointmentDateTime desc";
        _dest.query(query, new com.medcisive.utility.sql2.SQLObject() {

            @Override
            public void row(ResultSet rs) throws SQLException {
                _processResultSet(rs);
            }
        });
        t.print();
    }

    private void _processResultSet(ResultSet rs) {
        String prevSID = null;
        String patientSID;
        int missedMH = 0;
        int totMHOneYear = 0;
        int missedNonMH = 0;
        int totNonMHOneYear = 0;
        int numAppts8weeks = 0;
        try {
            while (rs.next()) {
                patientSID = rs.getString("patientSID");
                java.sql.Timestamp AppointmentDateTime = rs.getTimestamp("AppointmentDateTime");
                String CancelNoShowCode = rs.getString("CancelNoShowCode");
                String PrimaryStopCode = rs.getString("PrimaryStopCode");
                if(prevSID==null) {
                    prevSID = patientSID;
                }
                if(!prevSID.equalsIgnoreCase(patientSID)) {
                    try {
                        _insertAppointment(_sta3n,Integer.parseInt(prevSID),missedMH,totMHOneYear,missedNonMH,totNonMHOneYear,numAppts8weeks);
                        _currentCount++;
                        prevSID = patientSID;
                        missedMH = 0;
                        totMHOneYear = 0;
                        missedNonMH = 0;
                        totNonMHOneYear = 0;
                        numAppts8weeks = 0;
                    } catch(java.lang.NumberFormatException e) {
                        LogUtility.error(e);
                    }
                }
                int psc = Integer.parseInt(PrimaryStopCode);
                if((psc>499) && (psc<600)) { // MH appt
                    totMHOneYear++; // AppointmentStatus was changed to CancelNoShowCode in the new RDW tables
                    if(CancelNoShowCode!=null && CancelNoShowCode.equalsIgnoreCase("N")) { // this was "NO-SHOW" RDW changed it to N
                        missedMH++;
                    }
                } else {
                    totNonMHOneYear++;
                    if(CancelNoShowCode!=null && CancelNoShowCode.equalsIgnoreCase("N")) {
                        missedNonMH++;
                    }
                    if(AppointmentDateTime.getTime()>_today.getTime()) { numAppts8weeks++; }
                }
            }
            _pushData();
        } catch (java.sql.SQLException e) {
            LogUtility.error(e);
        }
    }

    private void _insertAppointment(int sta3n, int patientSID, int missedMH, int totMHOneYear, int missedNonMH, int totNonMHOneYear, int numAppts8weeks) {
        float pcMissedMH = 0;
        float pcMissedNonMH = 0;
        if(totMHOneYear>0) {
            pcMissedMH = ((float)missedMH/(float)totMHOneYear)*100.0f;
        }
        if(totNonMHOneYear>0) {
            pcMissedNonMH = ((float)missedNonMH/(float)totNonMHOneYear)*100.0f;
        }
        String query =
                "INSERT INTO Commend.dbo.CommendVISNApptSummary (sta3n,patientSID,pcMissedMH,totMHOneYear,pcMissedNonMH,totNonMHOneYear,numAppts8weeks) \n"
                + "VALUES (" + sta3n + "," + patientSID + ","
                + pcMissedMH + "," + totMHOneYear + "," + pcMissedNonMH + "," + totNonMHOneYear + "," + numAppts8weeks + ")";
        _inserts.add(query);
        if(_inserts.size()>5000) { _pushData(); }
    }

    private void _pushData() {
        synchronized(_inserts) {
            java.lang.StringBuilder sb = new java.lang.StringBuilder();
            for(String insert : _inserts) {
                sb.append(insert);
            }
            _inserts.clear();
            System.out.println(_nf.format((_currentCount/_totalCount)*100) + "% complete");
            _destSecondary.update(sb.toString());
        }
    }
}
