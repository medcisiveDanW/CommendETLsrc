package com.medcisive.commend.database.site;

import com.medcisive.utility.Timer;
import com.medcisive.utility.sql2.DBC;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
/**
 *
 * @author vhapalchambj
 */
public class OEF4StatisticsBuilder extends com.medcisive.utility.sql2.DBCUtil {
    private static final java.util.ArrayList<java.sql.Timestamp> _rollingMonthStart = new java.util.ArrayList();
    private static final int _numberOfMonthsToProcess = 60;

    static {
        int year = Calendar.getInstance().get(Calendar.YEAR);
        int month = Calendar.getInstance().get(Calendar.MONTH);
        Calendar c = Calendar.getInstance();
        year += 2;
        c.set(year, month, 1, 0, 0, 0);
        for(int i = 0; i < _numberOfMonthsToProcess; i++) {
            _rollingMonthStart.add(new java.sql.Timestamp(c.getTimeInMillis()));
            c.roll(Calendar.MONTH, -1);
            if(c.get(Calendar.MONTH)==Calendar.DECEMBER) {
                c.set(Calendar.YEAR, c.get(Calendar.YEAR)-1);
            }
        }
    }

    public OEF4StatisticsBuilder() {}

    public static Thread createThread(final int sta3n) {
        
        return new Thread() {
            OEF4StatisticsBuilder visnApptSummaryBuilder = new OEF4StatisticsBuilder();
            
            @Override
            public void run() {
                visnApptSummaryBuilder._buildOEF4Statistics();
            }
        };
    }

    private void _buildOEF4Statistics() { // starts from this month and counts backwards for 12 months.
        Timer t = Timer.start();
        String query = "DELETE FROM Commend.dbo.CommendVISNOEF4Counts";
        _dest.update(query);
        Calendar c = Calendar.getInstance();
        java.lang.StringBuilder result = new java.lang.StringBuilder();
        for(int i = 0; i < _numberOfMonthsToProcess-1; i++) {
            java.sql.Timestamp start = _rollingMonthStart.get(i+1);
            java.sql.Timestamp end = _rollingMonthStart.get(i);
            c.setTimeInMillis(start.getTime());
            java.util.Map<String,com.google.common.collect.Multiset<String>> map = _getProviderDateRangeCounts(start,end);
            for(String sid : map.keySet()) {
                com.google.common.collect.Multiset<String> set = map.get(sid);
                int complete = set.count("Complete");
                int incomplete = set.count("Incomplete");
                int failed = set.count("Failed");
                int qualified = complete + incomplete + failed;
                query =
                        "INSERT INTO Commend.dbo.CommendVISNOEF4Counts(providerSID,spanStart,spanEnd,complete,incomplete,failed,qualified) \n"
                        + "VALUES ("+ DBC.fixString(sid) + ","
                                    + DBC.fixTimestamp(start) + ","
                                    + DBC.fixTimestamp(end)  + ","
                                    + DBC.fixString("" + complete) + ","
                                    + DBC.fixString("" + incomplete) + ","
                                    + DBC.fixString("" + failed) + ","
                                    + DBC.fixString("" + qualified) + ") \n";
                result.append(query);
            }
        }
        _dest.update(result.toString());
        t.print();
    }

    private java.util.Map _getProviderDateRangeCounts(java.sql.Timestamp start, java.sql.Timestamp end) {
        final java.util.Map<String,com.google.common.collect.Multiset> result = new java.util.HashMap();
        if(start==null||end==null) {
            return null;
        }
        String query =
                "SELECT \n"
                + "     pan.staffSID \n"
                + "     ,enc.status \n"
                + "FROM \n"
                + "	Commend.dbo.CommendVISNPatientPanel pan \n"
                + "	,Commend.dbo.CommendVISNEncSummary enc \n"
                + "	,Commend.dbo.CommendVISNPatient ser \n"
                + "WHERE \n"
                + "     enc.patientSID = pan.patientSID \n"
                + " AND enc.patientSID = ser.PatientSID \n"
                + " AND enc.status != 'Exclude' \n"
                + " AND enc.status != 'NoQualifyingDate' \n"
                + " AND enc.deadline BETWEEN " + DBC.fixTimestamp(start) + " AND " + DBC.fixTimestamp(end) + " \n"
                + " AND enc.measureType != 'RM' \n"
                + " AND ser.OEFOIFService = 'Y' \n"
                + " AND (pan.FYM1Flag = 'Y' or pan.FYM1Flag = 'Y') \n"
                + "ORDER BY pan.patientName";
        _dest.query(query, new com.medcisive.utility.sql2.SQLObject() {

            @Override
            public void row(ResultSet rs) throws SQLException {
                String SID = rs.getString("staffSID");
                String status = rs.getString("status");
                com.google.common.collect.Multiset<String> set = result.get(SID);
                if(set==null) {
                    set = com.google.common.collect.HashMultiset.create();
                    set.add(status);
                    result.put(SID, set);
                }
                else {
                    set.add(status);
                }
            }
        });
        return result;
    }
}