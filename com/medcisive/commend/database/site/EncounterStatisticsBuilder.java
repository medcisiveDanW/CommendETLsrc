package com.medcisive.commend.database.site;

import com.medcisive.utility.LogUtility;
import com.medcisive.utility.Timer;
import com.medcisive.utility.sql2.DBC;
import com.medcisive.utility.sql2.SQLTable;
import java.util.Calendar;

/**
 *
 * @author vhapalchambj
 */
public class EncounterStatisticsBuilder extends com.medcisive.utility.sql2.DBCUtil {
    public static final java.util.ArrayList<java.sql.Timestamp> _rollingMonthStart = new java.util.ArrayList();
    public static final java.util.ArrayList<java.sql.Timestamp> _rollingQuarterStart = new java.util.ArrayList();
    private static final int _numberOfMonthsToProcess = 25;
    private static final int _numberOfQuartersToProcess = 5; // 4 quarters need 5 date points so _numberOfQuartersToProcess = numbOfQuarters + 1
    private SQLTable _providers = null;
    private java.util.Map<String,SQLTable> _groups = null;
    private java.util.List<GroupStatisticsBuilder> _globalGSBList = new java.util.ArrayList();
    private java.sql.Timestamp _today = new java.sql.Timestamp(System.currentTimeMillis());

    static {
        Calendar aCal = Calendar.getInstance();
        int year = aCal.get(Calendar.YEAR);
        int month = aCal.get(Calendar.MONTH);
        int quarter = month/3 + 1;
        Calendar c = Calendar.getInstance();
        c.set(year, month, 1, 0, 0, 0);
        for(int i = 0; i < _numberOfMonthsToProcess; i++) {
            _rollingMonthStart.add(new java.sql.Timestamp(c.getTimeInMillis()));
            c.roll(Calendar.MONTH, -1);
            if(c.get(Calendar.MONTH)==Calendar.DECEMBER) {
                c.set(Calendar.YEAR, c.get(Calendar.YEAR)-1);
            }
        }
        c.set(year, month, 1, 0, 0, 0);
        switch(quarter) {
            case 1:
                c.set(Calendar.MONTH, Calendar.JANUARY);
                break;
            case 2:
                c.set(Calendar.MONTH, Calendar.APRIL);
                break;
            case 3:
                c.set(Calendar.MONTH, Calendar.JULY);
                break;
            case 4:
                c.set(Calendar.MONTH, Calendar.OCTOBER);
                break;
            default:
                break;
        }
        for(int i = 0; i < _numberOfQuartersToProcess; i++) {
            _rollingQuarterStart.add(new java.sql.Timestamp(c.getTimeInMillis()));
            c.roll(Calendar.MONTH, -3);
            if(c.get(Calendar.MONTH)==Calendar.OCTOBER) {
                c.set(Calendar.YEAR, c.get(Calendar.YEAR)-1);
            }
        }
    }

    public enum TemporalClass {
        MONTH,
        QUARTER,
        YEAR
    }

    public EncounterStatisticsBuilder() { }
    
    public static Thread createThread() {
        Thread result = new Thread() {
            EncounterStatisticsBuilder esb = new EncounterStatisticsBuilder();
            @Override
            public void run() {
                esb.buildEncounterStatistics();
            }
        };
        result.start();
        return result;
    }

    public void buildEncounterStatistics() {
        Timer t = Timer.start();
        _providers = _getProviders();
        _groups = _getGroups();
        _dest.update("DELETE FROM Commend.dbo.CommendVISNPrvdrAnalysis");
        _dest.update("DELETE FROM Commend.dbo.CommendVISNFlatEncounters");
        new GroupStatisticsBuilder().clear();
        System.out.println("Start:" + _rollingQuarterStart.get(0) + " End:" + _today);
        BucketProcessor bpMonth = new BucketProcessor(_rollingQuarterStart.get(0),_today,0,TemporalClass.MONTH);
        bpMonth.start();
        BucketProcessor bpYear = new BucketProcessor(_rollingQuarterStart.get(4),_rollingQuarterStart.get(0),0,TemporalClass.YEAR);
        bpYear.start();
        BucketProcessor bpQuarter1 = new BucketProcessor(_rollingQuarterStart.get(1),_rollingQuarterStart.get(0),0,TemporalClass.QUARTER);
        bpQuarter1.start();
        BucketProcessor bpQuarter2 = new BucketProcessor(_rollingQuarterStart.get(2),_rollingQuarterStart.get(1),1,TemporalClass.QUARTER);
        bpQuarter2.start();
        BucketProcessor bpQuarter3 = new BucketProcessor(_rollingQuarterStart.get(3),_rollingQuarterStart.get(2),2,TemporalClass.QUARTER);
        bpQuarter3.start();
        BucketProcessor bpQuarter4 = new BucketProcessor(_rollingQuarterStart.get(4),_rollingQuarterStart.get(3),3,TemporalClass.QUARTER);
        bpQuarter4.start();
        try {
            bpMonth.join();
            bpQuarter1.join();
            bpQuarter2.join();
            bpQuarter3.join();
            bpQuarter4.join();
            bpYear.join();
            _cleanupFlatEncounters();
            _globalGSBList.addAll(bpMonth.GSBList);
            _globalGSBList.addAll(bpQuarter1.GSBList);
            _globalGSBList.addAll(bpQuarter2.GSBList);
            _globalGSBList.addAll(bpQuarter3.GSBList);
            _globalGSBList.addAll(bpQuarter4.GSBList);
            _globalGSBList.addAll(bpYear.GSBList);
            for(GroupStatisticsBuilder gsb : _globalGSBList){
                gsb.process();
            }
        } catch (java.lang.InterruptedException e) {LogUtility.error(e);}
//        EncounterClassifier.printUnknownCPTCodeSet();
        t.print();
    }

    public void buildEncounterStatistics(java.util.List list) {
        if(list==null || list.isEmpty()) { return; }
        Timer t = Timer.start();
        _providers = _getProviders(list);
        _groups = null;
        _dest.update("DELETE FROM Commend.dbo.CommendVISNPrvdrAnalysis \n"
                   + "WHERE staffSID in " + DBC.javaListToSQLList(list));
        _dest.update("DELETE FROM Commend.dbo.CommendVISNFlatEncounters \n"
                   + "WHERE providerSID in " + DBC.javaListToSQLList(list));        
        System.out.println("Start:" + _rollingQuarterStart.get(0) + " End:" + _today);
        BucketProcessor bpMonth = new BucketProcessor(_rollingQuarterStart.get(0),_today,0,TemporalClass.MONTH);
        bpMonth.start();
        BucketProcessor bpYear = new BucketProcessor(_rollingQuarterStart.get(4),_rollingQuarterStart.get(0),0,TemporalClass.YEAR);
        bpYear.start();
        BucketProcessor bpQuarter1 = new BucketProcessor(_rollingQuarterStart.get(1),_rollingQuarterStart.get(0),0,TemporalClass.QUARTER);
        bpQuarter1.start();
        BucketProcessor bpQuarter2 = new BucketProcessor(_rollingQuarterStart.get(2),_rollingQuarterStart.get(1),1,TemporalClass.QUARTER);
        bpQuarter2.start();
        BucketProcessor bpQuarter3 = new BucketProcessor(_rollingQuarterStart.get(3),_rollingQuarterStart.get(2),2,TemporalClass.QUARTER);
        bpQuarter3.start();
        BucketProcessor bpQuarter4 = new BucketProcessor(_rollingQuarterStart.get(4),_rollingQuarterStart.get(3),3,TemporalClass.QUARTER);
        bpQuarter4.start();
        try {
            bpMonth.join();
            bpYear.join();
            bpQuarter1.join();
            bpQuarter2.join();
            bpQuarter3.join();
            bpQuarter4.join();
        } catch (java.lang.InterruptedException e) { LogUtility.error(e); }
        t.print();
    }

    private SQLTable _getProviders() {
        String query =
                "select distinct \n"
                + "       stf.sta3n AS providerSta3n, \n"
                + "       stf.StaffSID AS providerSID, \n"
                + "       count(*) AS VisitCount \n"
                + "from VDWWork.Outpat.VProvider prv join \n"
                + "  VDWWork.Outpat.Visit vs on \n"
                + "    vs.sta3n = prv.sta3n and \n"
                + "    vs.visitSID = prv.visitSID and \n"
                + "    vs.patientSID = prv.patientSID \n"
                + "  join VDWWork.SStaff.SStaff stf on \n"
                + "    stf.sta3n = prv.sta3n and \n"
                + "    stf.staffSID = prv.providerSID \n"
                + "where vs.Sta3n = 640 and \n"
                + "      vs.primaryStopCode >= 500 and \n"
                + "      vs.primaryStopCode < 600 \n"
                + "   and vs.visitDateTime >=" + DBC.fixTimestamp(_rollingQuarterStart.get(4)) + " \n"
                + "group by stf.sta3n,stf.staffSID \n"
                + "having count(*) > 0 \n"
                + "order by stf.sta3n,stf.staffSID";
        return _dest.getTable(query);
    }

    private SQLTable _getProviders(java.util.List list) {
        String query =
                "SELECT DISTINCT \n"
                + "	sta3n AS providerSta3n \n"
                + "	,staffSID AS providerSID \n"
                + "FROM Commend.dbo.CommendVISNProvider \n"
                + "WHERE staffSID in " + DBC.javaListToSQLList(list);
        return _dest.getTable(query);
    }

    private java.util.Map<String,SQLTable> _getGroups() {
        String query =
                "SELECT DISTINCT \n"
                + "     wgID \n"
                + "FROM Commend.dbo.CommendVISNWorkGroupMembers \n"
                + "WHERE wgID != '-1' \n"
                + "ORDER BY wgID";
        SQLTable wgIds = _dest.getTable(query);
        java.util.Map<String,SQLTable> groups = new java.util.HashMap();
        String wgId = null;
        for(Integer i : wgIds.keySet()) {
            java.util.LinkedHashMap<String,Object> map = wgIds.getRow(i);
            wgId = "" + map.get("wgID");
            groups.put(wgId, _getGroup(wgId));
        }
        return groups;
    }

    private SQLTable _getGroup(String wgId) {
        String query =
                "SELECT DISTINCT \n"
                + "	providerSta3n \n"
                + "	,providerDUZ \n"
                + "	,providerSID \n"
                + "	,providerName \n"
                + "FROM Commend.dbo.CommendVISNWorkGroupMembers \n"
                + "WHERE wgID = " + com.medcisive.utility.sql2.DBC.fixString(wgId);
        return _dest.getTable(query);
    }
    
    public void _cleanupFlatEncounters() {
        String update =
                "SELECT distinct * \n" +
                "INTO Commend.dbo.CommendVISNFlatEncountersTemp \n" +
                "FROM Commend.dbo.CommendVISNFlatEncounters";
        _dest.update(update);
        _dest.update("DELETE FROM Commend.dbo.CommendVISNFlatEncounters");
        update = "INSERT INTO Commend.dbo.CommendVISNFlatEncounters \n" +
                "SELECT * FROM Commend.dbo.CommendVISNFlatEncountersTemp";
        _dest.update(update);
        update = "DROP TABLE Commend.dbo.CommendVISNFlatEncountersTemp";
        _dest.update(update);
    }

    private class BucketProcessor extends Thread {
        private com.medcisive.utility.sql2.DBC _sqlBP = null;
        private java.sql.Timestamp _start;
        private java.sql.Timestamp _end;
        private int _offset;
        private TemporalClass _temporalClass;
        private int maxInsertCount = 500;
        public final java.util.List<GroupStatisticsBuilder> GSBList = new java.util.ArrayList();

        public BucketProcessor(java.sql.Timestamp start, java.sql.Timestamp end, int offset, TemporalClass temporalClass) {
            _sqlBP = _dest.clone();
            _start = new java.sql.Timestamp(start.getTime()+100);
            _end = new java.sql.Timestamp(end.getTime()-100);
            _offset = offset;
            _temporalClass = temporalClass;
        }

        @Override
        public void run() {
            _processBucket();
        }

        private void _processBucket() {
            Timer t = Timer.start();
            //java.lang.StringBuilder result = new java.lang.StringBuilder();
            _processProviderBucket(_start,_end,_offset,_temporalClass);
            if(_groups!=null) {
                _processGroupBucket(_start,_end,_offset,_temporalClass);
            }
            //_sqlBP.update(result.toString());
            t.print();
        }

        private java.lang.StringBuilder _processProviderBucket(java.sql.Timestamp start, java.sql.Timestamp end, int offset, TemporalClass temporalClass) {
            Timer t = Timer.start();
            //java.util.ArrayList<EncounterProcessingBucket> list = new java.util.ArrayList();
            java.lang.StringBuilder result = new java.lang.StringBuilder();
            String sta3n = null;
            int insertCount = 0;
            for(Integer i : _providers.keySet()) {
                java.util.LinkedHashMap<String,Object> map = _providers.getRow(i);
                String sid = "" + map.get("providerSID");
                if(sta3n==null) {
                    sta3n = "" + map.get("providerSta3n");
                }
                EncounterProcessingBucket pe = new EncounterProcessingBucket(sid,sta3n,temporalClass,start,end);
                pe.startProcessingEncounterBucket();
                //list.add(pe);
                if(insertCount>maxInsertCount) {
                    insertCount = 0;
                    _sqlBP.update(result.toString());
                    result.setLength(0);
                }
                result.append(pe.insertStatment(offset, temporalClass.ordinal()));
                insertCount++;
            }
            
            //for(EncounterProcessingBucket peb: list) {    
            //}
            
            t.print();
            if(result.length()>0) {
                _sqlBP.update(result.toString());
            }
            return result;
        }

        private java.lang.StringBuilder _processGroupBucket(java.sql.Timestamp start, java.sql.Timestamp end, int offset, TemporalClass temporalClass) {
            Timer t = Timer.start();
            //java.util.List<EncounterProcessingBucket> list = new java.util.ArrayList();
            java.lang.StringBuilder result = new java.lang.StringBuilder();
            java.util.Set<String> providerSet;
            String sta3n;
            int insertCount = 0;
            for(String wgId : _groups.keySet()) {
                SQLTable tm = _groups.get(wgId);
                sta3n = "" + tm.getRow(0).get("providerSta3n");
                providerSet = new java.util.HashSet(tm.getColumn("providerSID"));
                EncounterProcessingBucket pe = new EncounterProcessingBucket(providerSet,sta3n,temporalClass,start,end);
                pe.setWgId(wgId);
                pe.startProcessingEncounterBucket();
                //list.add(pe);
                if(insertCount>maxInsertCount) {
                    insertCount = 0;
                    _sqlBP.update(result.toString());
                    result.setLength(0);
                }
                result.append(pe.insertStatment(offset, temporalClass.ordinal()));
                insertCount++;
                
                GroupStatisticsBuilder gsb = new GroupStatisticsBuilder();
                gsb.setup(Integer.parseInt(wgId), start, end, offset, temporalClass.ordinal());
                GSBList.add(gsb);
            }
            
            //for(EncounterProcessingBucket peb: list) {   
            //}
            
            t.print();
            if(result.length()>0) {
                _sqlBP.update(result.toString());
            }
            return result;
        }
    }
}
