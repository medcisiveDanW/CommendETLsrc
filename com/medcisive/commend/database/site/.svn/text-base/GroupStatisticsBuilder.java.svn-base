package com.medcisive.commend.database.site;

import com.medcisive.utility.sql2.DBC;
import com.medcisive.utility.sql2.SQLObject;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author vhapalchambj
 */
public class GroupStatisticsBuilder extends com.medcisive.utility.sql2.DBCUtil {
    private int _wgId;
    private java.sql.Timestamp _start;
    private java.sql.Timestamp _end;
    private int _offset;
    private int _temporalId;
    private boolean _isReady = false;

    public void process() {
        if(!_isReady) {
            return;
        }
        java.util.List groupStatistics = _getGroupStatistics(_wgId, _start, _end, _offset, _temporalId);
        String insert = _generateInserts(groupStatistics);
        if(!insert.isEmpty()) {
            _dest.update(insert);
        }
    }
    
    public void setup(int wgId,java.sql.Timestamp start, java.sql.Timestamp end, int offset, int id) {
        _wgId = wgId;
        _start = start;
        _end = end;
        _offset = offset;
        _temporalId = id;
        _isReady = true;
    }
    
    public void clear() {
        _dest.update("DELETE FROM Commend.dbo.CommendVISNGroupStatistics");
    }

    private String _generateInserts(java.util.List group) {
        StringBuilder result = new StringBuilder();
        for (int index = 0; index < group.size(); index++) {
            java.util.Map provider = (java.util.Map) group.get(index);
            result.append(_generateInsert(provider));
        }
        return result.toString();
    }

    private String _generateInsert(java.util.Map provider) {
        String result;
        String insert = "INSERT INTO Commend.dbo.CommendVISNGroupStatistics (Sta3n,ProviderSID,Name,WgID,TemporalOffset,TemporalID,NumberOfEncounters,UniquePatients,NumberOfSessions,MaxPatients,MedianPatients,ModePatients) \n";
        result = insert
                + "VALUES ("
                + provider.get("sta3n") + ","
                + provider.get("providerSID") + ","
                + DBC.fixString((String)provider.get("providername")) + ","
                + _wgId + ","
                + _offset + ","
                + _temporalId + ","
                + provider.get("encounters") + ","
                + provider.get("patients") + ","
                + provider.get("numberOfSessions") + ","
                + provider.get("maxPts") + ","
                + provider.get("medianPts") + ","
                + DBC.fixString((String)provider.get("modePts")) + ") \n";
        return result;
    }

    private java.util.List _getGroupStatistics(int wgId, java.sql.Timestamp start, java.sql.Timestamp end, int offset, int id) {
        final java.util.List<java.util.Map> result = new java.util.ArrayList();
        String query
                = "select st.sta3n, \n"
                + "     st.providerSID, \n"
                + "     st.providername, \n"
                + "     st.numInGroup, \n"
                + "     st.visitDateTime, \n"
                + "     pst.EncGro, \n"
                + "     pst.PatGro \n"
                + "from Commend.dbo.CommendVISNGroupTxStats st, \n"
                + "	 Commend.dbo.CommendVISNWorkGroupMembers wg, \n"
                + "	 Commend.dbo.CommendVISNProviderStatistics pst \n"
                + "where wg.providerSID = st.providerSID \n"
                + "  and wg.providerSta3n = st.sta3n \n"
                + "  and st.visitDateTime > " + DBC.fixTimestamp(start) + " \n"
                + "  and st.visitDateTime < " + DBC.fixTimestamp(end) + " \n"
                + "  and wg.wgID = " + wgId + " \n"
                + "  and pst.staffSID = wg.providerSID \n"
                + "  and pst.offset = " + offset + " \n"
                + "  and pst.offsetType = " + id + " \n"
                + "group by st.sta3n,st.providerSID,st.providerName,st.numInGroup,st.visitDateTime,pst.EncGro,pst.PatGro  \n"
                + "order by providerSID,numInGroup";
        _dest.query(query, new SQLObject() {

            @Override
            public void row(ResultSet rs) throws SQLException {
                result.add(_processProviderGroupStatistics(rs));
            }
        });
        return result;
    }

    private java.util.Map<String, Object> _processProviderGroupStatistics(ResultSet rs) throws SQLException {
        int sta3n = -1;
        int providerSID = -1;
        String providername = null;
        int encounters = 0;
        int patients = 0;
        com.google.common.collect.Multiset<Integer> _groupMembersSet = com.google.common.collect.HashMultiset.create();
        java.util.List<Integer> medianList = new java.util.ArrayList();
        int numberOfSessions = 0;
        int maxPts = 0;
        int maxNumberOfOccurance = 0;
        java.util.Map<Integer, java.util.Set<Integer>> modeMap = new java.util.HashMap();
        String modeSet = "";
        int medianPts = 0;
        java.util.Map<String, Object> result = new java.util.HashMap();
        do {
            numberOfSessions++;
            int currentProviderSID = rs.getInt("providerSID");
            int numInGroup = rs.getInt("numInGroup");
            if (providerSID == -1) {
                providerSID = currentProviderSID;
                sta3n = rs.getInt("sta3n");
                providername = rs.getString("providername");
                encounters = rs.getInt("EncGro");
                patients = rs.getInt("PatGro");
                _groupMembersSet.add(numInGroup);
                medianList.add(numInGroup);
            } else if (providerSID == currentProviderSID) {
                _groupMembersSet.add(numInGroup);
                medianList.add(numInGroup);
            } else {
                rs.previous();
                break;
            }
        } while (rs.next());

        for (Integer numberOfGroupMembers : _groupMembersSet) {
            int numberOfOccurance = _groupMembersSet.count(numberOfGroupMembers);
            if (numberOfGroupMembers > maxPts) {
                maxPts = numberOfGroupMembers;
            }
            if (numberOfOccurance > maxNumberOfOccurance) {
                maxNumberOfOccurance = numberOfOccurance;
            }
            if (!modeMap.containsKey(numberOfOccurance)) {
                java.util.Set s = new java.util.HashSet();
                s.add(numberOfGroupMembers);
                modeMap.put(numberOfOccurance, s);
            } else {
                java.util.Set s = modeMap.get(numberOfOccurance);
                s.add(numberOfGroupMembers);
            }
        }
        if (maxNumberOfOccurance > 1) {
            java.util.Set<Integer> currModeSet = modeMap.get(maxNumberOfOccurance);
            String marker = "";
            for (Integer i : currModeSet) {
                modeSet += marker + i;
                marker = ",";
            }
        }

        int middle = medianList.size() / 2;
        int[] medianArr = new int[medianList.size()];
        for (int i = 0; i < medianList.size(); i++) {
            medianArr[i] = medianList.get(i);
        }
        java.util.Arrays.sort(medianArr);

        if (middle > 0) {
            if (medianList.size() % 2 == 0) {
                medianPts = (medianArr[middle - 1] + medianArr[middle]) / 2;
            } else {
                medianPts = medianArr[middle];
            }
        }
        result.put("sta3n", sta3n);
        result.put("providerSID", providerSID);
        result.put("providername", providername);
        result.put("encounters", encounters);
        result.put("patients", patients);
        result.put("numberOfSessions", numberOfSessions);
        result.put("maxPts", maxPts);
        result.put("medianPts", medianPts);
        result.put("modePts", modeSet);
        return result;
    }
}
