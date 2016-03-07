package com.medcisive.commend.database.mdws;

import com.medcisive.commend.database.BatchBuilderDatabaseController;
import com.medcisive.utility.LogUtility;

/**
 *
 * @author vhapalchambj
 */
public class NoteBuilderThreadManager extends Thread {

    private java.util.List<Integer> _mdwsDUZs;
    private int _sta3n;
    private java.util.List<java.util.List<java.util.Map<String, Object>>> _arrayOfPatientInfoMap;
    private java.util.List<MDWSThreadManager> _MDWSThreadManagerList = new java.util.ArrayList();
    private BatchBuilderDatabaseController _dbc = new BatchBuilderDatabaseController();
    private java.sql.Timestamp _buildStart;
    private java.sql.Timestamp _buildFinish;
    private long _duration;

    public NoteBuilderThreadManager(int sta3n, java.util.List<Integer> mdwsDUZs) {
        _mdwsDUZs = mdwsDUZs;
        _sta3n = sta3n;
        int numberOfMDWSThreads = _mdwsDUZs.size();
        _arrayOfPatientInfoMap = _splitPatientHash(numberOfMDWSThreads);
        _dbc.logEvent("Note threads have started.", 1000003);
        _buildStart = new java.sql.Timestamp(System.currentTimeMillis());
        for (int i = 0; i < numberOfMDWSThreads; i++) {
            _MDWSThreadManagerList.add(new MDWSThreadManager(
                    mdwsDUZs.get(i),
                    _arrayOfPatientInfoMap.get(i)));
        }
        boolean isReady = false;
        while (!isReady) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                LogUtility.error(ex);
            }
            boolean local = true;
            for (MDWSThreadManager t : _MDWSThreadManagerList) {
                if (!t.isReady()) {
                    local = false;
                    break;
                }
            }
            if (local) {
                isReady = true;
            }
        }
    }

    public void run() {
        for (int i = 0; i < _mdwsDUZs.size(); i++) {
            _MDWSThreadManagerList.get(i).start();
        }
        for (Thread t : _MDWSThreadManagerList) {
            try {
                t.join();
            } catch (InterruptedException e) {
                LogUtility.error(e);
                System.err.println("Exception in joining threads(LogThread): " + e);
            }
        }
        _dbc.logEvent("Note threads have ended.", 1000004);
        _dbc.logEvent("" + _MDWSThreadManagerList.get(0)._mnb.mdws.errorCount, 1000006);
        _buildFinish = new java.sql.Timestamp(System.currentTimeMillis());
        _duration = (_buildFinish.getTime() - _buildStart.getTime()) / 1000;
        _dbc.logEvent(String.format("%d:%02d:%02d", _duration / 3600, (_duration % 3600) / 60, (_duration % 60)), 1000005);
    }

    private java.util.List<java.util.List<java.util.Map<String, Object>>> _splitPatientHash(int numbOfSplits) {
        //_patients.put("548047523","7411704");
        java.util.List<java.util.Map<String, Object>> patients = _dbc.getPatients(_sta3n);
        _dbc.logEvent("" + patients.size(), 1000007);
        java.util.List<java.util.List<java.util.Map<String, Object>>> result = new java.util.ArrayList();
        for (int i = 0; i < numbOfSplits; i++) {
            result.add(new java.util.ArrayList());
        }
        for (int i = 0; i < patients.size(); i++) {
            int indexMod = i % numbOfSplits;
            result.get(indexMod).add(patients.get(i));
        }
        return result;
    }
}