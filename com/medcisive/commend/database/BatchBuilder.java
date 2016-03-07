package com.medcisive.commend.database;

import com.google.gson.reflect.TypeToken;
import com.medcisive.commend.database.mdws.*;
import com.medcisive.commend.database.encounter.EncountersAnalyzer;
import com.medcisive.commend.database.mumps.*;
import java.util.ArrayList;
import com.medcisive.utility.LogUtility;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
/**
 *
 * @author vhapalchambj
 */
public class BatchBuilder {

    private BatchBuilderDatabaseController _dbc;
    private NoteBuilderThreadManager _nbThreadManager;
    private java.util.List<Integer> _duzList;
    private java.sql.Timestamp _buildStart;
    private java.sql.Timestamp _buildFinish;
    private long _duration;
    private boolean _completeRebuild = false;

    public BatchBuilder() {
        _dbc = new BatchBuilderDatabaseController();
        _dbc.logEvent("Database Builder Started.", 1000000);
        _buildStart = new java.sql.Timestamp(System.currentTimeMillis());
        _duzList = new java.util.ArrayList();
        String DUZ_LIST =  _dbc._properties.getProperty("DUZ_LIST");
        _completeRebuild = Boolean.parseBoolean(_dbc._properties.getProperty("COMPLETE_REBUILD"));
        if(DUZ_LIST != null) {
            com.google.gson.Gson g = new com.google.gson.Gson();
            Type type = new TypeToken<java.util.List<Integer>>(){}.getType();
            _duzList = g.fromJson(DUZ_LIST, type);
        } else {
            System.exit(1);
        }
        System.out.println("Numbers of MDWS Threads: " + _duzList.size());
    }

    public void buildDatabase(int sta3n) {
        _dbc.clearDatabase(sta3n,_completeRebuild);
        _dbc.insertCommendDemographics(sta3n);
        _dbc.insertCommendDemographicsTESTING(sta3n);
        _dbc.insertCommendLastNote(sta3n);
        _dbc.insertCommendEncounters(sta3n);
        _dbc.insertCommendEncountersTESTING(sta3n);

        _nbThreadManager = new NoteBuilderThreadManager(sta3n,_duzList);
        _nbThreadManager.start();

        System.out.println("*Starting MUMPS extraction...");
        String file = new MUMPSFileBuilder().getFile(sta3n);
        InputStream bais = new ByteArrayInputStream(file.getBytes());
        new MumpsParser().source(new InputStreamReader(bais)).start();

        _dbc.insertCommendPrescriptionsRaw(sta3n);
        _dbc.insertCommendPrescriptions(sta3n);
        _dbc.insertCommendMedicationManagementVAonly(sta3n);
        _dbc.startTimmer();
        ArrayList encounters = _dbc.getCommendEncounters();
        EncountersAnalyzer encAnalyzer = new EncountersAnalyzer(encounters);
        ArrayList encSummaries = encAnalyzer.performAnalysis();
        _dbc.saveEncSummaries(encSummaries);
        _dbc.logTimmer(1000017);

        try{ _nbThreadManager.join(); } catch(Exception e) { LogUtility.error(e); }
        _dbc.insertCommendPatientSummary();
        _dbc.logEvent("Database Builder Finished.", 1000001);
        _buildFinish = new java.sql.Timestamp(System.currentTimeMillis());
        _duration = _buildFinish.getTime() - _buildStart.getTime();
        _dbc.logEvent(_dbc.durationToString(_duration), 1000002);
    }
}
