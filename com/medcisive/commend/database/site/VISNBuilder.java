package com.medcisive.commend.database.site;

import com.medcisive.utility.LogUtility;
import com.medcisive.utility.Timer;

/**
 * Encapsulated partition used to build COMMEND SITE tables. This can be created
 * in sequence or in parallel to COMMEND's base SQL database builder.
 * @author vhapalchambj
 */
public class VISNBuilder {

    private SiteDatabaseController _sdc = null;
    private int _sta3n;

    /**
     * Constructor used to setup the connection to the database and initializing
     * associated components.<br>
     * Note: After integrating with the main COMMEND build, the setup of SQLUtility
     * will no longer be necessary.
     * @param sta3n Which site to process.
     */
    public VISNBuilder(int sta3n) {
        System.out.println("Starting VISN builder.");
        _sta3n = sta3n;
        _sdc = new SiteDatabaseController(_sta3n);
    }

    /**
     * Initiates the build order for the COMMEND site database tables.
     */
    public void buildDatabase() {
        _sdc.logEvent("VISN Database Builder Started.", 1000021);
        Timer t = Timer.start();
        
        _sdc.setupPatientProviderTables();
        Thread proEncThread = _sdc.insertCommendVISNPrvdrEncounters();
        Thread diagThread = _sdc.insertDiagnosis();
        Thread proThread = _sdc.insertProcedure();
        try {
            proEncThread.join();
            diagThread.join();
            proThread.join();
        } catch (java.lang.Exception e) {LogUtility.error(e);}
        
        Thread visnApptSummaryBuilder = VISNApptSummaryBuilder.createThread(_sta3n);
        Thread encounterStatisticsBuilder = EncounterStatisticsBuilder.createThread();
        Thread visnEncounterFlattener = VISNEncounterFlattener.createThread(_sta3n);
        Thread ptsdTherapyAnalysis = PTSDThearapyAnalysis.createThread(_sta3n);
//      Do not uncomment ***
//        Thread oef4StatisticsBuilder = OEF4StatisticsBuilder.createThread(_sta3n);
//        oef4StatisticsBuilder.start();
//        PTSDThearapyAnalysis _processVISNEncounterAnalysis(_sta3n);
//        PTSDThearapyAnalysis _processGraphData(); // do not enable.  OEF4 graphing system.
//        PTSDThearapyAnalysis _processInstitutionTable(); // do not enable.  OEF4 window system.
//      Do not uncomment ***
        try {
            visnApptSummaryBuilder.join();
            encounterStatisticsBuilder.join();
            visnEncounterFlattener.join();
            ptsdTherapyAnalysis.join();
            BHIPBuild.createThread(_sta3n).join();
//            oef4StatisticsBuilder.join();
        } catch (InterruptedException ex) { LogUtility.error(ex); }

        t.print();
        _sdc.logEvent("VISN Database Builder Finished in (" + t.getDuration() + ") seconds.", 1000022);
    }
}