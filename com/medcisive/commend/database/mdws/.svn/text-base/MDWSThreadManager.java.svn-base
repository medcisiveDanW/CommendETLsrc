package com.medcisive.commend.database.mdws;

/**
 *
 * @author vhapalchambj
 */
public class MDWSThreadManager extends Thread {
    public MDWSNoteBuilder _mnb;
    private java.util.List<java.util.Map<String, Object>> _patientMap;
    private Thread _startupThread;

    public MDWSThreadManager(final int providerDUZ, java.util.List<java.util.Map<String, Object>> patientMap) {
        _patientMap = patientMap;
        _startupThread = new Thread() {
            @Override
            public void run() {
                System.out.println("Starting MDWSNoteBuilder thread");
                _mnb = new MDWSNoteBuilder(providerDUZ);
            }
        };
        _startupThread.start();
    }

    public boolean isReady() {
        return !_startupThread.isAlive();
    }

    @Override
    public void run() {
        _mnb.buildNotes(_patientMap);
    }
}
