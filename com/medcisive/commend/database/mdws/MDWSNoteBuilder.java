package com.medcisive.commend.database.mdws;

import com.medcisive.commend.database.BatchBuilderDatabaseController;
import com.medcisive.commend.database.BuildNoteCollection;
import com.medcisive.mdws.MDWSAdaptor;

/**
 *
 * @author vhapalchambj
 */
public class MDWSNoteBuilder {

    private java.util.LinkedHashMap<String, BuildNoteCollection> _patients;
    private BatchBuilderDatabaseController _bbdc = new BatchBuilderDatabaseController();
    private java.util.Map<Integer,Integer> _patientDUZtoSIDMap;
    public MDWSAdaptor mdws;
    private boolean _showProcesses = true;
    public boolean isReady = false;
    private static float _patientCounter = 0;
    private static float _totalPatients = 0;

    MDWSNoteBuilder(int providerDUZ) {
        mdws = new MDWSAdaptor();
        mdws.providerDUZ = "" + providerDUZ;
        isReady = mdws.adInitium();
        _patients = new java.util.LinkedHashMap();
        _patientDUZtoSIDMap = _bbdc.getPatientIENtoSIDMap();
    }

    public void buildNotes(java.util.List<java.util.Map<String,Object>> patientList) {
        if (isReady) {
            _totalPatients += (float)patientList.size();
            java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
            nf.setMaximumFractionDigits(2);
            nf.setMinimumFractionDigits(2);
            for (java.util.Map<String, Object> patientInfoMap : patientList) {
                BuildNoteCollection nbc = new BuildNoteCollection(patientInfoMap,mdws);
                processNonVAMeds((Integer)patientInfoMap.get("PatientIEN"));
                _bbdc.insertCommendNoteEncounters(patientInfoMap, mdws.getEncounters());
                saveNote(nbc,_bbdc);
                _patientCounter++;
                float precentageComplete = (_patientCounter / _totalPatients) * 100;
                if (_showProcesses) {
                    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ " + nf.format(precentageComplete) + "% complete ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
                }
            }
            if (_showProcesses) {
                System.out.println("####################### FINISHED BUILDING NOTE SET ############################");
            }
            //saveNotes();
        }
    }

    private void processNonVAMeds(int dfn) {
        java.util.ArrayList<java.util.HashMap<String, Object>> medList = mdws.getNonVAMeds();
        //System.out.println("*Insert Commend Medication Management (Raw Non-VA only): " + medList.size());
        for (java.util.HashMap<String, Object> map : medList) {
            String LocalDrugNameWithDose = (String) map.get("LocalDrugNameWithDose");
            String Sig = (String) map.get("Sig");
            String RxStatus = (String) map.get("RxStatus");
            java.sql.Timestamp VistaCreateDate = (java.sql.Timestamp) map.get("VistaCreateDate");
            int isVAMed = (Integer) map.get("isVAMed");
            String sta3nStr = (String) map.get("Sta3n");
            int sta3n = Integer.parseInt(sta3nStr);
            int sid = _patientDUZtoSIDMap.get(dfn);
            if(sta3n>0 && sid>0){
                _bbdc.insertCommendMedicationManagementNonVA(sta3n, sid, LocalDrugNameWithDose, Sig, RxStatus, VistaCreateDate, isVAMed);
            }
        }
    }

    private void saveNote(BuildNoteCollection bnc, BatchBuilderDatabaseController dbc) {
        bnc.saveNotes(dbc);
        bnc.saveTreatmentModes(dbc);
        bnc.saveOutcomes(dbc);
        bnc.saveGoalTracking(dbc);
        bnc.saveSideEffects(dbc);
        bnc.saveCustomGoals(dbc);
    }

    private void saveNotes() {
        BatchBuilderDatabaseController bbdc = new BatchBuilderDatabaseController();
        for (Object o : _patients.keySet()) {
            BuildNoteCollection bnc = _patients.get((String) o);
            bnc.saveNotes(bbdc);
            bnc.saveTreatmentModes(bbdc);
            bnc.saveOutcomes(bbdc);
            bnc.saveGoalTracking(bbdc);
            bnc.saveSideEffects(bbdc);
            bnc.saveCustomGoals(bbdc);
        }
    }

    public void print() {
        for (Object o : _patients.keySet()) {
            _patients.get((String) o).printNotes();
        }
    }
}
