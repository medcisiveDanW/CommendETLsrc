package com.medcisive.commend.database;

import com.medcisive.utility.NoteBuilder;
import com.medcisive.mdws.MDWSAdaptor;

/**
 *
 * @author vhapalchambj
 */
public class BuildNoteCollection {

    private java.util.List<NoteBuilder> _notes;
    private java.util.Map<String, String> _notesWithTitle;
    private java.util.Map<String, String> _ienWithTitle = new java.util.HashMap();
    private int _sta3n;
    private int _patientSID;
    private int _patientIEN;
    private String _SSN;
    private boolean isPrint = false;
    private long _msYear = 31557600000L;
    private java.sql.Timestamp _3yearsAgo = new java.sql.Timestamp(System.currentTimeMillis() - _msYear*3);
    private boolean _isCommendNote = true;
    private static int _totalNotes = 0;

    public BuildNoteCollection(java.util.Map<String,Object> info, MDWSAdaptor mdws) {
        _sta3n      = (Integer)info.get("Sta3n");
        _patientSID = (Integer)info.get("PatientSID");
        _patientIEN = (Integer)info.get("PatientIEN");
        _SSN        = (String)info.get("SSN");
        java.sql.Timestamp date = null;
        if(info.get("Date")!=null) {
            date = (java.sql.Timestamp)info.get("Date");
        } else { date = _3yearsAgo; }
        _notes = new java.util.ArrayList();
        String fromDateStr = mdws.fromDateStr;
        mdws.fromDateStr = mdws.formater.format(date).toString();
        mdws.selectPatient("" + _patientIEN);
        _notesWithTitle = mdws.getNotesWithTitle();
        mdws.fromDateStr = fromDateStr;
        java.util.ArrayList<String> noteList = new java.util.ArrayList();
        for (String s : _notesWithTitle.keySet()) {
            String title = _notesWithTitle.get(s);
            java.util.Map<String, String> map = mdws.getNoteTitles(title);
            _ienWithTitle.putAll(map);
            noteList.add(s);
        }
        java.util.ArrayList<NoteBuilder> removeList = new java.util.ArrayList();
        for (String currentNote : noteList) {
            NoteBuilder nb = new NoteBuilder(currentNote);
            _notes.add(nb);
            if ((nb.getNoteBlocks().isEmpty())) {
                removeList.add(nb);
            }
        }
        _totalNotes += noteList.size();
        System.out.println("Number of CPRS: " + noteList.size() + " Commend: " + (noteList.size() - removeList.size()) + " Total: " + _totalNotes + "\n");
    }

    public void saveNotes(BatchBuilderDatabaseController dbc) {
        for (NoteBuilder nb : _notes) {
            String title = _notesWithTitle.get(nb.getRawText());
            String ien = null;
            for (String s : _ienWithTitle.keySet()) {
                if (_ienWithTitle.get(s).contains(title)) {
                    ien = s;
                    break;
                }
            }
            boolean isCommendNote = false;
            if(!nb.getNoteBlocks().isEmpty()) {
                isCommendNote = true;
            }
            dbc.insertNotes(_sta3n, _patientSID, dbc.getProviderSIDByName(nb.getRawProviderName()), nb.encounterDate, nb.noteDate, nb.getRawText(), ien, title, isCommendNote);
        }
    }

    public void saveTreatmentModes(BatchBuilderDatabaseController dbc) {
        java.util.LinkedHashMap<String, String> contactHash = dbc.getContactTypesHash();
        java.util.LinkedHashMap<String, String> treatmentHash = dbc.getTreatmentModeHash();
        java.util.LinkedHashMap<String, String> serviceHash = dbc.getServiceModesHash();

        for (NoteBuilder nb : _notes) {
            String pm = nb.get("Therapy Mode" + nb.pathSeparator + "PRIMARY MODE");
            String sm = nb.get("Therapy Mode" + nb.pathSeparator + "SECONDARY MODE");
            if(pm!=null && pm.contains("(Other)")) {
                int index = pm.indexOf("(Other)");
                pm = pm.substring(0, index+7);
            }
            if(sm!=null && sm.contains("(Other)")) {
                int index = sm.indexOf("(Other)");
                sm = sm.substring(0, index+7);
            }
            String primaryMode = treatmentHash.get(pm);
            String secondaryMode = treatmentHash.get(sm);
            if(pm!=null && (primaryMode==null)) {
                primaryMode = treatmentHash.get("Unknown");
            }
            if(sm!=null && (sm==null)) {
                secondaryMode = treatmentHash.get("Unknown");
            }

            String duration = nb.get("Therapy Mode" + nb.pathSeparator + "Duration");
            duration = removeMinutes(duration);
            String serviceModality = serviceHash.get(nb.get("Therapy Mode" + nb.pathSeparator + "Service Modality"));
            String contactType = contactHash.get(nb.get("Therapy Mode" + nb.pathSeparator + "Contact Type"));

            if(isPrint) {
                System.out.println("Note Info: " + nb.getNoteInfo());
                System.out.println("    PM: " + pm + "/" + primaryMode + " SM: " + sm + "/" + secondaryMode);
                System.out.println("    primaryMode: " + primaryMode + " secondaryMode: " + secondaryMode + " duration: " + duration + " serviceModality: " + serviceModality + " contactType: " + contactType);
            }

            if ((primaryMode!=null) && (duration!=null) ) {
                dbc.insertCommendTreatmentMode(_SSN, nb.getDate(), primaryMode, secondaryMode, duration, serviceModality, contactType);
            }
        }
    }

    public void saveOutcomes(BatchBuilderDatabaseController dbc) {
        java.util.LinkedHashMap<String, String> outcomeHash = dbc.getOutcomeHashToLower();
        for (NoteBuilder nb : _notes) { // strange lower case to simplify database compare
            String path = "Symptom/Functioning Scale";
            String key = "PHQ-9";
            String value = nb.get(path + nb.pathSeparator + key);
            if (value != null) {
                dbc.insertCommendOutcome(outcomeHash.get(key.toLowerCase()), _SSN, nb.getDate(), value, _isCommendNote);
            }
            key = "PCL-C";
            value = nb.get(path + nb.pathSeparator + key);
            if (value != null) {
                dbc.insertCommendOutcome(outcomeHash.get(key.toLowerCase()), _SSN, nb.getDate(), value, _isCommendNote);
            }
            key = "BAM";
            value = nb.get(path + nb.pathSeparator + key);
            if (value != null) {
                dbc.insertCommendOutcome(outcomeHash.get(key.toLowerCase()), _SSN, nb.getDate(), value, _isCommendNote);
            }
            key = "BPRS";
            value = nb.get(path + nb.pathSeparator + key);
            if (value != null) {
                dbc.insertCommendOutcome(outcomeHash.get(key.toLowerCase()), _SSN, nb.getDate(), value, _isCommendNote);
            }
            key = "BDI2";
            value = nb.get(path + nb.pathSeparator + key);
            if (value != null) {
                dbc.insertCommendOutcome(outcomeHash.get(key.toLowerCase()), _SSN, nb.getDate(), value, _isCommendNote);
            }
            key = "AUDC";
            value = nb.get(path + nb.pathSeparator + key);
            if (value != null) {
                dbc.insertCommendOutcome(outcomeHash.get(key.toLowerCase()), _SSN, nb.getDate(), value, _isCommendNote);
            }
        }
    }

    public void saveGoalTracking(BatchBuilderDatabaseController dbc) {
        java.util.LinkedHashMap<String, String> outcomeHash = dbc.getOutcomeHashToLower();
        for (NoteBuilder nb : _notes) {
            java.util.ArrayList list = (java.util.ArrayList) nb.parseDelimited(nb.get("Goal Tracking Measures"));
            if (list != null) {
                for (Object o : list) {
                    String key = (String) o;
                    String value = nb.parseSingleDigitNumber(nb.get("Goal Tracking Measures" + nb.pathSeparator + (String) o));
                    String id = outcomeHash.get(key.toLowerCase());
                    if ((value != null) && (id != null)) {
                        dbc.insertCommendOutcome(id, _SSN, nb.getDate(), value, _isCommendNote);
                    }
                }
            }
        }
    }

    public void saveSideEffects(BatchBuilderDatabaseController dbc) {
        java.util.LinkedHashMap<String, String> sideEffectsHash = dbc.getSideEffectsHash();
        for (NoteBuilder nb : _notes) {
            java.util.ArrayList<String> list = (java.util.ArrayList) nb.parseDelimited(nb.get("Side Effects" + nb.pathSeparator + "Ordered By Severity"));
            String frequency = nb.parseSingleDigitNumber(nb.get("Side Effects" + nb.pathSeparator + "Frequency"));
            if ((list != null) && (sideEffectsHash != null)) {
                String idList = "";
                for (String s : list) {
                    String comment = nb.sideEffectComment(s);
                    if ((comment != null) && !comment.equalsIgnoreCase("")) {
                        s = s.replace(comment, "");
                        s = nb.removeTrailingWhiteSpace(s);
                    }
                    idList += sideEffectsHash.get(s) + comment + ",";
                }
                if (!idList.isEmpty()) {
                    int lastComma = idList.lastIndexOf(',');
                    idList = idList.substring(0, lastComma);
                    dbc.insertCommendSideEffects(_SSN, nb.getDate(), idList, frequency);
                }
            }
        }
    }

    public void saveCustomGoals(BatchBuilderDatabaseController dbc) {
        for (NoteBuilder nb : _notes) {
            java.util.ArrayList list = (java.util.ArrayList) nb.parseDelimited(nb.get("Custom Goals"));
            if (list != null) {
                for (Object o : list) {
                    String value = nb.parseFirstNumber(nb.get("Custom Goals" + nb.pathSeparator + (String) o));
                    String id = nb.parseFirstNumber((String) o);
                    if(isPrint) { System.out.println("saveCustomGoals: " + id + " value: " + value); }
                    if ((value != null) && (id != null)) {
                        dbc.insertCommendOutcome(id, _SSN, nb.getDate(), value, _isCommendNote);
                    }
                }
            }
        }
    }

    private String pullIdFromCustom(String str) {
        String id = null;
        int start = str.indexOf('(');
        if (start > -1) {
            int end = str.indexOf(')');
            if (end > -1) {
                id = str.substring(start, end);
            }
        }
        return id;
    }

    public void printNotes() {
        for (NoteBuilder nb : _notes) {
            System.out.println(nb.getRawText());
            System.out.println("block data +++++++++++++++");
            //nb.print();
            //System.out.println(nb.getTreeStr());
        }
    }

    private String removeMinutes(String str) {
        if(str==null) { return null; }
        String result = str.toLowerCase();
        if(result.contains("minutes")) {
            int index = result.indexOf("minutes");
            result = result.substring(0,index);
        }
        result = result.trim();
        return result;
    }
}
