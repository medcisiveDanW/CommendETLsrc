package com.medcisive.commend.database.mumps;

import com.medcisive.commend.database.BatchBuilderDatabaseController;
import com.medcisive.utility.LogUtility;
import java.io.FileNotFoundException;

/**
 *
 * @author vhapalchambj
 */
public class MumpsParser extends Thread {
    private BatchBuilderDatabaseController                      _dbc = new BatchBuilderDatabaseController();
    private String[]                                            _filenameList = null;
    private String                                              _selectedFileName;
    private String                                              _directory;
    private java.util.LinkedHashMap<java.sql.Timestamp,String>  _fileNameHash = new java.util.LinkedHashMap();
    private java.util.ArrayList<MentalHealthTest>               _assessmentList = new java.util.ArrayList();
    private java.sql.Timestamp                                  _getMostResentFileDate = null;
    private AIMSScorer                                          _aims = new AIMSScorer();
    private AUDCScorer                                          _audc = new AUDCScorer();
    private AUDITScorer                                         _audit= new AUDITScorer();
    private BAIScorer                                           _bai  = new BAIScorer();
    private BPRSScorer                                          _bprs = new BPRSScorer();
    private PCLCScorer                                          _pclc = new PCLCScorer();
    private PCLMScorer                                          _pclm = new PCLMScorer();
    private PHQ9Scorer                                          _phq9 = new PHQ9Scorer();

    public MumpsParser() {}

    @Override
    public void run() {
        java.util.LinkedHashMap<String, String> outcomeHash = _dbc.getOutcomeHash();
        if (_assessmentList != null && !_assessmentList.isEmpty()) {
            MentalHealthTest mht = null;
            for (int i = 0; i < _assessmentList.size(); i++) {
                mht = _assessmentList.get(i);
                String score = Integer.toString(mht.score);
                _dbc.insertCommendOutcome(outcomeHash.get(mht.commendName), mht.SSN, mht.date, score, false);
                if (mht.m_saveComponentAnswersSQL != null) {
                    _dbc.insertAssessmentComponentAnswers(mht.m_saveComponentAnswersSQL);
                }
            }
        }
    }

    public MumpsParser source(String directory) {
        this._directory = directory;
        java.io.FileInputStream fis = null;
        try {
            setupFileHash();
            _getMostResentFileDate = getMostResentKey();
            _selectedFileName = _fileNameHash.get(_getMostResentFileDate);

        } catch(NullPointerException e) { LogUtility.error(e); System.out.println("Error: " + e + " probaly incorrect mumps DIR path"); }
        try {
            fis = new java.io.FileInputStream(directory + "\\" + _selectedFileName);
        } catch (java.io.FileNotFoundException e) {LogUtility.error(e);}
        java.io.DataInputStream in = new java.io.DataInputStream(fis);
        source(new java.io.InputStreamReader(in));
        return this;
    }

    public MumpsParser source(java.io.Reader r) {
        scoreMentalHealthTest(r);
        return this;
    }

    public void print() {
        if(_fileNameHash.isEmpty()){
            System.out.println("Specified directory does not exist or is not a directory.");
        }else{
            for(java.sql.Timestamp key : _fileNameHash.keySet()) {
                System.out.println(key + " : " + _fileNameHash.get(key));
            }
        }
    }
    private void setupFileHash() {
        java.text.SimpleDateFormat sdf   = new java.text.SimpleDateFormat("yyMMdd");
        java.io.File dir = new java.io.File(_directory);
        _filenameList = dir.list();
        String fileName;
        String dateString;
        if(_filenameList!=null){
            for(int i = 0; i < _filenameList.length; i++){
                fileName = _filenameList[i];
                try {
                    int index = fileName.indexOf('.');
                    dateString = fileName.substring(0, index);
                    index = dateString.indexOf("mh_");
                    dateString = dateString.substring(index+3, dateString.length());
                    _fileNameHash.put( new java.sql.Timestamp(sdf.parse(dateString).getTime()), fileName);
                }
                catch (Exception e) {LogUtility.error(e);}
            }
        }
    }
    private java.sql.Timestamp getMostResentKey() {
        long curr = 0;
        java.sql.Timestamp returnKey = new java.sql.Timestamp(0);
        if(!_fileNameHash.isEmpty()){
            for(java.sql.Timestamp key : _fileNameHash.keySet()) {
                if(curr < key.getTime()) {
                    curr = key.getTime();
                    returnKey = key;
                }
            }
        }
        return returnKey;
    }

    private void scoreMentalHealthTest(java.io.Reader r) {
        int count = 0;
        try {
            java.io.BufferedReader br = new java.io.BufferedReader(r);//new java.io.InputStreamReader(in));
            String strLine;
            while ((strLine = br.readLine()) != null) {
                if(!strLine.equalsIgnoreCase("")) {
                    MentalHealthTest mht = new MentalHealthTest(strLine);
                    if(mht.instrumentName.equalsIgnoreCase("AIMS")) {
                        mht.score = _aims.getScore(mht.questionChoiceIdHash);
                        mht.commendName = _aims.getName();
                    }
                    else if(mht.instrumentName.equalsIgnoreCase("AUDC")) {
                        mht.score = _audc.getScore(mht.questionChoiceIdHash);
                        mht.commendName = _audc.getName();
                    }
                    else if(mht.instrumentName.equalsIgnoreCase("AUDIT")) {
                        mht.score = _audit.getScore(mht.questionChoiceIdHash);
                        mht.commendName = _audit.getName();
                    }
                    else if(mht.instrumentName.equalsIgnoreCase("BAI")) {
                        mht.score = _bai.getScore(mht.questionChoiceIdHash);
                        mht.commendName = _bai.getName();
                    }
                    else if(mht.instrumentName.equalsIgnoreCase("BPRS")) {
                        mht.score = _bprs.getScore(mht.questionChoiceIdHash);
                        mht.commendName = _bprs.getName();
                    }
                    else if(mht.instrumentName.equalsIgnoreCase("PCLC")) {
                        mht.score = _pclc.getScore(mht.questionChoiceIdHash);
                        //mht.enableSavingComponentAnswers( _pclc.getScoreMap());
                        mht.commendName = _pclc.getName();
                    }
                    else if(mht.instrumentName.equalsIgnoreCase("PCLM")) {
                        mht.score = _pclm.getScore(mht.questionChoiceIdHash);
                        mht.commendName = _pclm.getName();
                    }
                    else if(mht.instrumentName.equalsIgnoreCase("PHQ9")) {
                        mht.score = _phq9.getScore(mht.questionChoiceIdHash);
                        mht.commendName = _phq9.getName();
                    }
                    else {
                        mht = null;
                    }
                    if(mht!=null) {
                        //System.out.println("Name: " + mht.patientName + " SSN: " + mht.SSN + " Date: " + mht.dateGiven + " Test: " + mht.instrumentName + " score: " + mht.score);
                        _assessmentList.add(mht);
                        count++;
                    }
                }
            }
            System.out.println("Total assessments: " + count);
            r.close();
        } catch (Exception e) { LogUtility.error(e); System.err.println("Error: " + e.getMessage()); }

    }
}
