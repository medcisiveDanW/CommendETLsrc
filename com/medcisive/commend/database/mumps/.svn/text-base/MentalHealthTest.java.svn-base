package com.medcisive.commend.database.mumps;

import com.medcisive.utility.LogUtility;
import java.util.*;
/**
 *
 * @author vhapalchambj
 */
public class MentalHealthTest {
    public String copse;
    public String stationId;
    public String patientId;
    public String patientName;
    public String SSN;
    public String administrationId;
    public String dateGiven;
    public java.sql.Timestamp date;
    public String instrumentName;
    public String commendName;
    public int score = -1;
    public String m_saveComponentAnswersSQL = null; // This is a SQL insert string 
       // which enables saving all the component scores for the particular assessment/test
       // of interest.  It is null if we do not wish to save the individual components.
    
    public java.util.LinkedHashMap<String,String> questionChoiceIdHash;
    
    // These are the outcomeID's of the PCLC components as defined in the 
    // database.  Note: the name PCLCNN corresponds to outcomeID 
    // (100+NN), e.g. PCLC15 has outcomeID 115.
    public static String[] m_PCLCOutcomeIDList =
    { "101", "102", "103", "104", "105",
      "106", "107", "108", "109", "110",
      "111", "112", "113", "114", "115",
      "116", "117" };
    
    
    public MentalHealthTest(String str) {
        m_saveComponentAnswersSQL = null;
        copse = str;
        questionChoiceIdHash = new java.util.LinkedHashMap();
        str = chopCarot(str);

        stationId = pullValue(str);
        str = chopCarot(str);

        patientId = pullValue(str);
        str = chopCarot(str);

        patientName = pullValue(str);
        str = chopCarot(str);

        SSN = pullValue(str);
        str = chopCarot(str);

        administrationId = pullValue(str);
        str = chopCarot(str);

        dateGiven = pullValue(str);
        str = chopCarot(str);

        instrumentName = pullValue(str);
        str = chopCarot(str);

        while(str.length()>0) {
            String key = pullValue(str);
            str = chopCarot(str);
            String value = pullValue(str);
            str = chopCarot(str);
            questionChoiceIdHash.put(key, value);
        }
        java.text.SimpleDateFormat sdf   = new java.text.SimpleDateFormat("MM/dd/yy");
        try {
            date = new java.sql.Timestamp(sdf.parse(dateGiven).getTime());
        }
        catch (Exception e) { LogUtility.error(e); System.out.println("Error parsing date given: " + dateGiven); }
    }
    
    // Here to create the SQL statement that allows saving all component answers.
    // the resultant SQL statement is found in m_saveComponentAnswersSQL.
    // The arguement scoreMap is for the particular assessment of choice
    public void enableSavingComponentAnswers(java.util.HashMap scoreMap)
    {
        m_saveComponentAnswersSQL = null;      
        if (scoreMap == null || scoreMap.size() <= 0 )
            return;
               
        StringBuffer tempSQL = new StringBuffer("insert into dbo.CommendOutcomes (outcomeID,SSN,date,value) ");
        String SSNandDate = ",'" + SSN + "','" + dateGiven + "',";
        String uAll = new String(" union all ");
        
        int counter = 0;
        for(String key : questionChoiceIdHash.keySet()) {
            String choice = questionChoiceIdHash.get(key);
            Integer choiceValue = (Integer) scoreMap.get(choice);
 
            tempSQL.append("select ").
                    append("'").append(m_PCLCOutcomeIDList[counter]).append("'").
                    append(SSNandDate).append(choiceValue.intValue());
            if (counter <16) {
               tempSQL.append(uAll);
            }                  
            counter++;
        }
        m_saveComponentAnswersSQL = tempSQL.toString();                     
        //System.out.println("m_saveComponentAnswersSQL: " + m_saveComponentAnswersSQL);
    }
    
    public void print() {        
        System.out.println("patientName: " + patientName + " SSN: " + SSN + " dateGiven: " + dateGiven + " instrumentName: " + instrumentName);
        for(String key : questionChoiceIdHash.keySet()) {
            System.out.println("  Key: " + key + " Value: " + questionChoiceIdHash.get(key));
        }
    }
    private String pullValue(String str) {
        int index = str.indexOf('^');
        if(index>-1) {
            str = str.substring(0 , index);
        }
        return str;
    }
    private String chopCarot(String str) {
        int index = str.indexOf('^');
        if(index>-1) {
            str = str.substring(index+1 , str.length());
        }
        else { str = ""; }
        return str;
    }
}
