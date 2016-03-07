package com.medcisive.commend.database.util;

/**
 *
 * @author vhapalchambj
 */
public class CommendOutcomesDataObject
{
    public String               patientDFN;
    public String               outcomeID;
    public java.sql.Timestamp   date;
    public int                  value;

    public CommendOutcomesDataObject(String patientDFN, String outcomeID, java.sql.Timestamp date, int value)
    {
        this.patientDFN = patientDFN;
        this.outcomeID  = outcomeID;
        this.date       = date;
        this.value      = value;
    }

    public void print()
    {
        System.out.println("    Patient DFN: " + this.patientDFN);
        System.out.println("    outcome ID:  " + this.outcomeID);
        System.out.println("    date:        " + this.date);
        System.out.println("    value:       " + this.value);
    }
}
