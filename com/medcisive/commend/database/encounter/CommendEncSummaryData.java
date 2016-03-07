package com.medcisive.commend.database.encounter;

import java.sql.*;
/**
 *
 * @author vhapalchambj
 */
public class CommendEncSummaryData
{

    public String m_SSN;
    public Integer m_numTotAppts;          // number of total appointments
    public Integer m_numTotNoShows;        // number of NoShows or missed appointments
    public Timestamp m_nextAppt;
    public Timestamp m_lastAppt;

    public CommendEncSummaryData(String ssn, Integer numTotAppts, Integer numTotNoShows, 
                                 Timestamp nextAppt, Timestamp lastAppt) {
      m_SSN = ssn;
      m_numTotAppts = numTotAppts;
      m_numTotNoShows = numTotNoShows;
      m_nextAppt = nextAppt;
      m_lastAppt = lastAppt;
      
    }                               

}
