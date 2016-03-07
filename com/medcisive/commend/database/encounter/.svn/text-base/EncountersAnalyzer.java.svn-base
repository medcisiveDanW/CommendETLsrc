package com.medcisive.commend.database.encounter;

import java.util.*;
import java.sql.Timestamp;

/**
 *
 * @author vhapalwangd
 */
public class EncountersAnalyzer {

    private ArrayList<CommendEncounterData> m_encounters;
    private static String CLINIC_CANCELLED_STATUS = "C";
    private static String PATIENT_NO_SHOW_STATUS = "N";
    private static int YEARS_TO_EXAMINE = 3;
    private static long EarliestAppointmentTime = 0;
    private static long TodayTime = 0;
    private static long yearInMillisec = (long) 365 * (long) 24 * (long) 3600000;  // 31536000000=365 * 24 * 60 * 60 * 1000;

    static {
        Calendar today = Calendar.getInstance();
        TodayTime = today.getTimeInMillis();
        EarliestAppointmentTime = TodayTime - (YEARS_TO_EXAMINE * yearInMillisec);
    }

    public EncountersAnalyzer(ArrayList encounters) {
        m_encounters = encounters;
    }

    //
    // The analysis assumes that m_encounters is ordered first
    // by SSN, then by apptDateTime in desc order
    // If that is not true, the analysis will not be accurate
    //
    public ArrayList<CommendEncSummaryData> performAnalysis() {
        ArrayList<CommendEncSummaryData> encSums = null;
        if (m_encounters == null || m_encounters.size() <= 0) {
            return null;
        }
        encSums = new ArrayList();
        ArrayList<CommendEncounterData> onePtEncounters = new ArrayList();
        String ssnNow = null;
        String ssnTemp = null;
        for (int j = 0; j < m_encounters.size(); j++) {
            CommendEncounterData encData = (CommendEncounterData) m_encounters.get(j);
            ssnTemp = encData.m_SSN;
            if (!ssnTemp.equals(ssnNow)) {
                if (onePtEncounters != null && onePtEncounters.size() > 0) {
                    CommendEncSummaryData anEncSummary = computeSummaryForOnePatient(onePtEncounters);
                    encSums.add(anEncSummary);
                }
                onePtEncounters.clear();
                onePtEncounters.add(encData);
                ssnNow = ssnTemp;
            } else {
                onePtEncounters.add(encData);
            }
        }
        if (onePtEncounters != null && onePtEncounters.size() > 0) {
            CommendEncSummaryData anEncSummary = computeSummaryForOnePatient(onePtEncounters);
            encSums.add(anEncSummary);
        }
        return encSums;
    }

    /*
     * Given the input, which is an ArrayList of CommendEncounterData instances
     * all having the same SSN, we analyze the data to obtain the
     * CommendEncSummaryData instance for that SSN
     * It is also assumed that the apptDateTimes in encounters is
     * ordered in desc order from future to past times
     *
     */
    private CommendEncSummaryData computeSummaryForOnePatient(ArrayList encounters) {
        CommendEncSummaryData resEncSummary = null;
        if (encounters == null || encounters.size() <= 0) {
            return null;
        }
        String ssn = null;
        String cancelNoShow = null;
        Timestamp tsTemp = null;
        Timestamp tsNextAppt = null;
        Timestamp tsLastAppt = null;
        long timeNextAppt = TodayTime + (YEARS_TO_EXAMINE * yearInMillisec);
        int countTotAppts = 0;
        int countNoShows = 0;
        int numberEncounters = encounters.size();

        for (int j = 0; j < numberEncounters; j++) {
            CommendEncounterData anEnc = (CommendEncounterData) encounters.get(j);
            ssn = anEnc.m_SSN;
            cancelNoShow = anEnc._cancelNoShow;
            tsTemp = anEnc.m_apptDateTime;
            long timeTemp = tsTemp.getTime();
            if (cancelNoShow != null && cancelNoShow.equals(CLINIC_CANCELLED_STATUS)) {
                continue;
            }
            if (tsTemp.getTime() < EarliestAppointmentTime) { break; }
            countTotAppts += 1;
            if (cancelNoShow != null && cancelNoShow.equals(PATIENT_NO_SHOW_STATUS)) {
                countNoShows += 1;
            }
            if (timeTemp > TodayTime && timeTemp < timeNextAppt) {
                tsNextAppt = tsTemp;
                timeNextAppt = timeTemp;
            }
            if (tsLastAppt == null && timeTemp < TodayTime) {
                tsLastAppt = tsTemp;
            }
        }
        if (ssn != null) {
            resEncSummary = new CommendEncSummaryData(ssn, countTotAppts, countNoShows, tsNextAppt, tsLastAppt);
        }
        return resEncSummary;
    }
}
