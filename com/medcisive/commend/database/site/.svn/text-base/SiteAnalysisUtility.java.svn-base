package com.medcisive.commend.database.site;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 *
 * @author vhapalchambj
 */
public class SiteAnalysisUtility extends Thread {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    protected static long _yearms = 31536000000L;
    protected static long _dayms = 86400000L;
    protected static long _nintyEightDays = 8467200000L;
    protected static final java.util.Date _todayDate = new java.util.Date();
    protected static final java.sql.Timestamp _today = new java.sql.Timestamp(_todayDate.getTime());
    protected static final java.util.ArrayList<java.sql.Timestamp> _fiscalYears = new java.util.ArrayList();

    static {
        Calendar c = Calendar.getInstance();
        int year = c.get(Calendar.YEAR);
        c.set(year, Calendar.OCTOBER, 1);
        long fiscal = c.getTimeInMillis();
        long now = _today.getTime();
        if(fiscal>now) {
            year--;
        }
        year++;
        c.set(year, Calendar.OCTOBER, 1, 0, 0, 0);
        java.sql.Timestamp fy = new java.sql.Timestamp(c.getTimeInMillis());
        _fiscalYears.add(fy);
        year--;
        c.set(year, Calendar.OCTOBER, 1, 0, 0, 0);
        fy = new java.sql.Timestamp(c.getTimeInMillis());
        _fiscalYears.add(fy);
        year--;
        c.set(year, Calendar.OCTOBER, 1, 0, 0, 0);
        fy = new java.sql.Timestamp(c.getTimeInMillis());
        _fiscalYears.add(fy);
    }

    public SiteAnalysisUtility() {}

    protected java.util.Map<String, java.sql.Timestamp> getQualifyingVisit(java.sql.Timestamp start, java.sql.Timestamp end, java.util.List<java.sql.Timestamp> list) {
        Queue<java.sql.Timestamp> queue = new LinkedList<java.sql.Timestamp>();
        java.util.Map<java.sql.Timestamp, java.sql.Timestamp> map = new java.util.LinkedHashMap();
        for (java.sql.Timestamp ts : list) {
            queue.add(ts);
            if (queue.size() == 2) {
                java.sql.Timestamp s = queue.peek();
                long diff = _differenceOfDatesInDays(s, ts);
                if (diff <= 90) {
                    map.put(ts, s);
                }
                queue.poll();
            }
        }
        java.util.Map<String, java.sql.Timestamp> result = new java.util.LinkedHashMap();
        for (java.sql.Timestamp ts : map.keySet()) {
            if (ts.getTime() >= start.getTime() && (ts.getTime() <= end.getTime())) {
                result.put("second", ts);
                result.put("first", map.get(ts));
                return result;
            } else if (ts.getTime() >= end.getTime()) {
                break;
            }
        }
        return null;
    }

    protected List<java.sql.Timestamp> _get8in14(java.sql.Timestamp start, java.sql.Timestamp end, java.util.List<java.sql.Timestamp> list) {
        Queue<java.sql.Timestamp> result = new LinkedList();
        for (java.sql.Timestamp latest : list) {
            result.add(latest);
            if (result.size() == 8) {
                java.sql.Timestamp earliest = result.peek();
                if( (earliest.getTime() < start.getTime()) || (latest.getTime() > end.getTime()) ) {
                    result.poll();
                    continue;
                }
                long diff = _differenceOfDatesInDays(earliest, latest);
                if (diff <= 98) {
                    return (List<java.sql.Timestamp>) result;
                }
                result.poll();
            }
        }
        return null;
    }

    private long _differenceOfDatesInDays(java.sql.Timestamp t1, java.sql.Timestamp t2) {
        long result = t2.getTime() - t1.getTime();
        result /= 1000;
        result /= 60;
        result /= 60;
        result /= 24;
        return result;
    }

    protected java.util.List<java.sql.Timestamp> _hasPossible8in14(java.util.List<java.sql.Timestamp> list, java.sql.Timestamp end) {
        java.util.List<java.sql.Timestamp> result = new java.util.ArrayList();
        if ((list == null) || (end==null)) {
            return result;
        }
        int size = list.size();
        java.sql.Timestamp last = list.get(size - 1);
        for (java.sql.Timestamp ts : list) {
            result = _sublist(list,ts);
            long diff = last.getTime() - ts.getTime();
            //System.out.println("is less than 98 days?... Diff(days): " + diff/_dayms + " last: " + last + " current: " + ts);
            if (((diff) > _nintyEightDays) || (result.isEmpty())) {
                continue;
            } else if (_isPossible8in14((java.util.List<java.sql.Timestamp>) result, end)) {
                return (java.util.List<java.sql.Timestamp>) result;
            }
        }
        return null;
    }

    private java.util.List<java.sql.Timestamp> _sublist(java.util.List<java.sql.Timestamp> list, java.sql.Timestamp start) {
        java.util.List<java.sql.Timestamp> result = new java.util.ArrayList();
        if(list==null || start==null) { return result; }
        for(java.sql.Timestamp ts : list) {
            if(ts.getTime()>=start.getTime()) {
                result.add(ts);
            }
        }
        return result;
    }

    private boolean _isPossible8in14(java.util.List<java.sql.Timestamp> list, java.sql.Timestamp end) {
        if (list == null) {
            return false;
        }
        int size = list.size();
        java.sql.Timestamp first = list.get(0);
        if (size > 0) {
            float numberOfSessionsToDo = 8 - size;
            long timeRemaining = _nintyEightDays - (end.getTime() - first.getTime());
            long timeNeeded = (long)((numberOfSessionsToDo / 1.0) * 7.0) * _dayms;
            //System.out.println("List: " + list + " Sessions needed: " + numberOfSessionsToDo + " timeRemaining: " + timeRemaining/60000 + " timeNeeded: " + timeNeeded/60000);
            if (timeRemaining > timeNeeded) {
                return true;
            }
        }
        return false;
    }
}
