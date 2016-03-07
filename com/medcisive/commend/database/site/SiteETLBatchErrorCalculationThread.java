//package com.medcisive.commend.database.site;
//
//import com.medcisive.utility.sql.TableMap;
//
///**
// *
// * @author vhapalchambj
// */
//public class SiteETLBatchErrorCalculationThread extends Thread {
//    private String removeQuery = "";
//    private String ssn;
//    public static int count = 0;
//
//    public SiteETLBatchErrorCalculationThread(String ssn) {
//        this.ssn = ssn;
//    }
//
//    @Override
//    public void run() {
//        go();
//    }
//
//    private void go() {
//        com.medcisive.utility.sql.DatabaseController _sql = com.medcisive.utility.sql.SQLUtility.getSingleton("FRAMEWORK").getDestinationDatabase();
//        String query =
//                "SELECT * \n"
//                + "FROM Commend.dbo.CommendTempSiteETLBatchErrorTable \n"
//                + "WHERE SSN = " + ssn + " \n"
//                + "ORDER BY encounterDateTime ";
//        TableMap table = _sql.getTable(query);
//        java.util.ArrayList<java.sql.Timestamp> dates = new java.util.ArrayList();
//        if(table==null || table.keySet() == null) {
//            System.out.println("Error (SiteETLBatchErrorCalculationThread): " + ssn);
//            return;
//        }
//        for (Integer i : table.keySet()) {
//            java.util.LinkedHashMap<String, Object> map = table.getRow(i);
//            java.sql.Timestamp ts = (java.sql.Timestamp) map.get("encounterDateTime");
//            if (!dates.contains(ts)) {
//                dates.add(ts);
//            }
//        }
//        for (java.sql.Timestamp date : dates) {
//            int keepBatchID = -1;
//            for (Integer i : table.keySet()) {
//                java.util.LinkedHashMap<String, Object> map = table.getRow(i);
//                java.sql.Timestamp ts = (java.sql.Timestamp) map.get("encounterDateTime");
//                if (date.getTime() == ts.getTime()) {
//                    int batch = (Integer) map.get("ETLBatchID");
//                    if (batch > keepBatchID) {
//                        keepBatchID = batch;
//                    }
//                }
//            }
//            if (keepBatchID > 0) {
//                count++;
//                removeQuery +=
//                        "DELETE FROM Commend.dbo.CommendTempSiteFilteredProcedureTable \n"
//                        + "WHERE SSN = " + ssn + " \n"
//                        + "AND ETLBatchID != " + keepBatchID + " \n"
//                        + "AND encounterDateTime = '" + date + "' \n\n";
//            }
//        }
//    }
//
//    public String getQuery() {
//        return removeQuery;
//    }
//}
