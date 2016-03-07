package com.medcisive.commend.database;

import com.google.gson.Gson;
import com.medcisive.commend.database.site.*;
import com.medcisive.utility.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
/**
 *
 * @author vhapalchambj
 */
public class DatabaseBuilder extends UtilityFramework {

    public static void main(String[] args) {
        String build = args[1];
        init2(args[0]);
        LogUtility.warn("DatabaseBuilder has started. (" + build + ")");
        DatabaseThreadUtility tu = new DatabaseThreadUtility();
        tu.start();
        if(build.equalsIgnoreCase("main")) {
            BatchBuilder bb = new BatchBuilder();
            bb.buildDatabase(640);
        }
        else if(build.equalsIgnoreCase("site")) {
            VISNBuilder sb = new VISNBuilder(640);
            java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
            DateFormat formater = new SimpleDateFormat("EEEE");
            String today = formater.format(ts);
            java.util.List buildDays = null;
            try {
                buildDays = new Gson().fromJson(PropertiesUtility.get("FRAMEWORK").getProperty("VISN_BUILD_DAYS"), java.util.List.class);
            } catch (Exception ex) {
                LogUtility.error(ex);
            }
            if((buildDays!=null)&&(!buildDays.isEmpty())) {
                boolean correctDay = false;
                for(Object o : buildDays) {
                    if(today.equalsIgnoreCase(o.toString())) {
                        System.out.println("Today is " + o);
                        correctDay = true;
                        break; // shouldnt be needed but blocks second attempt to run builder.
                    }
                }
                if(correctDay) {
                    sb.buildDatabase();
                } else {
                    System.out.println("Today: " + formater.format(new java.sql.Timestamp(System.currentTimeMillis())) + " is not the build DAY of: " + buildDays);
                }
            }
            else { System.out.println("VISN_BUILD_DAYS not set correctly. i.e. [Monday,Tuesday]"); }
        }
        else { System.out.println("Could not initialize. Incorrect second argument."); }
        tu.kill();
        LogUtility.warn("DatabaseBuilder has ended. (" + build + ")");
        System.runFinalization();
    }
}