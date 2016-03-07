package com.medcisive.commend.database;

import com.medcisive.utility.*;
/**
 *
 * @author vhapalchambj
 */
public class DatabaseThreadUtility extends ThreadUtility {

    public DatabaseThreadUtility() {
        super("FRAMEWORK");
    }
    @Override
    protected void _timeoutException() {
        LogUtility.error("Timeout has occurred!");
        new BatchBuilderDatabaseController().logEvent("MDWS Threads Timeout.", 1000009);
    }
}
