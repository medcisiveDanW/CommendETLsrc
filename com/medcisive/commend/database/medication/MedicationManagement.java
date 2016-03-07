package com.medcisive.commend.database.medication;

/**
 *
 * @author vhapalchambj
 */
public class MedicationManagement {

    private static java.util.HashMap<String, java.util.ArrayList<MedicationManagementData>> medVAHash;
    private static java.util.HashMap<String, java.util.ArrayList<MedicationManagementData>> medNonVAHash;
    private static int totalNumberOfMedications;
    private static java.util.Set<String> excludedSet;

    public MedicationManagement() {
    }

    public static String get(java.util.ArrayList<MedicationManagementData> medArray) {
        categorizeMedHashMap(medArray);
        String returnStr = header();
        totalNumberOfMedications = 0;

        if ((!medVAHash.isEmpty()) || (!medNonVAHash.isEmpty())) {
            if (!medVAHash.isEmpty()) {
                returnStr += returnNonPendingMeds(activeVAMedications(), medVAHash);
                returnStr += returnPendingMeds(pendingInpatientVAMedications(), medVAHash);
            }
            if (!medNonVAHash.isEmpty()) {
                returnStr += returnNonPendingMeds(activeNonVAMedications(), medNonVAHash);
                returnStr += returnPendingMeds(pendingInpatientNonVAMedications(), medNonVAHash);
            }
            returnStr += totalMedications();
        } else { /* Possible filler text if no meds.  Need to test if so. */ }
        return returnStr;
    }

    private static String returnNonPendingMeds(String anonymousHeader, java.util.HashMap<String, java.util.ArrayList<MedicationManagementData>> hash) {
        int medCounter = 0;
        String str = "";
        boolean firstPass = true;
        if (!hash.isEmpty()) {
            for (String key : hash.keySet()) {
                if (!key.equalsIgnoreCase("PENDING")) {
                    if (firstPass) {
                        firstPass = false;
                        str += anonymousHeader;
                    }
                    java.util.ArrayList<MedicationManagementData> subMedArray = hash.get(key);
                    for (int i = 0; i < subMedArray.size(); i++) {
                        medCounter++;
                        str += numberFormat(medCounter) + subMedArray.get(i).getFormatedString();
                    }
                }
            }
            totalNumberOfMedications += medCounter;
        }
        return str;
    }

    private static String returnPendingMeds(String anonymousHeader, java.util.HashMap<String, java.util.ArrayList<MedicationManagementData>> hash) {
        int medCounter = 0;
        String str = "";
        boolean firstPass = true;
        if (!hash.isEmpty()) {
            for (String key : hash.keySet()) {
                if (key.equalsIgnoreCase("PENDING")) {
                    if (firstPass) {
                        firstPass = false;
                        str += anonymousHeader;
                    }
                    java.util.ArrayList<MedicationManagementData> subMedArray = hash.get(key);
                    for (int i = 0; i < subMedArray.size(); i++) {
                        medCounter++;
                        str += numberFormat(medCounter) + subMedArray.get(i).getFormatedString();
                    }
                }
            }
            totalNumberOfMedications += medCounter;
        }
        return str;
    }

    private static void categorizeMedHashMap(java.util.ArrayList<MedicationManagementData> medArray) {
        medVAHash = new java.util.HashMap();
        medNonVAHash = new java.util.HashMap();
        for (int i = 0; i < medArray.size(); i++) {
            MedicationManagementData mmd = medArray.get(i);
            if (mmd.isVAMed) {
                setHash(mmd, medVAHash);
            } else {
                setHash(mmd, medNonVAHash);
            }
        }
    }

    private static void setHash(MedicationManagementData mmd, java.util.HashMap<String, java.util.ArrayList<MedicationManagementData>> hash) {
        if (!hash.containsKey(mmd.status)) {
            java.util.ArrayList<MedicationManagementData> subArray = new java.util.ArrayList();
            subArray.add(mmd);
            hash.put(mmd.status, subArray);
        } else {
            java.util.ArrayList<MedicationManagementData> subArray = hash.get(mmd.status);
            subArray.add(mmd);
        }
    }

    private static String numberFormat(int count) {
        String prefix = count + ")";
        int numberLenght = prefix.length();
        int numberOfSpaces = 5 - numberLenght;
        for (int i = 0; i < numberOfSpaces; i++) {
            prefix = prefix + " ";
        }
        return prefix;
    }

    private static String header() {
        String str = "MEDICATION MANAGEMENT:\n"
                + "Active Inpatient and Outpatient Medications (including Supplies):\n"
                + "\n";
        return str;
    }

    private static String activeVAMedications() {
        String str = "\n     Active VA Medications                                Status\n"
                + "=======================================================================\n";
        return str;
    }

    private static String pendingInpatientVAMedications() {
        String str = "\n     Pending Inpatient Medications                        Status\n"
                + "=======================================================================\n";
        return str;
    }

    private static String activeNonVAMedications() {
        String str = "\n     Active Non-VA Medications                            Status\n"
                + "=======================================================================\n";
        return str;
    }

    private static String pendingInpatientNonVAMedications() {
        String str = "\n     Pending Non-VA Inpatient Medications                 Status\n"
                + "=======================================================================\n";
        return str;
    }

    private static String totalMedications() {
        String str = "\n" + totalNumberOfMedications + " Total Medications";
        return str;
    }
}
