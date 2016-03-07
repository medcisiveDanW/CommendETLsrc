package com.medcisive.commend.database.medication;

import com.medcisive.commend.database.medication.MedicationManagementNode;

/**
 *
 * @author vhapalchambj
 */
public class MedicationManagementBuilder {
    public MedicationManagementBuilder() {}
    public String buildMedicationManagementString(java.util.ArrayList<MedicationManagementNode> va, java.util.ArrayList<MedicationManagementNode> non) {
        int totalMeds = 0;
        int counter = 0;
        String header =     "Active Inpatient and Outpatient Medications (including Supplies):\n" +
                            "\n";
        String vaMeds = "";
        String nonMeds = "";
        if(!va.isEmpty()) {
            vaMeds =        "     Active Outpatient Medications                          Status\n" +
                            "=========================================================================\n";
            for(int i = 0; i < va.size(); i++){
                MedicationManagementNode mmn = va.get(i);
                counter = i + 1;
                vaMeds += buildMedicationString(counter, mmn.localDrugNameWithDose, mmn.sig, mmn.rxStatus, mmn.isVAMed);
            }
            totalMeds = counter;
            counter = 0;
            vaMeds += "\n";
        }
        if(!non.isEmpty()) {
            nonMeds =       "     Active Non-VA Medications                              Status\n" +
                            "=========================================================================\n";
            for(int i = 0; i < non.size(); i++){
                MedicationManagementNode mmn = non.get(i);
                counter = i + 1;
                nonMeds += buildMedicationString(counter, mmn.localDrugNameWithDose, mmn.sig, mmn.rxStatus, mmn.isVAMed);
            }
            totalMeds += counter;
        }
        String footer =     "\n" + totalMeds + " Total Medications\n";
        return header + vaMeds + nonMeds + footer;
    }
    private String buildMedicationString(int counter, String localDrugNameWithDose, String sig, String RxSatus, boolean isVAMed) {
        String med = localDrugNameWithDose + sig;
        String returnStr = "";
        RxSatus = RxSatus.toUpperCase();
        java.util.ArrayList<String> medArrayRaw = new java.util.ArrayList();
        java.util.ArrayList<String> medArray = new java.util.ArrayList();
        String nonVAMed = "Non-VA ";
        if(!isVAMed) { med = nonVAMed + med; }
        int start = 53;
        int secondary = 51;
        boolean firstPass = true;
        while(med.length()>0) {
            String temp;
            if(firstPass) {
                temp = getSubstringBeforeWordAtIndexOf(start, med);
                firstPass = false;
            }
            else {
                temp = getSubstringBeforeWordAtIndexOf(secondary, med);
            }
            temp = temp.trim();
            medArrayRaw.add(temp);
            med = med.substring(temp.length());
            med = med.trim();
        }
        for(int i = 0; i<medArrayRaw.size(); i++) {
            if(i==0) {
                String currStr = getCounterIndentString(counter) + medArrayRaw.get(i);
                int currStrLenght = currStr.length();
                int neededSpacesTo60 = 60 - currStrLenght;
                String spaces = "";
                for(int spaceCounter = 0; spaceCounter < neededSpacesTo60; spaceCounter++) {
                    spaces = spaces + " ";
                }
                currStr = currStr + spaces + RxSatus;
                medArray.add(currStr);
            }
            else {
                medArray.add("       " + medArrayRaw.get(i));
            }
        }
        for(int i = 0; i<medArray.size(); i++) {
            returnStr += medArray.get(i) + "\n";
        }
        return returnStr;
    }
    private String getCounterIndentString(int counter) {
        String medCoutner = counter + ")";
        int numbOfSpaces = 5 - medCoutner.length();
        for(int i = 0; i<numbOfSpaces;i++) {
            medCoutner = medCoutner + " ";
        }
        return medCoutner;
    }
    private String getSubstringBeforeWordAtIndexOf(int index, String str) {
        if(index>str.length()) { return str; }
        if(str.charAt(index-1)==' ') { return str.substring(0, index); }
        else {
            int curr = index-1;
            while(curr>0) {
                if(str.charAt(curr)==' ') { return str.substring(0, curr); }
                curr--;
            }
        }
        return str;
    }
}