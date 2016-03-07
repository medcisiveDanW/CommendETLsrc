package com.medcisive.commend.database.medication;

/**
 *
 * @author vhapalchambj
 */
public class MedicationManagementNode {

    public String patientIEN;
    public String localDrugNameWithDose;
    public String sig;
    public String rxStatus;
    public java.sql.Timestamp vistaCreateDate;
    public boolean isVAMed;
    public String sta3n;

    public MedicationManagementNode() {
    }

    public MedicationManagementNode(String patientIEN, String localDrugNameWithDose, String sig, String rxStatus, java.sql.Timestamp vistaCreateDate, boolean isVAMed, String sta3n) {
        this.patientIEN = patientIEN;
        this.localDrugNameWithDose = localDrugNameWithDose;
        this.sig = sig;
        this.rxStatus = rxStatus;
        this.vistaCreateDate = vistaCreateDate;
        this.isVAMed = isVAMed;
        this.sta3n = sta3n;
    }
}
