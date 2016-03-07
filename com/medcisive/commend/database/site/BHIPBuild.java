package com.medcisive.commend.database.site;

/**
 *
 * @author vhapalchambj
 */
public class BHIPBuild extends com.medcisive.utility.sql2.DBCUtil {
    
    public static Thread createThread(final int sta3n) {
        Thread result = new Thread() {
            BHIPBuild bhipBuilder = new BHIPBuild();
            
            @Override
            public void run() {
                bhipBuilder.insertCommendVISNBHIPTeams();
                bhipBuilder.insertCommendVISNBHIPPatientPanel();
                bhipBuilder.updateCommendVISNFlatEncounters();
            }
        };
        result.start();
        return result;
    }

    public void insertCommendVISNBHIPTeams() {
        String query =
            "insert into Commend.dbo.CommendVISNBHIPTeams \n" +
            "select bpt.Sta3n, \n" +
            "       pt.PatientName, \n" +
            "       pt.PatientSSN, \n" +
            "       pt.PatientSID, \n" +
            "       pt.Age, \n" +
            "       bpt.Team, \n" +
            "       stf.staffName as ProviderName, \n" +
            "       stf.staffSID as ProviderSID \n" +
            "from VDWWork.PCMM.patientTeamAssignment bpt \n" +
            " join VDWWork.SPatient.SPatient pt on \n" +
            "   pt.sta3n = bpt.sta3n and \n" +
            "   pt.patientSID = bpt.patientSID \n" +
            " join VDWWork.PCMM.PatientProviders prv on \n" +
            "   prv.sta3n = bpt.sta3n and \n" +
            "   prv.patientSID = bpt.patientSID and \n" +
            "   prv.Team = bpt.team and \n" +
            "   prv.relationshipStartDate < CONVERT(date,getDate()) and \n" +
            "   prv.relationshipEndDate is null \n" +
            " join VDWWork.SStaff.SStaff stf on \n" +
            "   stf.sta3n = prv.sta3n and \n" +
            "   stf.staffSID = prv.primaryProviderSID \n" +
            "where bpt.team like 'MH-BHIP%' \n" +
            "  and bpt.PatientTeamStartDate < CONVERT(date,getDate()) \n" +
            "  and bpt.PatientTeamEndDate is null";
        _dest.update("DELETE FROM Commend.dbo.CommendVISNBHIPTeams");
        _dest.update(query);
    }
        
    public void insertCommendVISNBHIPPatientPanel(){
        String query =
            "insert into Commend.dbo.CommendVISNBHIPPatientPanel (sta3n, patientSID, nextAppointmentDateTime, nextAppointmentLocation) \n" +
            "select tm.Sta3n, \n" +
            "       tm.PatientSID, \n" +
            "       apt.appointmentDateTime, \n" +
            "       lc.locationName \n" +
            "from Commend.dbo.CommendVISNBHIPTeams tm \n" +
            "  join VDWWork.Appt.Appointment apt on \n" +
            "    apt.Sta3n = tm.sta3n and \n" +
            "    apt.PatientSID = tm.patientSID and \n" +
            "    apt.AppointmentDateTime >= CONVERT(date, getDate()) \n" +
            "  join VDWWork.dim.location lc on \n" +
            "    lc.sta3n = apt.sta3n and \n" +
            "    lc.locationSID = apt.locationSID and \n" +
            "    lc.primaryStopCode >= 500 and \n" +
            "    lc.primaryStopCode < 600 \n" +
            "where \n" +
            "     not exists \n" +
            "     (select ap2.appointmentDateTime \n" +
            "      from VDWWork.Appt.Appointment ap2 \n" +
            "         join VDWWork.dim.location lc2 on \n" +
            "           lc2.sta3n = ap2.sta3n and \n" +
            "           lc2.locationSID = ap2.locationSID and \n" +
            "           lc2.primaryStopCode >= 500 and \n" +
            "           lc2.primaryStopCode < 600 \n" +
            "      where ap2.sta3n = tm.sta3n and \n" +
            "            ap2.patientSID = tm.patientSID and \n" +
            "            ap2.AppointmentDateTime >= CONVERT(date, getDate()) and \n" +
            "            ap2.appointmentDateTime < apt.appointmentDateTime \n" +
            "      ) \n" +
            "order by apt.appointmentDateTime";
        _dest.update("DELETE FROM Commend.dbo.CommendVISNBHIPPatientPanel");
        _dest.update(query);
    }
    
    public void updateCommendVISNFlatEncounters() {
        String query =
            "update enc \n" +
            "set enc.primaryICDCode = vw.ICDCode, \n" +
            "    enc.primaryICDCodeDescription = vw.icddescription \n" +
            "from Commend.dbo.CommendVISNFlatEncounters as enc \n" +
            "join Commend.dbo.CommendVISNBHIPDiagnosesView vw on \n" +
            "      vw.sta3n = enc.sta3n and \n" +
            "      vw.patientSID = enc.patientSID and \n" +
            "      vw.visitDateTime = enc.visitDateTime";
        _dest.update(query);
    }
}
