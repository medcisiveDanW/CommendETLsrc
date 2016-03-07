package com.medcisive.commend.database.medication;


/**
 *
 * @author vhapalchambj
 */
public class MedicationManagementData {

    public String med;
    public String instruction;
    public String status;
    public boolean isVAMed;
    public java.util.ArrayList<String> strArray;


    public MedicationManagementData() {
		strArray = new java.util.ArrayList();
	}
    public MedicationManagementData(String med, String instruction, String status, boolean isVAMed) {
		this.med = med;
		this.instruction = instruction;
		this.status = status;
		this.isVAMed = isVAMed;
		strArray = new java.util.ArrayList();
	}
    public String getFormatedString() {
		String str = "";
		if(!isVAMed) {
			str += "Non-VA ";
		}
        str += med + " " + instruction;
        String subStr = "";
        String returnString = "";
        int strLength = 52;
        while(str.length()>0) {
			subStr = getUnformattedString(str, strLength);
			str = str.substring(subStr.length(), str.length());
			subStr = formatString(subStr, strLength);
			if(strLength==52) {
				subStr += " " + status;
				strLength = 50;
			}
			else {
				subStr = "       " + subStr;
			}
			strArray.add(subStr + "\n");
		}
		for(int i = 0; i < strArray.size(); i++) {
			returnString = returnString + strArray.get(i);
		}
        return returnString;
    }
    private String getUnformattedString(String str, int length) {
		int pos = postionBeforeWord(str,length);
		return str.substring(0, pos);
	}
    private String formatString(String str, int length) {
        str = str.trim();
        int neededSpaces = length - str.length();
        while(neededSpaces>0) {
                neededSpaces--;
                str = str + " ";
        }
        return str;
    }
    private int postionBeforeWord(String str, int currentPostion) {
		int currPos = currentPostion;
		if(str.length()<currPos) {
			return str.length();
		}
		char currChar = str.charAt(currPos);
		if(currChar==' ') {	return currPos; }
		else {
			while(currChar!=' ') {
				currPos--;
				currChar = str.charAt(currPos);
			}
		}
		return currPos;
	}
}
