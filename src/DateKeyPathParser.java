import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Weeks;


public class DateKeyPathParser {
	
	private static final DateTime GPS_START_DATE = 
		new DateTime(1980,1,6,0,0,0,0);
	
	public String replaceWithLiterals(String INPUT, DateTime dt, String key) {

		Pattern p;
		Matcher m;

		String genNAME      = "\\$key\\$";
		String genUPPERNAME = genNAME.toUpperCase();
		String genYEAR      = "yyyy";
		String genYY        = "yy";
		String genMONTH     = "mm";
		String genDAY       = "dd";
		String genDOY       = "doy";
		String genGPSWEEK   = "gpsweek";
		String genGPSDAY    = "gpsday";
		String genGPSDATE   = "gpsdate";

		// Get date & time info
		int year  = dt.getYear();
		int month = dt.getMonthOfYear();
		int day   = dt.getDayOfMonth();
		int doy   = dt.getDayOfYear();

		String strYear = "" + year;
		String strYY   = strYear.substring(2, 4);
		String strMonth;
		String strDay;
		String strDoy;
		String strGpsWeek;

		// format strMonth
		if (month < 10) {
			strMonth = "0" + month;
		} else {
			strMonth = "" + month;
		}

		// format strDay
		if (day < 10) {
			strDay = "0" + day;
		} else {
			strDay = "" + day;
		}

		// format strDoy
		if (doy < 10) {
			strDoy = "00" + doy;
		} else if (doy < 100) {
			strDoy = "0" + doy;
		} else {
			strDoy = "" + doy;
		}
		
		// calculate the gpsweek, gpsday, and gpsdate
		Weeks numWeeks = Weeks.weeksBetween(GPS_START_DATE,dt);
		Days numDays   = Days.daysBetween(GPS_START_DATE, dt);
		
		String strGPSWeek = Integer.toString( numWeeks.getWeeks() );
		String strGPSDay  = Integer.toString( numDays.getDays() % 7 );
		
		if (numWeeks.getWeeks()<1000){
			strGPSWeek = "0"+strGPSWeek;
		}
		
		String strGPSDate = strGPSWeek+strGPSDay;

		// Replace any occurrence of "yyyy" with
		// the actual 4 digit year.
		p = Pattern.compile(genYEAR);
		m = p.matcher(INPUT);
		INPUT = m.replaceAll(strYear);

		// Replace any occurrence of "yy" with
		// the actual 4 digit year.
		p = Pattern.compile(genYY);
		m = p.matcher(INPUT);
		INPUT = m.replaceAll(strYY);

		// Replace any occurrence of "mm" with
		// the actual 2 digit month.
		p = Pattern.compile(genMONTH);
		m = p.matcher(INPUT);
		INPUT = m.replaceAll(strMonth);

		// Replace any occurrence of "dd" with
		// the actual day of the month.
		// NOTE: Not day of year.
		p = Pattern.compile(genDAY);
		m = p.matcher(INPUT);
		INPUT = m.replaceAll(strDay);

		// Replace any occurrence of "doy" with
		// the actual 3 digit day of year.
		p = Pattern.compile(genDOY);
		m = p.matcher(INPUT);
		INPUT = m.replaceAll(strDoy);
		
		// Replace any occurrence of "gpsweek" with
		// the actual number of weeks between GPS_START_DATE
		// and the current date dt
		p = Pattern.compile(genGPSWEEK);
		m = p.matcher(INPUT);
		INPUT = m.replaceAll(strGPSWeek);
		
		// Replace any occurrence of "gpsday" with
		// the actual number of day, modulo 7, between 
		// GPS_START_DATE and the current date dt
		p = Pattern.compile(genGPSDAY);
		m = p.matcher(INPUT);
		INPUT = m.replaceAll(strGPSDay);
		
		// Replace any occurrence of "gpsdate" with
		// the actual gpsweek + number of day, modulo 7, between 
		// GPS_START_DATE and the current date dt
		//   Ex: feb 4, 2009 = 15173
		//   where gpsweek is 1517 and gpsday is 3
		//   The 3rd day of gpsweek 1517.
		p = Pattern.compile(genGPSDATE);
		m = p.matcher(INPUT);
		INPUT = m.replaceAll(strGPSDate);

		// Replace any occurrence of "name" with the
		// actual name of the station as listed.
		p = Pattern.compile(genNAME);
		m = p.matcher(INPUT); // get a matcher object
		INPUT = m.replaceAll(key);

		// Replace any occurrence of "NAME" with the
		// upper case actual name of the station as listed.
		p = Pattern.compile(genUPPERNAME);
		m = p.matcher(INPUT); // get a matcher object
		INPUT = m.replaceAll(key.toUpperCase());

		return INPUT;
	}
	
	public static void main(String[] args) {
	}

}
