package nz.ac.aut.SentienceLab.EarthquakeDatasetReader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Class for data of a single earthquake event.
 * 
 * @author  Stefan Marks
 * @version 1.0 - 30.11.2016: Created
 */
public class EarthquakeData 
{
    public String  id, information;
    public Date    timestamp;
    public double  longitude, latitude;
    public float   depth, magnitude;

    
    public enum Item
    {
        ID, TIMESTAMP, LONGITUDE, LATITUDE, DEPTH, MAGNITUDE, INFORMATION;
    }

    
    public EarthquakeData()
    {
        id = "";
        timestamp = null;
        longitude = latitude = 0;
        magnitude = depth    = 0;
    }
    
    
    public EarthquakeData(EarthquakeData copy)
    {
        id          = copy.id;
        timestamp   = (Date) copy.timestamp.clone();
        longitude   = copy.longitude;
        latitude    = copy.latitude;
        depth       = copy.depth;
        magnitude   = copy.magnitude;
        information = copy.information;
    }


    @Override
    public String toString()
    {
        return String.format(
                "EQ '%1$s': %2$td/%2$tm/%2$tY %2$tH:%2$tM, Mag %6$.1f, Pos %3$+7.2f/%4$+6.2f/%5$.1fkm, '%7$s'", 
                id, timestamp, longitude, latitude, depth, magnitude, information);
    }
    
    
    public String toCSV()
    {
        DATE_FORMAT_CSV.setTimeZone(TIMEZONE);

        StringBuilder output = new StringBuilder();
        NUMBER_FORMAT.setMaximumFractionDigits(8);
        output.append('"').append(id).append('"')
            .append(',').append(DATE_FORMAT_CSV.format(timestamp))
            .append(',').append(NUMBER_FORMAT.format(longitude))
            .append(',').append(NUMBER_FORMAT.format(latitude));
        NUMBER_FORMAT.setMaximumFractionDigits(3);
        output
            .append(',').append(NUMBER_FORMAT.format(depth))
            .append(',').append(NUMBER_FORMAT.format(magnitude))
            .append(',').append('"').append(information).append('"')
        ;
        return output.toString();
    }
    
    
    public ByteBuffer toBinary()
    {
        BUFFER.order(ByteOrder.LITTLE_ENDIAN);
        BUFFER.clear();
        
        ByteBuffer idUTF8 = STRING_CHARSET.encode(id);
        BUFFER.put((byte) idUTF8.limit()); // not strictly correct for strings > 127 characters (C# expects 7bit variable integer encoded)
        BUFFER.put(idUTF8);
        
        CALENDAR.setTime(timestamp);
        BUFFER.putShort((short)  CALENDAR.get(Calendar.YEAR));
        BUFFER.put(     (byte)  (CALENDAR.get(Calendar.MONTH) + 1));
        BUFFER.put(     (byte)   CALENDAR.get(Calendar.DAY_OF_MONTH));
        BUFFER.put(     (byte)   CALENDAR.get(Calendar.HOUR));
        BUFFER.put(     (byte)   CALENDAR.get(Calendar.MINUTE));
        BUFFER.put(     (byte)   CALENDAR.get(Calendar.SECOND));
        
        BUFFER.putFloat((float) longitude);
        BUFFER.putFloat((float) latitude);
        BUFFER.putFloat(depth);
        BUFFER.putFloat(magnitude);

        ByteBuffer infoUTF8 = STRING_CHARSET.encode(information);
        BUFFER.put((byte) infoUTF8.limit());
        BUFFER.put(infoUTF8);

        return BUFFER.flip();
    }
    
    
    public void fromCSV(String data) throws NumberFormatException, ParseException
    {
        DATE_FORMAT_CSV.setTimeZone(TIMEZONE);
        
        String[] parts = data.split(",");
        
        id        = parts[0].replace("\"", "");
        timestamp = DATE_FORMAT_CSV.parse(parts[1]);
        longitude = round(Double.valueOf(parts[2]), 8);
        latitude  = round(Double.valueOf(parts[3]), 8);
        depth     = (float) round(Double.valueOf(parts[4]), 3);
        magnitude = (float) round(Double.valueOf(parts[5]), 3);
        information = (parts.length > 6) ? parts[6].replace("\"", "") : "";
    }
    
    
    private double round(double _value, int digits)
    {
        double power = Math.pow(10, digits);
        return (Math.round(_value * power) / power);
    }
    
    
    public static String getCSV_Header()
    {
        return "id,timestamp,longitude,latitude,depth,magnitude,information";
    }


    @Override
    public boolean equals(Object o)
    {
        if (o instanceof EarthquakeData) 
        {
            EarthquakeData other = (EarthquakeData) o;
            if (this == o) return true;
            return this.id.equals(other.id);
        }
        return false;
    }

    
    public void checkMinimum(EarthquakeData other)
    {
        id        = "min";
        longitude = Math.min(longitude, other.longitude);
        latitude  = Math.min(latitude,  other.latitude);
        magnitude = Math.min(magnitude, other.magnitude);
        depth     = Math.min(depth,     other.depth);
        timestamp = timestamp.compareTo(other.timestamp) < 0 ? timestamp : other.timestamp;
    }
    
    
    public void checkMaximum(EarthquakeData other)
    {
        id        = "max";
        longitude = Math.max(longitude, other.longitude);
        latitude  = Math.max(latitude,  other.latitude);
        magnitude = Math.max(magnitude, other.magnitude);
        depth     = Math.max(depth,     other.depth);
        timestamp = timestamp.compareTo(other.timestamp) > 0 ? timestamp : other.timestamp;
    }


    public static final Comparator<EarthquakeData> SORT_BY_ID   = (EarthquakeData o1, EarthquakeData o2) -> o1.id.compareToIgnoreCase(o2.id);
    public static final Comparator<EarthquakeData> SORT_BY_DATE = (EarthquakeData o1, EarthquakeData o2) -> o1.timestamp.compareTo(o2.timestamp);
    
    private static final DateFormat    DATE_FORMAT_CSV = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    public  static final TimeZone      TIMEZONE        = TimeZone.getTimeZone("GMT");
    private static final DecimalFormat NUMBER_FORMAT   = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));

    private static final ByteBuffer    BUFFER          = ByteBuffer.allocate(1024);
    private static final Charset       STRING_CHARSET  = Charset.forName("UTF8");
    private static final Calendar      CALENDAR        = Calendar.getInstance(TIMEZONE);
}
