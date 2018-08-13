package nz.ac.aut.SentienceLab.EarthquakeDatasetReader;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;


/**
 * Class for a USGS data sources for earthquake data.
 * 
 * @author  Stefan Marks
 * @version 1.0 - 30.11.2016: Created
 * 
 */
public class DataSource_USGS extends DataSource 
{
    @Override
    public int getMaximumItemsPerQuery()
    {
        return 20000;
    };
    

    @Override
    public URL constructQuery(float minMagnitude, Date startDate, Date endDate)
    {
        URL url = null;
        try
        {
            url = new URL(
                "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv" +
                "&starttime=" + DATE_FORMAT_QUERY.format(startDate) + 
                "&endtime=" + DATE_FORMAT_QUERY.format(endDate) +
                "&minmagnitude=" + minMagnitude + 
                "&eventtype=earthquake&orderby=time");
        }
        catch (MalformedURLException e)
        {
            System.err.println(e);
        }
        return url;
    }
    
    
    @Override
    public EarthquakeData filter(EarthquakeData data)
    {
        return data;
    }

            
    @Override
    public void setHeaderNames(Map<String, EarthquakeData.Item> map)
    {
        map.put("id",        EarthquakeData.Item.ID);
        map.put("time",      EarthquakeData.Item.TIMESTAMP);
        map.put("longitude", EarthquakeData.Item.LONGITUDE);
        map.put("latitude",  EarthquakeData.Item.LATITUDE);
        map.put("depth",     EarthquakeData.Item.DEPTH);
        map.put("mag",       EarthquakeData.Item.MAGNITUDE);
    }
    

    @Override
    public EarthquakeData parseData(String[] parts) throws ParseException
    {
        EarthquakeData data  = super.parseData(parts);
        if ( data != null )
        {   
            data.timestamp = DATE_FORMAT_PARSE.parse(parts[columnMap.get(EarthquakeData.Item.TIMESTAMP)]);
        }
        return data;
    }
    
    
    @Override
    public String toString()
    {
        return "USGS";
    }
    
        
    private static final DateFormat DATE_FORMAT_QUERY = new SimpleDateFormat("yyyy-MM-dd%20HH:mm:ss");
    private static final DateFormat DATE_FORMAT_PARSE = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
}
