package nz.ac.aut.SentienceLab.EarthquakeDatasetReader;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;


/**
 * Class for a GeoNet data sources for earthquake data.
 * 
 * @author  Stefan Marks
 * @version 1.0 - 30.11.2016: Created
 * 
 */
public class DataSource_GeoNet extends DataSource 
{
    @Override
    public int getMaximumItemsPerQuery()
    {
        return Integer.MAX_VALUE;
    };
    

    @Override
    public URL constructQuery(float minMagnitude, Date startDate, Date endDate)
    {
        URL url = null;
        try
        {
            url = new URL(
                "http://wfs.geonet.org.nz/geonet/ows?service=WFS&version=1.0.0" +
                "&request=GetFeature&typeName=geonet:quake_search_v1&outputFormat=csv" + 
                "&cql_filter=origintime>='" + DATE_FORMAT_QUERY.format(startDate) + "'" +
                "+AND+origintime<'" + DATE_FORMAT_QUERY.format(endDate) + "'" +
                "+AND+magnitude>=" + minMagnitude + 
                "+AND+eventtype=earthquake");
        }
        catch (MalformedURLException e)
        {
            System.err.println(e);
        }
        return url;
    }
    
    
    @Override
    public void setHeaderNames(Map<String, EarthquakeData.Item> map)
    {
        map.put("publicid",   EarthquakeData.Item.ID);
        map.put("origintime", EarthquakeData.Item.TIMESTAMP);
        map.put("longitude",  EarthquakeData.Item.LONGITUDE);
        map.put("latitude",   EarthquakeData.Item.LATITUDE);
        map.put("depth",      EarthquakeData.Item.DEPTH);
        map.put("magnitude",  EarthquakeData.Item.MAGNITUDE);
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
        return "GeoNet";
    }

    
    private static final DateFormat DATE_FORMAT_QUERY = new SimpleDateFormat("yyyy-MM-dd");
    private static final DateFormat DATE_FORMAT_PARSE = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
}
