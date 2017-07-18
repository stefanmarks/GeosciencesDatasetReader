package nz.ac.aut.SentienceLab.EarthquakeDatasetReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Interface for managing data sources for earthquake data.
 * 
 * @author  Stefan Marks
 * @version 1.0 - 30.11.2016: Created
 * 
 */
public abstract class DataSource 
{
    public DataSource()
    {
        separator = ",";
        columnMap = new HashMap<>();
    }
    
    
    public abstract int getMaximumItemsPerQuery();
    

    public abstract URL constructQuery(float minMagnitude, Date startDate, Date endDate);
    
    
    public List<EarthquakeData> readSource(URL url, ProgressListener listener) throws IOException, ParseException
    {
        LinkedList<EarthquakeData> arrData = new LinkedList<>();
        
        URLConnection  connection = url.openConnection();
        BufferedReader in = new BufferedReader(
            new InputStreamReader(
            connection.getInputStream(), "UTF-8")
        );
        
        String inputLine;
        int     lineCount = 0;
        boolean execute   = true;
        while (execute && (inputLine = in.readLine()) != null)
        {
            if (lineCount == 0)
            {
                // process header
                String[] parts = inputLine.split(separator);
                parseHeader(parts);
            }
            else
            {
                // process data
                String[] parts = inputLine.split(separator, maxColumn + 2);
                if (parts.length >= maxColumn)
                {
                    EarthquakeData data = parseData(parts);
                    arrData.add(data);
                }
            }
            lineCount++;
            
            if (listener != null)
            {
                execute &= listener.signalProgress(lineCount);
            }
        }
        in.close();
        
        return arrData;
    }
    
    
    public abstract void setHeaderNames(Map<String, EarthquakeData.Item> map);
    
    
    public boolean parseHeader(String[] headers)
    {
        Map<String, EarthquakeData.Item> headerNames = new HashMap<>();
        setHeaderNames(headerNames);
        
        maxColumn = -1;
        for (int column = 0 ; column < headers.length ; column++ )
        {
            EarthquakeData.Item item = headerNames.get(headers[column]);
            if (item != null)
            {
                columnMap.put(item, column);
                maxColumn = Math.max(maxColumn, column);
            }
        }
        
        // sanity check and maximum column number check
        boolean success = true;
        for (EarthquakeData.Item item : EarthquakeData.Item.values())
        {
            if ( columnMap.get(item) == null )
            {
                System.err.println("Could not find data column for " + item);
                success = false;
            }
        }
        return success;
    }

    
    public EarthquakeData parseData(String[] parts) throws ParseException
    {
        EarthquakeData data  = new EarthquakeData();
        
        data.id        = parts[columnMap.get(EarthquakeData.Item.ID)].trim();
        data.longitude = Double.parseDouble(parts[columnMap.get(EarthquakeData.Item.LONGITUDE)]);
        data.latitude  = Double.parseDouble(parts[columnMap.get(EarthquakeData.Item.LATITUDE)]);
        data.depth     = Float.parseFloat(parts[columnMap.get(EarthquakeData.Item.DEPTH)]);
        data.magnitude = Float.parseFloat(parts[columnMap.get(EarthquakeData.Item.MAGNITUDE)]);
        
        return data;
    }
    
    
    public interface ProgressListener
    {
        public boolean signalProgress(int lineCount);
    }
    
    
    protected       String                            separator;
    protected final Map<EarthquakeData.Item, Integer> columnMap;
    protected       int                               maxColumn;
}
