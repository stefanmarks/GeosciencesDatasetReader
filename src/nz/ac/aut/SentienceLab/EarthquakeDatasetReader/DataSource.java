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
        connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0");
        connection.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
        connection.connect();
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
                // System.out.println("Header: '" + inputLine + "'");
                if (!parseHeader(parts))
                {
                    throw new ParseException("Header format mismatch (input: " + inputLine + ")", lineCount);
                }
            }
            else
            {
                // process data
                String[] parts = inputLine.split(separator, maxColumn + 2);
                if (parts.length >= maxColumn)
                {
                    EarthquakeData data = parseData(parts);
                    data = filter(data);
                    if ( data != null)
                    {
                        arrData.add(data);
                    }
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
    
    
    public abstract EarthquakeData filter(EarthquakeData data);


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
        for (EarthquakeData.Item item : headerNames.values())
        {
            if (columnMap.get(item) == null)
            {
                System.err.println("Could not find data column for " + item);
                success = false;
            }
        }
        return success;
    }

    
    private double parse(String[] parts, EarthquakeData.Item item)
    {
        double value = 0;
        int    col   = columnMap.get(item);
        try
        {
            value = Double.parseDouble(parts[col]);
        }
        catch (NumberFormatException e)
        {
            System.err.println("Could not parse data in " + item + " column (\"" + parts[col] + "\")");
        }
        return value;
    }
    
    
    public EarthquakeData parseData(String[] parts) throws ParseException
    {
        EarthquakeData data  = new EarthquakeData();
        
        data.id        = parts[columnMap.get(EarthquakeData.Item.ID)].trim();
        data.longitude = parse(parts, EarthquakeData.Item.LONGITUDE);
        data.latitude  = parse(parts, EarthquakeData.Item.LATITUDE);
        data.depth     = (float) parse(parts, EarthquakeData.Item.DEPTH);
        data.magnitude = (float) parse(parts, EarthquakeData.Item.MAGNITUDE);
        
        if (columnMap.containsKey(EarthquakeData.Item.INFORMATION))
        {
            data.information = parts[columnMap.get(EarthquakeData.Item.INFORMATION)];
        }
        else            
        {
            data.information = "";
        }

        if (columnMap.containsKey(EarthquakeData.Item.TYPE))
        {
            data.type = parts[columnMap.get(EarthquakeData.Item.TYPE)].trim();
        }
        else
        {
            data.type = "";
        }
        
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
