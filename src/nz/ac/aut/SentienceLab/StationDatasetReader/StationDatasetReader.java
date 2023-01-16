package nz.ac.aut.SentienceLab.StationDatasetReader;

import edu.iris.dmc.criteria.CriteriaException;
import edu.iris.dmc.criteria.OutputLevel;
import edu.iris.dmc.criteria.StationCriteria;
import edu.iris.dmc.fdsn.station.model.Channel;
import edu.iris.dmc.fdsn.station.model.Network;
import edu.iris.dmc.fdsn.station.model.Station;
import edu.iris.dmc.service.NoDataFoundException;
import edu.iris.dmc.service.ServiceNotSupportedException;
import edu.iris.dmc.service.ServiceUtil;
import edu.iris.dmc.service.StationService;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

/**
 * @author Stefan Marks (stefan.marks.ac@gmail.com)
 */
public class StationDatasetReader 
{
    public static void main(String[] args) throws Exception 
    {
        ServiceUtil serviceUtil = ServiceUtil.getInstance();
        serviceUtil.setAppName("AUT SentienceLab Java Client");
        
        String baseURL = "http://beta-service.geonet.org.nz/fdsnws/";
        StationService stationService = serviceUtil.getStationService(baseURL + "station/1/");
   
        StringBuilder s = new StringBuilder();
        
        StationCriteria stationCriteria = new StationCriteria();
        stationCriteria.addNetwork("NZ");
        stationCriteria.addChannel("*");
        
        try 
        {
            DateFormat DATE_FORMAT_CSV = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            TimeZone   TIMEZONE        = TimeZone.getTimeZone("GMT");
            DATE_FORMAT_CSV.setTimeZone(TIMEZONE);
            
            List<Network> networks = stationService.fetch(stationCriteria, OutputLevel.CHANNEL);
            
            s.append("Code\tDescription\tLatitude\tLongitude\tElevation\tStart\tEnd\tNumChannels\tChannels\n");
            
            for (Network network : networks)
            {
                for (Station station : network.getStations()) 
                {
                    String channels = "";
                    if (station.getChannels() != null)
                    {
                        for (Channel channel : station.getChannels()) {
                            channels += channel.getLocationCode() + ":" + channel.getCode() + ",";
                        }
                    }
                    
                    s.append(station.getCode());
                    s.append("\t").append(station.getDescription());
                    s.append("\t").append(station.getLatitude().getValue());
                    s.append("\t").append(station.getLongitude().getValue());
                    s.append("\t").append(station.getElevation().getValue());
                    s.append("\t").append((station.getStartDate() != null) ? DATE_FORMAT_CSV.format(station.getStartDate()) : "");
                    s.append("\t").append((station.getEndDate()   != null) ? DATE_FORMAT_CSV.format(station.getEndDate()  ) : "");
                    s.append("\t").append(station.getTotalNumberChannels().intValue());
                    s.append("\t").append(channels);
                    s.append("\n");
                }
            }
        
            try (FileWriter f = new FileWriter("NZ_StationList.txt")) {
                f.append(s);
            }
        } 
        catch (NoDataFoundException | CriteriaException | ServiceNotSupportedException | IOException ex) 
        {
            ex.printStackTrace(System.err);
        }
    }
}
