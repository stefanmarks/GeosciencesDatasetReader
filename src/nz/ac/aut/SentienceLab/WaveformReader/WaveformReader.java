package nz.ac.aut.SentienceLab.WaveformReader;

import thirdparty.WavFileException;
import thirdparty.WavFile;
import edu.iris.dmc.criteria.CriteriaException;
import edu.iris.dmc.criteria.EventCriteria;
import edu.iris.dmc.criteria.StationCriteria;
import edu.iris.dmc.criteria.WaveformCriteria;
import edu.iris.dmc.event.model.Event;
import edu.iris.dmc.fdsn.station.model.Network;
import edu.iris.dmc.fdsn.station.model.Station;
import edu.iris.dmc.service.EventService;
import edu.iris.dmc.service.NoDataFoundException;
import edu.iris.dmc.service.ServiceNotSupportedException;
import edu.iris.dmc.service.ServiceUtil;
import edu.iris.dmc.service.StationService;
import edu.iris.dmc.service.WaveformService;
import edu.iris.dmc.timeseries.model.Segment;
import edu.iris.dmc.timeseries.model.Timeseries;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;

/**
 * @author Stefan Marks (stefan.marks.ac@gmail.com)
 */
public class WaveformReader 
{
    public WaveformReader()
    {
        serviceUtil = ServiceUtil.getInstance();
        serviceUtil.setAppName("AUT SentienceLab Java Client");
        
        String baseURL = "http://beta-service.geonet.org.nz/fdsnws/";
        stationService  = serviceUtil.getStationService(baseURL + "station/1/");
        eventService    = serviceUtil.getEventService(baseURL + "event/1/");
        waveformService = serviceUtil.getWaveformService(baseURL + "dataselect/1/");
    }


    public List<Station> getStations(Event event)
    {
        StationCriteria stationCriteria = new StationCriteria();
        stationCriteria.addNetwork("NZ");
        stationCriteria.addLocation("20");
        stationCriteria.addChannel("H*");
        
        List<Station> stations = new ArrayList<>();
        try 
        {
            List<Network> networks = stationService.fetch(stationCriteria);
            networks.forEach((network) -> 
            {
                network.getStations().forEach((station) -> 
                {
                    Calendar eventDate = new GregorianCalendar(); 
                    eventDate.setTime(event.getPreferredOrigin().getTime());
                    
                    Calendar start = new GregorianCalendar();
                    start.setTime(station.getStartDate());
                    
                    Calendar end = new GregorianCalendar();
                    if (station.getEndDate() != null) end.setTime(station.getEndDate());
                    else                              end.setTime(Date.from(Instant.now()));
                    
                    if ((start.compareTo(eventDate) < 0) &&
                        (end.compareTo(eventDate) > 0) &&
                        (station.getSelectedNumberChannels().intValue() >= 3)) 
                    {
                        stations.add(station);
                    }
                });
            });
        } 
        catch (NoDataFoundException | CriteriaException | ServiceNotSupportedException | IOException ex) 
        {
            Logger.getLogger(WaveformReader.class.getName()).log(Level.SEVERE, null, ex);
        }
        return stations;
    }
    
    
    public Event getEvent(String name)
    {
        EventCriteria eventCriteria = new EventCriteria();
        eventCriteria.setEventId(name);
        List<Event> events = null;
        try 
        {
            events = eventService.fetch(eventCriteria);
            for (Event event : events)
            {
                System.out.println(event.getPreferredOrigin().getTime());
            }
        } 
        catch (NoDataFoundException | CriteriaException | ServiceNotSupportedException | IOException ex) 
        {
            Logger.getLogger(WaveformReader.class.getName()).log(Level.SEVERE, null, ex);
        }
        return events == null ? null : events.get(0);
    }
    
    
    public void getWaveform(Event event, String station)
    {
        Date eventDate = event.getPreferredOrigin().getTime();
        
        Calendar cal = Calendar.getInstance();
        cal.setTime(eventDate);
        cal.add(Calendar.SECOND, 0);
        Date startDate = cal.getTime();
        
        cal.setTime(eventDate);
        cal.add(Calendar.HOUR, 1);
        Date endDate = cal.getTime();
        
        WaveformCriteria waveCriteria = new WaveformCriteria();
        waveCriteria.add("NZ", station, "20", "H*", startDate, endDate); // TEPS, 
        // to request additional data, we coulld use more criteria.add(...) statements
        // get a list of Timeseries objects
        try
        {
            List<Timeseries> timeSeriesCollection = waveformService.fetch(waveCriteria);

            int channels = timeSeriesCollection.size();
            Timeseries t = timeSeriesCollection.get(0);
            
            String filename = event.getPublicId().split("/")[1] + "_" + t.getNetworkCode() + "_" + t.getStationCode() + "_" + t.getLocation();
                
            int[] offsets      = new int[channels];
            int   maxAmplitude = Integer.MIN_VALUE;
            int[] startOffsets = new int[channels];
            int minNumSamples  = Integer.MAX_VALUE;
            int samplerate = 0;
            int channel = 0;
            for (Timeseries timeseries : timeSeriesCollection) 
            {
                for (Segment segment : timeseries.getSegments()) 
                {
                    System.out.print(
                        timeseries.getNetworkCode() + "\t" +
                        timeseries.getStationCode() + "\t" +
                        timeseries.getLocation() + "\t" +
                        timeseries.getChannelCode() + "\t");
                    System.out.print(
                        segment.getStartTime() + "\t" +
                        segment.getSampleCount() + "\t" +
                        segment.getSamplerate() + "\t" +
                        segment.getType().name() + "\t");
                    
                    samplerate = (int) segment.getSamplerate();
                    startOffsets[channel] = (int) ((segment.getStartTime().getTime() - eventDate.getTime()) * segment.getSamplerate() / 1000.0);
                    
                    long average = 0;
                    int min = Integer.MAX_VALUE;
                    int max = Integer.MIN_VALUE;
                    for (Integer v : segment.getIntData()) 
                    {
                        average += v;
                        min = Math.min(min, v);
                        max = Math.max(max, v);
                    }
                    offsets[channel] = (int) (average / segment.getSampleCount());
                    maxAmplitude = Math.max(maxAmplitude,   max - offsets[channel]);
                    maxAmplitude = Math.max(maxAmplitude, -(min - offsets[channel]));
                    
                    int numSamples = segment.getSampleCount() + startOffsets[channel];
                    minNumSamples = Math.min(minNumSamples, numSamples);
                    System.out.println(startOffsets[channel] + "\t" + offsets[channel] + "\t" + numSamples);
                    
                    channel++;
                }
            }
                
            WavFile wavFile = WavFile.newWavFile(new File(filename + ".wav"), channels, minNumSamples, 16, samplerate * 100);

            channel = 0;
            double[][] data = new double[channels][(int) minNumSamples];
            for (Timeseries timeseries : timeSeriesCollection) 
            {
                for (Segment segment : timeseries.getSegments()) 
                {
                    int idx = startOffsets[channel];
                    for (Integer v : segment.getIntData()) 
                    {
                        if ((idx >= 0) && (idx < data[channel].length))
                        {
                            double signal = (double)(v - offsets[channel]) / maxAmplitude;
                            data[channel][idx] = Math.pow(Math.abs(signal), 0.75) * Math.signum(signal);
                        }
                        idx++;
                    }
                }
                channel++;
            }
            wavFile.writeFrames(data, minNumSamples);
            wavFile.close();
        }
        catch (CriteriaException | NoDataFoundException | ServiceNotSupportedException | IOException | WavFileException ex)
        {
            Logger.getLogger(WaveformReader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    protected final ServiceUtil     serviceUtil;
    protected final StationService  stationService;
    protected final EventService    eventService;
    protected final WaveformService waveformService;
    
       
    public static void main(String[] args) throws Exception 
    {
        WaveformReader r = new WaveformReader();
        
        Event event = r.getEvent("2016p858000");//"2016p858000");
        if (event != null)
        {
            List<Station> stations = r.getStations(event);
            
            List<String> stationNames = new ArrayList<>(stations.size());
            for (Station station : stations) 
            {
                System.out.println(station.getCode() + " " + station.getTotalNumberChannels() + "\t" + station.getSite().getName());
                stationNames.add(station.getCode() + ": " + station.getSite().getName());
            }
            
            Object choice = JOptionPane.showInputDialog(null, 
                    "Select the station name",
                    "Station Selection",
                    JOptionPane.PLAIN_MESSAGE,
                    null,
                    stationNames.toArray(), stationNames.get(0));
            if (choice instanceof String)
            {
                r.getWaveform(event, ((String) choice).split(":")[0]);
            }
        }
    }

}
