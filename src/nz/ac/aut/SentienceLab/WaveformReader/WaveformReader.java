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
import java.util.Map;
import java.util.HashMap;

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
    
    
    private class ChannelData
    {
        int    idx;
        int    sampleCount;
        double signalSum;
        
        public ChannelData(int _idx) { idx = _idx; sampleCount = 0; signalSum = 0; }
        public void   addSignal(double _s) { sampleCount++; signalSum += _s; }
        public double getOffset() { return signalSum / sampleCount; }
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

            System.out.println("Preprocessing signals...");
           
            // finding earliest start timestamp
            long startTimeMs  = 0;
            for (Timeseries timeseries : timeSeriesCollection) 
            {
                for (Segment segment : timeseries.getSegments()) 
                {
                    System.out.println(
                        timeseries.getNetworkCode() + "\t" +
                        timeseries.getStationCode() + "\t" +
                        timeseries.getLocation() + "\t" +
                        timeseries.getChannelCode() + "\t" +
                        segment.getStartTime() + "\t" +
                        segment.getSampleCount() + "\t" +
                        segment.getSamplerate() + "\t" +
                        segment.getType().name());
                    
                    long startTime = segment.getStartTime().getTime();
                    if (startTimeMs == 0) 
                    {
                        startTimeMs = startTime;
                    }
                    if (startTime < startTimeMs)
                    {
                        startTimeMs = startTime;
                    }
                }
            }
            
            // determining buffer size
            int    sampleRate   = 0;
            double maxSize      = 0;
            int    maxSignal    = 0;
            Map<String, ChannelData> channels = new HashMap<>();
            
            for (Timeseries timeseries : timeSeriesCollection) 
            {
                String channelName = timeseries.getChannelCode();
                ChannelData channel = channels.get(channelName);
                if (channel == null)
                {
                    channel = new ChannelData(channels.size());
                    channels.put(channelName, channel);
                }
                
                for (Segment segment : timeseries.getSegments()) 
                {
                    if (sampleRate == 0)
                    {
                        sampleRate = (int) segment.getSamplerate();
                    }
                        
                    long   timeOffsetMs = segment.getStartTime().getTime() - startTimeMs;
                    double startIdx     = timeOffsetMs / 1000.0 * sampleRate;
                    double endIdx       = startIdx + segment.getSampleCount();
                    maxSize = Math.max(maxSize, endIdx);
                    
                    for (Integer v : segment.getIntData()) 
                    {
                        maxSignal = Math.max(maxSignal, Math.abs(v));
                        channel.addSignal(v);
                    }
                }
            }
            long waveformBufferSize = (long) maxSize; 
            System.out.println("Waveform buffer size: " + waveformBufferSize);
            System.out.println("Max signal amplitude: " + maxSignal);
            System.out.println("Channel names       : " + channels.keySet().toString());
            System.out.println("Allocatig buffers");
            double[][] signal = new double[channels.size()][(int) waveformBufferSize];
            for (double[] channel : signal) {
                for (int j = 0; j < channel.length; j++) {
                    channel[j] = 0;
                }
            }
            
            System.out.println("Generating WAV files");
            for (Timeseries timeseries : timeSeriesCollection) 
            {
                ChannelData channel = channels.get(timeseries.getChannelCode());
                int channelIdx = channel.idx;
                double offset = channel.getOffset();
                
                for (Segment segment : timeseries.getSegments()) 
                {
                    
                    long timeOffsetMs = segment.getStartTime().getTime() - startTimeMs;
                    int  startIdx     = (int) (timeOffsetMs / 1000.0 * sampleRate);
                    
                    for (Integer v : segment.getIntData()) 
                    {
                        signal[channelIdx][startIdx] = ((double) v - offset) / maxSignal;
                        startIdx++;
                    }
                }
            }
            
            String filename = "";
            for (Timeseries timeseries : timeSeriesCollection) 
            {
                filename = event.getPublicId().split("/")[1] 
                        + "_" + timeseries.getNetworkCode() 
                        + "_" + timeseries.getStationCode() 
                        + "_" + timeseries.getLocation()
                        + ".wav";
            }
            
            WavFile wavFile = WavFile.newWavFile(new File(filename), signal.length, waveformBufferSize, 16, sampleRate * 100);
            wavFile.writeFrames(signal, (int) waveformBufferSize);
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
        
        String eventName = "2016p858000"; 
        //Inangahua 1968	1550210
        //Edgecumbe 1987	04228
        //East Cape 1995	731516
        //Fjordland 2003	2103645
        //Dusky Sound 2009	3124785
        //Darfield 2010         3366146
        //Christchurch 2011	3468575
        //Cook Strait 2013	2013p543824
        //East Coast 2016	2016p661332
        //Kaikoura 2016         2016p858000
        
        Event event = r.getEvent(eventName);
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
