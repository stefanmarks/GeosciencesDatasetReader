package nz.ac.aut.SentienceLab.EarthquakeDatasetReader;

import java.awt.event.ItemEvent;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JFileChooser;
import javax.swing.JSpinner;
import javax.swing.SpinnerDateModel;

/**
 * Main class for the reader dialog.
 * 
 * @author Stefan Marks, SentienceLab, Auckland University of Technology
 */
public class EarthquakeDatasetReaderForm extends javax.swing.JFrame
{
    /**
     * Creates new form MainForm
     */
    public EarthquakeDatasetReaderForm()
    {
        Calendar c = Calendar.getInstance(); 
        c.getTime();
        maxDate = c.getTime();
        c.set(1900, 1, 1, 0, 0, 0); 
        minDate = c.getTime();
        
        initComponents();
        OnDataSourceChanged();
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {
        java.awt.GridBagConstraints gridBagConstraints;

        jFormattedTextField1 = new javax.swing.JFormattedTextField();
        javax.swing.JPanel pnlMain = new javax.swing.JPanel();
        pnlSettings = new javax.swing.JPanel();
        javax.swing.JLabel lblSource = new javax.swing.JLabel();
        cbxSource = new javax.swing.JComboBox<>();
        javax.swing.JLabel lblTime = new javax.swing.JLabel();
        javax.swing.JLabel lblStart = new javax.swing.JLabel();
        spnStart = new javax.swing.JSpinner();
        javax.swing.JLabel lblEnd = new javax.swing.JLabel();
        spnEnd = new javax.swing.JSpinner();
        javax.swing.JLabel lblMagnitude = new javax.swing.JLabel();
        spnMagnitude = new javax.swing.JSpinner();
        javax.swing.JLabel lblDestination = new javax.swing.JLabel();
        txtDestination = new javax.swing.JTextField();
        btnDestination = new javax.swing.JButton();
        javax.swing.JPanel pnlButtons = new javax.swing.JPanel();
        btnStart = new javax.swing.JButton();
        prgLoading = new javax.swing.JProgressBar();

        jFormattedTextField1.setText("jFormattedTextField1");

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        pnlMain.setBorder(javax.swing.BorderFactory.createEmptyBorder(5, 5, 5, 5));
        pnlMain.setLayout(new java.awt.BorderLayout(0, 10));

        pnlSettings.setLayout(new java.awt.GridBagLayout());

        lblSource.setText("Source:");
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 0;
        gridBagConstraints.gridy = 0;
        gridBagConstraints.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints.anchor = java.awt.GridBagConstraints.WEST;
        gridBagConstraints.insets = new java.awt.Insets(0, 0, 0, 5);
        pnlSettings.add(lblSource, gridBagConstraints);

        cbxSource.setModel(new DefaultComboBoxModel(new DataSource[] { new DataSource_GeoNet(), new DataSource_USGS() }));
        cbxSource.addItemListener(new java.awt.event.ItemListener() {
            public void itemStateChanged(java.awt.event.ItemEvent evt) {
                cbxSourceItemStateChanged(evt);
            }
        });
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridwidth = 4;
        gridBagConstraints.fill = java.awt.GridBagConstraints.HORIZONTAL;
        gridBagConstraints.weightx = 0.3;
        pnlSettings.add(cbxSource, gridBagConstraints);

        lblTime.setText("Time:");
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 0;
        gridBagConstraints.gridy = 1;
        gridBagConstraints.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints.anchor = java.awt.GridBagConstraints.WEST;
        gridBagConstraints.insets = new java.awt.Insets(5, 0, 0, 5);
        pnlSettings.add(lblTime, gridBagConstraints);

        lblStart.setText("Start:");
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 1;
        gridBagConstraints.gridy = 1;
        gridBagConstraints.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints.anchor = java.awt.GridBagConstraints.WEST;
        gridBagConstraints.insets = new java.awt.Insets(5, 0, 0, 5);
        pnlSettings.add(lblStart, gridBagConstraints);

        spnStart.setModel(new SpinnerDateModel(minDate, null, maxDate, Calendar.MONTH));
        spnStart.setEditor(new JSpinner.DateEditor(spnStart, "MM/yyyy")
        );
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 2;
        gridBagConstraints.gridy = 1;
        gridBagConstraints.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints.weightx = 0.3;
        gridBagConstraints.insets = new java.awt.Insets(5, 0, 0, 0);
        pnlSettings.add(spnStart, gridBagConstraints);

        lblEnd.setText("End:");
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 3;
        gridBagConstraints.gridy = 1;
        gridBagConstraints.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints.anchor = java.awt.GridBagConstraints.EAST;
        gridBagConstraints.insets = new java.awt.Insets(6, 10, 0, 5);
        pnlSettings.add(lblEnd, gridBagConstraints);

        spnEnd.setModel(new SpinnerDateModel(maxDate, minDate, maxDate, Calendar.MONTH));
        spnEnd.setEditor(new JSpinner
            .DateEditor(spnEnd, "MM/yyyy")
        );
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 4;
        gridBagConstraints.gridy = 1;
        gridBagConstraints.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints.weightx = 0.3;
        gridBagConstraints.insets = new java.awt.Insets(6, 0, 0, 0);
        pnlSettings.add(spnEnd, gridBagConstraints);

        lblMagnitude.setText("Magnitude:");
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 0;
        gridBagConstraints.gridy = 2;
        gridBagConstraints.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints.anchor = java.awt.GridBagConstraints.WEST;
        gridBagConstraints.insets = new java.awt.Insets(5, 0, 0, 5);
        pnlSettings.add(lblMagnitude, gridBagConstraints);

        spnMagnitude.setModel(new javax.swing.SpinnerNumberModel(Float.valueOf(4.0f), Float.valueOf(2.0f), Float.valueOf(5.0f), Float.valueOf(0.1f)));
        spnMagnitude.setEditor(new javax.swing.JSpinner.NumberEditor(spnMagnitude, "0.0"));
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 1;
        gridBagConstraints.gridy = 2;
        gridBagConstraints.gridwidth = 2;
        gridBagConstraints.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints.weightx = 0.3;
        gridBagConstraints.insets = new java.awt.Insets(5, 0, 0, 0);
        pnlSettings.add(spnMagnitude, gridBagConstraints);

        lblDestination.setText("Destination:");
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 0;
        gridBagConstraints.gridy = 3;
        gridBagConstraints.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints.anchor = java.awt.GridBagConstraints.WEST;
        gridBagConstraints.insets = new java.awt.Insets(10, 0, 0, 5);
        pnlSettings.add(lblDestination, gridBagConstraints);

        txtDestination.setText(".\\data.csv");
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 1;
        gridBagConstraints.gridy = 3;
        gridBagConstraints.gridwidth = 3;
        gridBagConstraints.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints.insets = new java.awt.Insets(10, 0, 0, 0);
        pnlSettings.add(txtDestination, gridBagConstraints);

        btnDestination.setText("...");
        btnDestination.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnDestinationActionPerformed(evt);
            }
        });
        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 4;
        gridBagConstraints.gridy = 3;
        gridBagConstraints.fill = java.awt.GridBagConstraints.BOTH;
        gridBagConstraints.insets = new java.awt.Insets(10, 5, 0, 0);
        pnlSettings.add(btnDestination, gridBagConstraints);

        pnlMain.add(pnlSettings, java.awt.BorderLayout.CENTER);

        pnlButtons.setLayout(new java.awt.GridLayout(0, 1, 0, 10));

        btnStart.setText("Start Download");
        btnStart.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnStartActionPerformed(evt);
            }
        });
        pnlButtons.add(btnStart);

        prgLoading.setEnabled(false);
        prgLoading.setStringPainted(true);
        pnlButtons.add(prgLoading);

        pnlMain.add(pnlButtons, java.awt.BorderLayout.PAGE_END);

        getContentPane().add(pnlMain, java.awt.BorderLayout.CENTER);

        pack();
        setLocationRelativeTo(null);
    }// </editor-fold>//GEN-END:initComponents

    
    private void btnStartActionPerformed(java.awt.event.ActionEvent evt)//GEN-FIRST:event_btnStartActionPerformed
    {//GEN-HEADEREND:event_btnStartActionPerformed
        if (downloader != null && downloader.isRunning())
        {
            downloader.stop();
            downloader = null;
        }
        else
        {
            downloader     = new Downloader();
            downloadThread = new Thread(downloader);
            downloadThread.start();
        }        
    }//GEN-LAST:event_btnStartActionPerformed

    
    private void btnDestinationActionPerformed(java.awt.event.ActionEvent evt)//GEN-FIRST:event_btnDestinationActionPerformed
    {//GEN-HEADEREND:event_btnDestinationActionPerformed
        File file = new File(txtDestination.getText());
        JFileChooser fc = new JFileChooser(file.getParent());
        fc.setMultiSelectionEnabled(false);
        fc.setDialogTitle("Select Destination File");
        fc.setDialogType(JFileChooser.SAVE_DIALOG);
        fc.setSelectedFile(file);
        int choice = fc.showSaveDialog(this);
        if ( choice == JFileChooser.APPROVE_OPTION )
        {
            txtDestination.setText(fc.getSelectedFile().toString());
        }
    }//GEN-LAST:event_btnDestinationActionPerformed

    
    private void cbxSourceItemStateChanged(java.awt.event.ItemEvent evt)//GEN-FIRST:event_cbxSourceItemStateChanged
    {//GEN-HEADEREND:event_cbxSourceItemStateChanged
        if (evt.getStateChange() == ItemEvent.SELECTED)
        {
            OnDataSourceChanged();
        }
    }//GEN-LAST:event_cbxSourceItemStateChanged

    
    private void OnDataSourceChanged()
    {
        DataSource ds = (DataSource) cbxSource.getSelectedItem();
        if (ds instanceof DataSource_GeoNet)
        {
            txtDestination.setText("./EQ_NZ_1900_2.csv");
            spnMagnitude.setValue(2.0f);
        }
        if (ds instanceof DataSource_USGS)
        {
            txtDestination.setText("./EQ_World_1900_4.csv");
            spnMagnitude.setValue(4.0f);
        }
    }
    
    
    /**
     * @param args the command line arguments
     */
    public static void main(String args[])
    {
        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable()
        {
            @Override
            public void run()
            {
                new EarthquakeDatasetReaderForm().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton btnDestination;
    private javax.swing.JButton btnStart;
    private javax.swing.JComboBox<String> cbxSource;
    private javax.swing.JFormattedTextField jFormattedTextField1;
    private javax.swing.JPanel pnlSettings;
    private javax.swing.JProgressBar prgLoading;
    private javax.swing.JSpinner spnEnd;
    private javax.swing.JSpinner spnMagnitude;
    private javax.swing.JSpinner spnStart;
    private javax.swing.JTextField txtDestination;
    // End of variables declaration//GEN-END:variables

    
    private class Downloader implements Runnable
    {
        @Override
        public void run()
        {
            if ( !execute )
            {
                execute = true;
                initialise();
                
                DataSource ds = (DataSource) cbxSource.getSelectedItem();
                
                Calendar calFrom = Calendar.getInstance(EarthquakeData.TIMEZONE); 
                calFrom.setTime((Date) spnStart.getValue());
                
                Calendar calTo = Calendar.getInstance(EarthquakeData.TIMEZONE); 
                calTo.setTime(calFrom.getTime());
                
                Calendar calEnd = Calendar.getInstance(EarthquakeData.TIMEZONE); 
                calEnd.setTime((Date) spnEnd.getValue());
                
                List<EarthquakeData> earthquakeList = new ArrayList<>();
                DateFormat fmt = new SimpleDateFormat("yyyy/MM");
                        
                DataSource.ProgressListener progressListener = (int lineCount) ->
                {
                    prgLoading.setString(fmt.format(calFrom.getTime()) + " / " + lineCount + " / " + earthquakeList.size());
                    return execute;
                };
                
                int  step    = 1;
                int  maxStep = 1024;
                
                while ( execute && (calFrom.compareTo(calEnd) < 0) )
                {
                    // calculate next from/to
                    calTo.setTime(calFrom.getTime());
                    calTo.add(Calendar.MONTH, step);
                    
                    Double minMag = (double)(Float) spnMagnitude.getValue();
                    URL url = ds.constructQuery(minMag.floatValue(), calFrom.getTime(), calTo.getTime());
                    try
                    {
                        List<EarthquakeData> list = ds.readSource(url, progressListener);
                        earthquakeList.addAll(list);
                        
                        if ((list.size() < ds.getMaximumItemsPerQuery() / 2) && (step < maxStep))
                        {
                            step *= 2;
                        }
                        if ( list.size() > ds.getMaximumItemsPerQuery() / 3 )
                        {
                            step /= 2;
                        }
                        // System.out.println(list.size() + " > " + step);

                        // success > next step
                        calFrom.setTime(calTo.getTime());
                    }
                    catch (ParseException | IOException e)
                    {
                        if ( e.getMessage().contains("HTTP") && e.getMessage().contains("400") )
                        {
                            // invalid query > reduce request range
                            step   /= 2;
                            maxStep = step;
                        }
                        else
                        {
                            // everything else: signal and terminate
                            System.err.println(e);
                            execute = false;
                        }
                    }
                }

                if ( execute )
                {
                    // sort by date
                    earthquakeList.sort(EarthquakeData.SORT_BY_DATE);

                    // determine extremes
                    minData = new EarthquakeData(earthquakeList.get(0));
                    maxData = new EarthquakeData(earthquakeList.get(0));

                    for (EarthquakeData eq : earthquakeList)
                    {
                        minData.checkMinimum(eq);
                        maxData.checkMaximum(eq);
                    }

                    try
                    {
                        prgLoading.setIndeterminate(false);
                        prgLoading.setValue(0);

                        PrintStream ps = new PrintStream(new File(txtDestination.getText()));
                        // print header + line count
                        ps.println(EarthquakeData.getCSV_Header() + "," + earthquakeList.size());
                        // print extremes
                        ps.println(minData.toCSV());
                        ps.println(maxData.toCSV());

                        int line       = 0;
                        int oldPercent = -1;
                        for (EarthquakeData eq : earthquakeList) 
                        {
                            ps.println(eq.toCSV());
                            line++;
                            int percent = line * 100 / earthquakeList.size();
                            if (percent != oldPercent)
                            {
                                oldPercent = percent;
                                prgLoading.setString(line + "/" + earthquakeList.size() + "(" + percent + "%)");
                                prgLoading.setValue(percent);
                            }
                        }

                        ps.close();
                        prgLoading.setValue(0);
                        prgLoading.setIndeterminate(true);
                        prgLoading.setString("");
                    }
                    catch (FileNotFoundException e)
                    {

                    }
                }
                
                terminate();
            }
        }

        
        private void initialise()
        {
            pnlSettings.setEnabled(false);
            btnStart.setText("Cancel");
            prgLoading.setEnabled(true);
            prgLoading.setString("");
            prgLoading.setIndeterminate(true);
        }
        
        
        private void terminate()
        {
            pnlSettings.setEnabled(true);
            btnStart.setText("Start Download");
            prgLoading.setEnabled(false);
            prgLoading.setString("");
            prgLoading.setIndeterminate(false);
        }
        
        
        public boolean isRunning()
        {
            return execute;
        }                    
        
        
        public void stop()
        {
            execute = false;
        }
        
        private volatile boolean execute;
    }
    
    
    private       Downloader     downloader;
    private       Thread         downloadThread;
    private final Date           minDate, maxDate;
    private       EarthquakeData minData, maxData;
}
