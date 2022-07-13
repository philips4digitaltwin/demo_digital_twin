package com.philips.hc.iap.svc.digitaltwin.model;

import javax.persistence.*;
import java.sql.Timestamp;

@Entity
//@Table(name="dtwin_detector", schema="digital_twin")
@Table(name="dtwin_detector", schema="dbo")
public class DTwinDetector {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int dTwinInstanceDetectorID;

    @Column(name="dtwin_instance_id")
    private String dtwin_instance_id;

    @ManyToOne
    @JoinColumn(name="dtwin_id" , nullable = false)
    private DTwin dTwin;
    @Column(name="detectortypeid")
    private String detectorTypeID;
    @Column(name="lastattdt")
    private Timestamp lastAttDt;

    @Column(name="cumulative_op_time")
    private String cumulative_op_time;
    @Column(name="power_on_cycles")
    private String power_on_cycles;

    private String  detectorTemp;

    private String calibrationTemp;

    private Timestamp eventDateTime;



    public DTwinDetector() {

    }


    public DTwinDetector(int dTwinInstanceDetectorID, String dtwin_instance_id, DTwin dTwin, String detectorTypeID, Timestamp lastAttDt, String cumulative_op_time, String power_on_cycles, String detectorTemp, String calibrationTemp, Timestamp eventDateTime) {
        this.dTwinInstanceDetectorID = dTwinInstanceDetectorID;
        this.dtwin_instance_id = dtwin_instance_id;
        this.dTwin = dTwin;
        this.detectorTypeID = detectorTypeID;
        this.lastAttDt = lastAttDt;
        this.cumulative_op_time = cumulative_op_time;
        this.power_on_cycles = power_on_cycles;
        this.detectorTemp = detectorTemp;
        this.calibrationTemp = calibrationTemp;
        this.eventDateTime = eventDateTime;
    }

    public String getCumulative_op_time() {
        return cumulative_op_time;
    }

    public void setCumulative_op_time(String cumulative_op_time) {
        this.cumulative_op_time = cumulative_op_time;
    }

    public String getPower_on_cycles() {
        return power_on_cycles;
    }

    public void setPower_on_cycles(String power_on_cycles) {
        this.power_on_cycles = power_on_cycles;
    }

    public String getDetectorTemp() {
        return detectorTemp;
    }

    public void setDetectorTemp(String detectorTemp) {
        this.detectorTemp = detectorTemp;
    }

    public String getCalibrationTemp() {
        return calibrationTemp;
    }

    public void setCalibrationTemp(String calibrationTemp) {
        this.calibrationTemp = calibrationTemp;
    }

    public Timestamp getEventDateTime() {
        return eventDateTime;
    }

    public void setEventDateTime(Timestamp eventDateTime) {
        this.eventDateTime = eventDateTime;
    }

    public String getDtwin_instance_id() {
        return dtwin_instance_id;
    }

    public void setDtwin_instance_id(String dtwin_instance_id) {
        this.dtwin_instance_id = dtwin_instance_id;
    }





    public int getdTwinInstanceDetectorID() {
        return dTwinInstanceDetectorID;
    }

    public void setdTwinInstanceDetectorID(int dTwinInstanceDetectorID) {
        this.dTwinInstanceDetectorID = dTwinInstanceDetectorID;
    }




    public DTwin getdTwin() {
        return dTwin;
    }

    public void setdTwin(DTwin dTwin) {
        this.dTwin = dTwin;
    }

    public String getDetectorTypeID() {
        return detectorTypeID;
    }

    public void setDetectorTypeID(String detectorTypeID) {
        this.detectorTypeID = detectorTypeID;
    }

    public Timestamp getLastAttDt() {
        return lastAttDt;
    }

    public void setLastAttDt(Timestamp lastAttDt) {
        this.lastAttDt = lastAttDt;
    }








}
