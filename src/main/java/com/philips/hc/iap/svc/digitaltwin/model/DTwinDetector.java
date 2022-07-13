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
    private String dTwinDetectorID;

    @ManyToOne
    @JoinColumn(name="dtwin_id" , nullable = false)
    private DTwin dTwin;
    @Column(name="detectortypeid")
    private String detectorTypeID;
    @Column(name="lastattdt")
    private Timestamp lastAttDt;
    @Column(name="cumulative_op_time")
    private int cumulativeOPTime;
    @Column(name="power_on_cycles")
    private int powerOnCycles;

    public DTwinDetector(int dTwinInstanceDetectorID, String dTwinDetectorID, DTwin dTwin, String detectorTypeID, Timestamp lastAttDt, int cumulativeOPTime, int powerOnCycles) {
        this.dTwinInstanceDetectorID = dTwinInstanceDetectorID;
        this.dTwinDetectorID = dTwinDetectorID;
        this.dTwin = dTwin;
        this.detectorTypeID = detectorTypeID;
        this.lastAttDt = lastAttDt;
        this.cumulativeOPTime = cumulativeOPTime;
        this.powerOnCycles = powerOnCycles;
    }

    public DTwinDetector() {

    }

    public int getdTwinInstanceDetectorID() {
        return dTwinInstanceDetectorID;
    }

    public void setdTwinInstanceDetectorID(int dTwinInstanceDetectorID) {
        this.dTwinInstanceDetectorID = dTwinInstanceDetectorID;
    }

    public String getdTwinDetectorID() {
        return dTwinDetectorID;
    }

    public void setdTwinDetectorID(String dTwinDetectorID) {
        this.dTwinDetectorID = dTwinDetectorID;
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

    public int getCumulativeOPTime() {
        return cumulativeOPTime;
    }

    public void setCumulativeOPTime(int cumulativeOPTime) {
        this.cumulativeOPTime = cumulativeOPTime;
    }

    public int getPowerOnCycles() {
        return powerOnCycles;
    }

    public void setPowerOnCycles(int powerOnCycles) {
        this.powerOnCycles = powerOnCycles;
    }
}
