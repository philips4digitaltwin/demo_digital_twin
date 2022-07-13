package com.philips.hc.iap.svc.digitaltwin.model;

import javax.persistence.*;
import java.sql.Timestamp;
@Entity
//@Table(name="dtwin_battery", schema="digital_twin")
@Table(name="dtwin_battery", schema="dbo")
public class DTwinBattery {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int dTwinInstanceBatteryID;
    @Column(name="dtwin_instance_id")
    private String dTwinDetectorID;
    @ManyToOne
    @JoinColumn(name="dtwin_id" , nullable = false)
    private DTwin dTwin;
    @Column(name="batteryserialnumber")
    private String batterySerialNumber;
    @Column(name="remaininglifetime")
    private Timestamp remainingLifetime;
    @Column(name="remaininglifetimedays")
    private int remainingLifetimeDays;
    @Column(name="ischargingcyclesexceeded")
    private boolean isChargingCyclesExceeded;
    @Column(name="numberofchargingcycles")
    private int numberOfChargingCycles;

    public DTwinBattery(String dTwinDetectorID, DTwin dTwin, String batterySerialNumber, Timestamp remainingLifetime, int remainingLifetimeDays, boolean isChargingCyclesExceeded, int numberOfChargingCycles) {
        this.dTwinDetectorID = dTwinDetectorID;
        this.dTwin = dTwin;
        this.batterySerialNumber = batterySerialNumber;
        this.remainingLifetime = remainingLifetime;
        this.remainingLifetimeDays = remainingLifetimeDays;
        this.isChargingCyclesExceeded = isChargingCyclesExceeded;
        this.numberOfChargingCycles = numberOfChargingCycles;
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

    public String getBatterySerialNumber() {
        return batterySerialNumber;
    }

    public void setBatterySerialNumber(String batterySerialNumber) {
        this.batterySerialNumber = batterySerialNumber;
    }

    public Timestamp getRemainingLifetime() {
        return remainingLifetime;
    }

    public void setRemainingLifetime(Timestamp remainingLifetime) {
        this.remainingLifetime = remainingLifetime;
    }

    public int getRemainingLifetimeDays() {
        return remainingLifetimeDays;
    }

    public void setRemainingLifetimeDays(int remainingLifetimeDays) {
        this.remainingLifetimeDays = remainingLifetimeDays;
    }

    public boolean isChargingCyclesExceeded() {
        return isChargingCyclesExceeded;
    }

    public void setChargingCyclesExceeded(boolean chargingCyclesExceeded) {
        isChargingCyclesExceeded = chargingCyclesExceeded;
    }

    public int getNumberOfChargingCycles() {
        return numberOfChargingCycles;
    }

    public void setNumberOfChargingCycles(int numberOfChargingCycles) {
        this.numberOfChargingCycles = numberOfChargingCycles;
    }
}
