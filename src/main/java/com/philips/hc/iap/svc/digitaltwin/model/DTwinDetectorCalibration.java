package com.philips.hc.iap.svc.digitaltwin.model;

import javax.persistence.*;
import java.sql.Timestamp;
@Entity
@Table(name="dtwin_detector_calibration", schema="dbo")
public class DTwinDetectorCalibration {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int dTwinInstanceDetectorCalibrationID;
    private String dtwin_instance_id;

    private double  detectorTemp;
    private double calibrationTemp;
    private double softLowLimitTemp;
    private double softHighLimitTemp;
    private double hardLowLimitTemp;
    private double hardHighLimitTemp;
    private double tempDev;
    private int calibrationPeriod;
    private Timestamp latestCalibration;

    private Timestamp calibrationDueDate;
    private int   timeToRecalibrate;
    private Timestamp  eventDate;

    private Timestamp eventDateTime;

    @ManyToOne
    @JoinColumn(name="dtwin_id" , nullable = false)
    private DTwin dTwin;

    public DTwinDetectorCalibration(int dTwinInstanceDetectorCalibrationID, String dtwin_instance_id, double detectorTemp, double calibrationTemp, double softLowLimitTemp, double softHighLimitTemp, double hardLowLimitTemp, double hardHighLimitTemp, double tempDev, int calibrationPeriod, Timestamp latestCalibration, Timestamp calibrationDueDate, int timeToRecalibrate, Timestamp eventDate, Timestamp eventDateTime, DTwin dTwin) {
        this.dTwinInstanceDetectorCalibrationID = dTwinInstanceDetectorCalibrationID;
        this.dtwin_instance_id = dtwin_instance_id;
        this.detectorTemp = detectorTemp;
        this.calibrationTemp = calibrationTemp;
        this.softLowLimitTemp = softLowLimitTemp;
        this.softHighLimitTemp = softHighLimitTemp;
        this.hardLowLimitTemp = hardLowLimitTemp;
        this.hardHighLimitTemp = hardHighLimitTemp;
        this.tempDev = tempDev;
        this.calibrationPeriod = calibrationPeriod;
        this.latestCalibration = latestCalibration;
        this.calibrationDueDate = calibrationDueDate;
        this.timeToRecalibrate = timeToRecalibrate;
        this.eventDate = eventDate;
        this.eventDateTime = eventDateTime;
        this.dTwin = dTwin;
    }

    public Timestamp getEventDateTime() {
        return eventDateTime;
    }

    public void setEventDateTime(Timestamp eventDateTime) {
        this.eventDateTime = eventDateTime;
    }

    public DTwin getdTwin() {
        return dTwin;
    }

    public void setdTwin(DTwin dTwin) {
        this.dTwin = dTwin;
    }

    public int getdTwinInstanceDetectorCalibrationID() {
        return dTwinInstanceDetectorCalibrationID;
    }

    public void setdTwinInstanceDetectorCalibrationID(int dTwinInstanceDetectorCalibrationID) {
        this.dTwinInstanceDetectorCalibrationID = dTwinInstanceDetectorCalibrationID;
    }

    public DTwinDetectorCalibration() {
    }

    public String getDtwin_instance_id() {
        return dtwin_instance_id;
    }

    public void setDtwin_instance_id(String dtwin_instance_id) {
        this.dtwin_instance_id = dtwin_instance_id;
    }

    public double getDetectorTemp() {
        return detectorTemp;
    }

    public void setDetectorTemp(double detectorTemp) {
        this.detectorTemp = detectorTemp;
    }

    public double getCalibrationTemp() {
        return calibrationTemp;
    }

    public void setCalibrationTemp(double calibrationTemp) {
        this.calibrationTemp = calibrationTemp;
    }

    public double getSoftLowLimitTemp() {
        return softLowLimitTemp;
    }

    public void setSoftLowLimitTemp(double softLowLimitTemp) {
        this.softLowLimitTemp = softLowLimitTemp;
    }

    public double getSoftHighLimitTemp() {
        return softHighLimitTemp;
    }

    public void setSoftHighLimitTemp(double softHighLimitTemp) {
        this.softHighLimitTemp = softHighLimitTemp;
    }

    public double getHardLowLimitTemp() {
        return hardLowLimitTemp;
    }

    public void setHardLowLimitTemp(double hardLowLimitTemp) {
        this.hardLowLimitTemp = hardLowLimitTemp;
    }

    public double getHardHighLimitTemp() {
        return hardHighLimitTemp;
    }

    public void setHardHighLimitTemp(double hardHighLimitTemp) {
        this.hardHighLimitTemp = hardHighLimitTemp;
    }

    public double getTempDev() {
        return tempDev;
    }

    public void setTempDev(double tempDev) {
        this.tempDev = tempDev;
    }

    public int getCalibrationPeriod() {
        return calibrationPeriod;
    }

    public void setCalibrationPeriod(int calibrationPeriod) {
        this.calibrationPeriod = calibrationPeriod;
    }

    public Timestamp getLatestCalibration() {
        return latestCalibration;
    }

    public void setLatestCalibration(Timestamp latestCalibration) {
        this.latestCalibration = latestCalibration;
    }

    public Timestamp getCalibrationDueDate() {
        return calibrationDueDate;
    }

    public void setCalibrationDueDate(Timestamp calibrationDueDate) {
        this.calibrationDueDate = calibrationDueDate;
    }

    public int getTimeToRecalibrate() {
        return timeToRecalibrate;
    }

    public void setTimeToRecalibrate(int timeToRecalibrate) {
        this.timeToRecalibrate = timeToRecalibrate;
    }

    public Timestamp getEventDate() {
        return eventDate;
    }

    public void setEventDate(Timestamp eventDate) {
        this.eventDate = eventDate;
    }
}
