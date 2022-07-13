package com.philips.hc.iap.svc.digitaltwin.model;

import javax.persistence.*;

@Entity
@Table(name="dtwin_detector_wifi", schema="dbo")
public class DtwinDetectorWIFI {
    @Column(name="dtwin_instance_wifi_id")
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int dTwinInstanceWIFIID;
    @ManyToOne
    @JoinColumn(name="dtwin_instance_id" , nullable = false)
    private DTwinDetector dTwinDetector;
    @Column(name="linkquality")
    private int linkQuality;
    @Column(name="signalpower")
    private int signalPower;
    @Column(name="datarate")
    private int dataRate;
    @Column(name="noisepower")
    private int noisePower;
    @Column(name="rawlinkquality")
    private int rawLinkQuality;

    @ManyToOne
    @JoinColumn(name="dtwin_id" , nullable = false)
    private DTwin dTwin;

    public DtwinDetectorWIFI(int dTwinInstanceWIFIID, DTwinDetector dTwinDetector, int linkQuality, int signalPower, int dataRate, int noisePower, int rawLinkQuality, DTwin dTwin) {
        this.dTwinInstanceWIFIID = dTwinInstanceWIFIID;
        this.dTwinDetector = dTwinDetector;
        this.linkQuality = linkQuality;
        this.signalPower = signalPower;
        this.dataRate = dataRate;
        this.noisePower = noisePower;
        this.rawLinkQuality = rawLinkQuality;
        this.dTwin = dTwin;
    }

    public int getdTwinInstanceWIFIID() {
        return dTwinInstanceWIFIID;
    }

    public void setdTwinInstanceWIFIID(int dTwinInstanceWIFIID) {
        this.dTwinInstanceWIFIID = dTwinInstanceWIFIID;
    }

    public DTwinDetector getdTwinDetector() {
        return dTwinDetector;
    }

    public void setdTwinDetector(DTwinDetector dTwinDetector) {
        this.dTwinDetector = dTwinDetector;
    }

    public int getLinkQuality() {
        return linkQuality;
    }

    public void setLinkQuality(int linkQuality) {
        this.linkQuality = linkQuality;
    }

    public int getSignalPower() {
        return signalPower;
    }

    public void setSignalPower(int signalPower) {
        this.signalPower = signalPower;
    }

    public int getDataRate() {
        return dataRate;
    }

    public void setDataRate(int dataRate) {
        this.dataRate = dataRate;
    }

    public int getNoisePower() {
        return noisePower;
    }

    public void setNoisePower(int noisePower) {
        this.noisePower = noisePower;
    }

    public int getRawLinkQuality() {
        return rawLinkQuality;
    }

    public void setRawLinkQuality(int rawLinkQuality) {
        this.rawLinkQuality = rawLinkQuality;
    }

    public DTwin getdTwin() {
        return dTwin;
    }

    public void setdTwin(DTwin dTwin) {
        this.dTwin = dTwin;
    }
}
