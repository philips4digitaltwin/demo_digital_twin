package com.philips.hc.iap.svc.digitaltwin.model;

import javax.persistence.*;
import java.sql.Timestamp;

@Entity
@Table(name="dtwin_lcc", schema="dbo")
public class DtwinLCC {


    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int dTwinInstanceLCCID;

    private String  dtwin_instance_id;

    private double LCC_GradAmpFilterPress ;

    private double  LCC_GradAmpPress;

    private double  LCC_GradCoilFilterPress;

    private double  LCC_GradCoilPress ;

    private double  LCC_GradCoilTemp;

    private double  LCC_SecondaryLoopFillingPress;

    private double  LCC_SecondaryLoopTemp;

    private double  LCC_T1Temp;

    private double  LCC_T2Temp;

    private double  LCC_GradAmpFillingPress;

    private double  LCC_GradAmpTemp;

    private double  LCC_GradCoilFillingPress;

    private double  LCC_HeliumCompressorFlow;

    private double  LCC_PrimaryFlow;

    private double  LCC_HCTemp;

    private double  LCC_HeliumCompressorTemp;

    private String  LCC_Type;

    private Timestamp EventDate;



    private String SRN;

    @ManyToOne
    @JoinColumn(name="dtwin_id" , nullable = false)
    private DTwin dTwin;

    public DtwinLCC(int dTwinInstanceLCCID, String dtwin_instance_id, double LCC_GradAmpFilterPress, double LCC_GradAmpPress, double LCC_GradCoilFilterPress, double LCC_GradCoilPress, double LCC_GradCoilTemp, double LCC_SecondaryLoopFillingPress, double LCC_SecondaryLoopTemp, double LCC_T1Temp, double LCC_T2Temp, double LCC_GradAmpFillingPress, double LCC_GradAmpTemp, double LCC_GradCoilFillingPress, double LCC_HeliumCompressorFlow, double LCC_PrimaryFlow, double LCC_HCTemp, double LCC_HeliumCompressorTemp, String LCC_Type, Timestamp eventDate, String SRN, DTwin dTwin) {
        this.dTwinInstanceLCCID = dTwinInstanceLCCID;
        this.dtwin_instance_id = dtwin_instance_id;
        this.LCC_GradAmpFilterPress = LCC_GradAmpFilterPress;
        this.LCC_GradAmpPress = LCC_GradAmpPress;
        this.LCC_GradCoilFilterPress = LCC_GradCoilFilterPress;
        this.LCC_GradCoilPress = LCC_GradCoilPress;
        this.LCC_GradCoilTemp = LCC_GradCoilTemp;
        this.LCC_SecondaryLoopFillingPress = LCC_SecondaryLoopFillingPress;
        this.LCC_SecondaryLoopTemp = LCC_SecondaryLoopTemp;
        this.LCC_T1Temp = LCC_T1Temp;
        this.LCC_T2Temp = LCC_T2Temp;
        this.LCC_GradAmpFillingPress = LCC_GradAmpFillingPress;
        this.LCC_GradAmpTemp = LCC_GradAmpTemp;
        this.LCC_GradCoilFillingPress = LCC_GradCoilFillingPress;
        this.LCC_HeliumCompressorFlow = LCC_HeliumCompressorFlow;
        this.LCC_PrimaryFlow = LCC_PrimaryFlow;
        this.LCC_HCTemp = LCC_HCTemp;
        this.LCC_HeliumCompressorTemp = LCC_HeliumCompressorTemp;
        this.LCC_Type = LCC_Type;
        EventDate = eventDate;
        this.SRN = SRN;
        this.dTwin = dTwin;
    }

    public String getLCC_Type() {
        return LCC_Type;
    }

    public void setLCC_Type(String LCC_Type) {
        this.LCC_Type = LCC_Type;
    }

    public int getdTwinInstanceLCCID() {
        return dTwinInstanceLCCID;
    }

    public void setdTwinInstanceLCCID(int dTwinInstanceLCCID) {
        this.dTwinInstanceLCCID = dTwinInstanceLCCID;
    }

    public DtwinLCC() {

    }

    public double getLCC_GradAmpFilterPress() {
        return LCC_GradAmpFilterPress;
    }

    public void setLCC_GradAmpFilterPress(double LCC_GradAmpFilterPress) {
        this.LCC_GradAmpFilterPress = LCC_GradAmpFilterPress;
    }

    public double getLCC_GradAmpPress() {
        return LCC_GradAmpPress;
    }

    public void setLCC_GradAmpPress(double LCC_GradAmpPress) {
        this.LCC_GradAmpPress = LCC_GradAmpPress;
    }

    public double getLCC_GradCoilFilterPress() {
        return LCC_GradCoilFilterPress;
    }

    public void setLCC_GradCoilFilterPress(double LCC_GradCoilFilterPress) {
        this.LCC_GradCoilFilterPress = LCC_GradCoilFilterPress;
    }

    public double getLCC_GradCoilPress() {
        return LCC_GradCoilPress;
    }

    public void setLCC_GradCoilPress(double LCC_GradCoilPress) {
        this.LCC_GradCoilPress = LCC_GradCoilPress;
    }

    public double getLCC_GradCoilTemp() {
        return LCC_GradCoilTemp;
    }

    public void setLCC_GradCoilTemp(double LCC_GradCoilTemp) {
        this.LCC_GradCoilTemp = LCC_GradCoilTemp;
    }

    public double getLCC_SecondaryLoopFillingPress() {
        return LCC_SecondaryLoopFillingPress;
    }

    public void setLCC_SecondaryLoopFillingPress(double LCC_SecondaryLoopFillingPress) {
        this.LCC_SecondaryLoopFillingPress = LCC_SecondaryLoopFillingPress;
    }

    public double getLCC_SecondaryLoopTemp() {
        return LCC_SecondaryLoopTemp;
    }

    public void setLCC_SecondaryLoopTemp(double LCC_SecondaryLoopTemp) {
        this.LCC_SecondaryLoopTemp = LCC_SecondaryLoopTemp;
    }

    public double getLCC_T1Temp() {
        return LCC_T1Temp;
    }

    public void setLCC_T1Temp(double LCC_T1Temp) {
        this.LCC_T1Temp = LCC_T1Temp;
    }

    public double getLCC_T2Temp() {
        return LCC_T2Temp;
    }

    public void setLCC_T2Temp(double LCC_T2Temp) {
        this.LCC_T2Temp = LCC_T2Temp;
    }

    public double getLCC_GradAmpFillingPress() {
        return LCC_GradAmpFillingPress;
    }

    public void setLCC_GradAmpFillingPress(double LCC_GradAmpFillingPress) {
        this.LCC_GradAmpFillingPress = LCC_GradAmpFillingPress;
    }

    public double getLCC_GradAmpTemp() {
        return LCC_GradAmpTemp;
    }

    public void setLCC_GradAmpTemp(double LCC_GradAmpTemp) {
        this.LCC_GradAmpTemp = LCC_GradAmpTemp;
    }

    public double getLCC_GradCoilFillingPress() {
        return LCC_GradCoilFillingPress;
    }

    public void setLCC_GradCoilFillingPress(double LCC_GradCoilFillingPress) {
        this.LCC_GradCoilFillingPress = LCC_GradCoilFillingPress;
    }

    public double getLCC_HeliumCompressorFlow() {
        return LCC_HeliumCompressorFlow;
    }

    public void setLCC_HeliumCompressorFlow(double LCC_HeliumCompressorFlow) {
        this.LCC_HeliumCompressorFlow = LCC_HeliumCompressorFlow;
    }

    public double getLCC_PrimaryFlow() {
        return LCC_PrimaryFlow;
    }

    public void setLCC_PrimaryFlow(double LCC_PrimaryFlow) {
        this.LCC_PrimaryFlow = LCC_PrimaryFlow;
    }

    public double getLCC_HCTemp() {
        return LCC_HCTemp;
    }

    public void setLCC_HCTemp(double LCC_HCTemp) {
        this.LCC_HCTemp = LCC_HCTemp;
    }

    public double getLCC_HeliumCompressorTemp() {
        return LCC_HeliumCompressorTemp;
    }

    public void setLCC_HeliumCompressorTemp(double LCC_HeliumCompressorTemp) {
        this.LCC_HeliumCompressorTemp = LCC_HeliumCompressorTemp;
    }



    public Timestamp getEventDate() {
        return EventDate;
    }

    public void setEventDate(Timestamp eventDate) {
        EventDate = eventDate;
    }

    public String getDtwin_instance_id() {
        return dtwin_instance_id;
    }

    public void setDtwin_instance_id(String dtwin_instance_id) {
        this.dtwin_instance_id = dtwin_instance_id;
    }

    public String getSRN() {
        return SRN;
    }

    public void setSRN(String SRN) {
        this.SRN = SRN;
    }

    public DTwin getdTwin() {
        return dTwin;
    }

    public void setdTwin(DTwin dTwin) {
        this.dTwin = dTwin;
    }
}
