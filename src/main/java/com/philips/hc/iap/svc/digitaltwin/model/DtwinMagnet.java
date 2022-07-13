package com.philips.hc.iap.svc.digitaltwin.model;

import javax.persistence.*;
import java.sql.Timestamp;


@Entity
@Table(name="dtwin_magnet", schema="dbo")
public class DtwinMagnet {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int dTwinInstanceMagnetID;

    private String  dtwin_instance_id;
    private double  magnet_pressure;

    private double magnet_bathheaterlow;

    private double  magnet_helium_level_value;

    private double magnet_quench;

    private String  Magnet_Type;

    private String Main_System_Type;

    private String  Magnet_Serial_Number;

    private String  Magnet_MMU_Type;

    private Timestamp EventDate ;



    private String InstanceType;

    private String SRN;

    @ManyToOne
    @JoinColumn(name="dtwin_id" , nullable = false)
    private DTwin dTwin;


    public DtwinMagnet() {

    }

    public DtwinMagnet(int dTwinInstanceMagnetID, String dtwin_instance_id, double magnet_pressure, double magnet_bathheaterlow, double magnet_helium_level_value, double magnet_quench, String magnet_Type, String main_System_Type, String magnet_Serial_Number, String magnet_MMU_Type, Timestamp eventDate, String instanceType, String SRN, DTwin dTwin) {
        this.dTwinInstanceMagnetID = dTwinInstanceMagnetID;
        this.dtwin_instance_id = dtwin_instance_id;
        this.magnet_pressure = magnet_pressure;
        this.magnet_bathheaterlow = magnet_bathheaterlow;
        this.magnet_helium_level_value = magnet_helium_level_value;
        this.magnet_quench = magnet_quench;
        Magnet_Type = magnet_Type;
        Main_System_Type = main_System_Type;
        Magnet_Serial_Number = magnet_Serial_Number;
        Magnet_MMU_Type = magnet_MMU_Type;
        EventDate = eventDate;
        InstanceType = instanceType;
        this.SRN = SRN;
        this.dTwin = dTwin;
    }

    public int getdTwinInstanceMagnetID() {
        return dTwinInstanceMagnetID;
    }

    public void setdTwinInstanceMagnetID(int dTwinInstanceMagnetID) {
        this.dTwinInstanceMagnetID = dTwinInstanceMagnetID;
    }

    public String getDtwin_instance_id() {
        return dtwin_instance_id;
    }

    public void setDtwin_instance_id(String dtwin_instance_id) {
        this.dtwin_instance_id = dtwin_instance_id;
    }

    public double getMagnet_pressure() {
        return magnet_pressure;
    }

    public void setMagnet_pressure(double magnet_pressure) {
        this.magnet_pressure = magnet_pressure;
    }

    public double getMagnet_bathheaterlow() {
        return magnet_bathheaterlow;
    }

    public void setMagnet_bathheaterlow(double magnet_bathheaterlow) {
        this.magnet_bathheaterlow = magnet_bathheaterlow;
    }

    public double getMagnet_helium_level_value() {
        return magnet_helium_level_value;
    }

    public void setMagnet_helium_level_value(double magnet_helium_level_value) {
        this.magnet_helium_level_value = magnet_helium_level_value;
    }

    public double getMagnet_quench() {
        return magnet_quench;
    }

    public void setMagnet_quench(double magnet_quench) {
        this.magnet_quench = magnet_quench;
    }

    public String getMagnet_Type() {
        return Magnet_Type;
    }

    public void setMagnet_Type(String magnet_Type) {
        Magnet_Type = magnet_Type;
    }

    public String getMain_System_Type() {
        return Main_System_Type;
    }

    public void setMain_System_Type(String main_System_Type) {
        Main_System_Type = main_System_Type;
    }

    public String getMagnet_Serial_Number() {
        return Magnet_Serial_Number;
    }

    public void setMagnet_Serial_Number(String magnet_Serial_Number) {
        Magnet_Serial_Number = magnet_Serial_Number;
    }

    public String getMagnet_MMU_Type() {
        return Magnet_MMU_Type;
    }

    public void setMagnet_MMU_Type(String magnet_MMU_Type) {
        Magnet_MMU_Type = magnet_MMU_Type;
    }

    public Timestamp getEventDate() {
        return EventDate;
    }

    public void setEventDate(Timestamp eventDate) {
        EventDate = eventDate;
    }

    public String getInstanceType() {
        return InstanceType;
    }

    public void setInstanceType(String instanceType) {
        InstanceType = instanceType;
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
