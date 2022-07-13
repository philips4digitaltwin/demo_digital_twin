package com.philips.hc.iap.svc.digitaltwin.model;
//import javax.persistence.Entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

//@Enti
@Entity
//@Table(name="dtwin_master", schema="digital_twin")
@Table(name="dtwin_master", schema="dbo")
public class DTwin {

    @Column(name="dtwin_id")
    @Id
    private String dtwin_id;

    @Column(name="dtwin_name")
    private String instanceType;

    public DTwin() {

    }

    public DTwin(String dtwin_id, String instanceType) {
        this.dtwin_id = dtwin_id;
        this.instanceType = instanceType;
    }

    public String getDtwin_id() {
        return dtwin_id;
    }

    public void setDtwin_id(String dtwin_id) {
        this.dtwin_id = dtwin_id;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public void setInstanceType(String instanceType) {
        this.instanceType = instanceType;
    }
}
