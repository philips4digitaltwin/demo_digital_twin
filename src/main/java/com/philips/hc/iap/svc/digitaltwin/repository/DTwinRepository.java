package com.philips.hc.iap.svc.digitaltwin.repository;

import com.philips.hc.iap.svc.digitaltwin.model.DTwin;
import org.springframework.data.repository.CrudRepository;

public interface DTwinRepository extends CrudRepository<DTwin , String> {

    public DTwin getDTwinByInstanceType(String InstanceType);
}
