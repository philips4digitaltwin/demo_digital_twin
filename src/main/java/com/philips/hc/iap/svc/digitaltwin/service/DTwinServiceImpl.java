package com.philips.hc.iap.svc.digitaltwin.service;

import com.philips.hc.iap.svc.digitaltwin.model.*;
import com.philips.hc.iap.svc.digitaltwin.repository.*;
import com.philips.hc.iap.svc.digitaltwin.utilities.DTwinConstant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DTwinServiceImpl implements DTwinService{

    @Autowired
    private DTwinRepository dTwinRepository;

    @Autowired
    private DtwinLCCRepository dtwinLCCRepository;

    @Autowired
    private DtwinMagnetRepository dtwinMagnetRepository;





    @Override
    public DTwin saveDTwin(DTwin dTwin) {
        return null;
    }











    @Override
    public DtwinLCC saveDtwinLCC(DtwinLCC dtwinLCC) {
        DTwin twin = dTwinRepository.getDTwinByInstanceType(DTwinConstant.MR_INSTANCE_LCC);
        dtwinLCC.setdTwin(twin);
        dtwinLCCRepository.save(dtwinLCC);
        return dtwinLCC;
    }



    @Override
    public DtwinMagnet saveDtwinMagnet(DtwinMagnet dtwinMagnet) {
        DTwin twin = dTwinRepository.getDTwinByInstanceType(DTwinConstant.MR_INSTANCE_LCC);
        dtwinMagnet.setdTwin(twin);
        dtwinMagnetRepository.save(dtwinMagnet);
        return dtwinMagnet;
    }
}
