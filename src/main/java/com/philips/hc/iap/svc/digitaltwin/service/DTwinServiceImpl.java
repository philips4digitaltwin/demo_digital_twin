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

    @Autowired
    private DtwinDetectorWIFIRepository dtwinDetectorWIFIRepository;

    @Autowired
    private DTwinDetectorRepository dTwinDetectorRepository;

    @Autowired
    private DTwinDetectorCalibrationRepository dTwinDetectorCalibrationRepository;

    @Autowired
    private DTwinBatteryRepository dTwinBatteryRepository;



    @Override
    public DTwin saveDTwin(DTwin dTwin) {
        return null;
    }

    @Override
    public DTwinBattery saveDTwinBattery(DTwinBattery dTwinBattery) {
        DTwin twin = dTwinRepository.getDTwinByInstanceType(DTwinConstant.DXR_INSTANCE_BATTERY);
        dTwinBattery.setdTwin(twin);
        dTwinBatteryRepository.save(dTwinBattery);
        return dTwinBattery;
    }

    @Override
    public DTwinDetector saveDTwinDetector(DTwinDetector dTwinDetector) {

        DTwin twin = dTwinRepository.getDTwinByInstanceType(DTwinConstant.DXR_INSTANCE_DETECTOR);
        dTwinDetector.setdTwin(twin);
        dTwinDetectorRepository.save(dTwinDetector);
        return dTwinDetector;

    }

    @Override
    public DtwinDetectorWIFI saveDtwinDetectorWIFI(DtwinDetectorWIFI dtwinDetectorWIFI) {
        DTwin twin = dTwinRepository.getDTwinByInstanceType(DTwinConstant.DXR_INSTANCE_DETECTOR_WIFI);
        dtwinDetectorWIFI.setdTwin(twin);
        dtwinDetectorWIFIRepository.save(dtwinDetectorWIFI);
        return dtwinDetectorWIFI;
    }




    @Override
    public DtwinLCC saveDtwinLCC(DtwinLCC dtwinLCC) {
        DTwin twin = dTwinRepository.getDTwinByInstanceType(DTwinConstant.MR_INSTANCE_LCC);
        dtwinLCC.setdTwin(twin);
        dtwinLCCRepository.save(dtwinLCC);
        return dtwinLCC;
    }

    @Override
    public DTwinDetectorCalibration saveDTwinDetectorCalibration(DTwinDetectorCalibration dTwinDetectorCalibration) {
        DTwin twin = dTwinRepository.getDTwinByInstanceType(DTwinConstant.DXR_INSTANCE_DETECTOR_CALIBRATION);
        dTwinDetectorCalibration.setdTwin(twin);
        dTwinDetectorCalibrationRepository.save(dTwinDetectorCalibration);
        return dTwinDetectorCalibration;
    }

    @Override
    public DtwinMagnet saveDtwinMagnet(DtwinMagnet dtwinMagnet) {
        DTwin twin = dTwinRepository.getDTwinByInstanceType(DTwinConstant.MR_INSTANCE_LCC);
        dtwinMagnet.setdTwin(twin);
        dtwinMagnetRepository.save(dtwinMagnet);
        return dtwinMagnet;
    }
}
