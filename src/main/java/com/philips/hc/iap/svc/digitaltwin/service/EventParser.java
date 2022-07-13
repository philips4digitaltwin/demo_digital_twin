package com.philips.hc.iap.svc.digitaltwin.service;

import com.philips.hc.iap.svc.digitaltwin.model.DTwinDetector;
import com.philips.hc.iap.svc.digitaltwin.model.DtwinLCC;
import com.philips.hc.iap.svc.digitaltwin.model.DtwinMagnet;
import com.philips.hc.iap.svc.digitaltwin.utilities.DTwinConstant;
import org.json.JSONArray;
import org.json.JSONObject;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.Locale;

@Service
public class EventParser {

  @Autowired
  DTwinService dTwinService;

  public void parseEvent(String eventData) {
    JSONObject json = new JSONObject(eventData);
    JSONObject newJS = new JSONObject();
    replaceKey(json , newJS);
    System.out.println("newJS: "+newJS.toString());


 //   System.out.println("Instance type : "+newJS.getString(DTwinConstant.INSTANCE_TYPE));
     switch (newJS.getString(DTwinConstant.INSTANCE_TYPE)){
       case DTwinConstant.MR_INSTANCE_LCC:
         DtwinLCC dtwinLCC = new DtwinLCC();
         dtwinLCC = json2Java(newJS.toString(), DtwinLCC.class);
         dTwinService.saveDtwinLCC(dtwinLCC);
         break;
       case DTwinConstant.MR_INSTANCE_MAGNET:
         DtwinMagnet dtwinMagnet = new DtwinMagnet();
         dtwinMagnet = json2Java(newJS.toString(), DtwinMagnet.class);
         dTwinService.saveDtwinMagnet(dtwinMagnet);
         break;
       case DTwinConstant.DXR_INSTANCE_DETECTOR:
         DTwinDetector dTwinDetector = new DTwinDetector();
         dTwinDetector = json2Java(newJS.toString(), DTwinDetector.class);
         dTwinService.saveDTwinDetector(dTwinDetector);
         break;

       default:
         break;

  }

}

    public static <T> T json2Java(String content, Class<T> classType){

      T t = null;
    //  File file = new File("src/main/resources/"+fileName);

      try {
        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        t=mapper.readValue(content, classType);
      }catch (Exception e) {
        e.printStackTrace();
      }

      return t;
    }

    public void replaceKey(JSONObject json,JSONObject newJS){

      json.keys().forEachRemaining(key -> {
          String  ky = null;

        if(key.contains("_")){
         ky =   key.substring(0,key.indexOf("_")).toLowerCase(Locale.ROOT) +key.substring(key.indexOf("_"));
          System.out.println("Substring : "+ky);
        }else if(key.length()<= 3){

            ky = key.toLowerCase(Locale.ROOT);
        }else{
          ky = Character.toLowerCase(key.charAt(0)) + key.substring(1);
        }
       // String  ky = key.toLowerCase(Locale.ROOT);
        Object value = json.get(key);
        newJS.put(ky,value);

      } );
      }


}
