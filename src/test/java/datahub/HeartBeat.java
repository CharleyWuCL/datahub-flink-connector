package datahub;

import java.io.Serializable;

/**
 * 柜机仓心跳
 */
public class HeartBeat implements Serializable {

  private String cabinetMac;

  private byte[] payload;


  public String getCabinetMac() {
    return cabinetMac;
  }

  public void setCabinetMac(String cabinetMac) {
    this.cabinetMac = cabinetMac;
  }

  public byte[] getPayload() {
    return payload;
  }

  public void setPayload(byte[] payload) {
    this.payload = payload;
  }
}
