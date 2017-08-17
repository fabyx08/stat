package utils;

import java.io.Serializable;
import java.util.Objects;

public class RankItem implements Serializable {
    private Integer id;
    private String city;
    private String address;
    private Integer km;
    private String model;
    private Long installationTimestamp;
    private Long meanExpirationTime;

    public RankItem(Integer id, String city, String address, Integer km, String model, Long installationTimestamp, Long meanExpirationTime) {
        this.id = id;
        this.city = city;
        this.address = address;
        this.km = km;
        this.model = model;
        this.installationTimestamp = installationTimestamp;
        this.meanExpirationTime = meanExpirationTime;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == null || !(obj instanceof RankItem))
            return false;

        RankItem other = (RankItem) obj;

        return Objects.equals(this.id, other.id);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public Long getInstallationTimestamp() {
        return installationTimestamp;
    }

    public void setInstallationTimestamp(Long installationTimestamp) {
        this.installationTimestamp = installationTimestamp;
    }

    public Long getMeanExpirationTime() {
        return meanExpirationTime;
    }

    public void setMeanExpirationTime(Long meanExpirationTime) {
        this.meanExpirationTime = meanExpirationTime;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Integer getKm() {
        return km;
    }

    public void setKm(Integer km) {
        this.km = km;
    }
}
