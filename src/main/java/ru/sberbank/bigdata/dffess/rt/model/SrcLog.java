package ru.sberbank.bigdata.dffess.rt.model;

// Таблица логов загрузки
public class SrcLog implements Entity {
    private String table_name;
    private Long max_src_ctl_loading;
    private String datetime;
    private String load_type;
    private String status;
    private String ctl_loading;
    private String stage;

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public Long getMax_src_ctl_loading() {
        return max_src_ctl_loading;
    }

    public void setMax_src_ctl_loading(Long max_src_ctl_loading) {
        this.max_src_ctl_loading = max_src_ctl_loading;
    }

    public String getDatetime() {
        return datetime;
    }

    public void setDatetime(String datetime) {
        this.datetime = datetime;
    }

    public String getLoad_type() {
        return load_type;
    }

    public void setLoad_type(String load_type) {
        this.load_type = load_type;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getCtl_loading() {
        return ctl_loading;
    }

    public void setCtl_loading(String ctl_loading) {
        this.ctl_loading = ctl_loading;
    }

    public String getStage() {
        return stage;
    }

    public void setStage(String stage) {
        this.stage = stage;
    }
}
