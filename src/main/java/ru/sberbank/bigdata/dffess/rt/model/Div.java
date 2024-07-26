package ru.sberbank.bigdata.dffess.rt.model;

import java.util.Date;

public class Div implements Entity{

    private String code;// STRING,
    private Long ctl_loading;// BIGINT,
    private Date ctl_validfrom;// TIMESTAMP,
    private boolean deleted_flag;// BOOLEAN,
    private boolean dirty_flag;// BOOLEAN,
    private Date div_close_dt;// DATE,
    private String div_dp_code;// STRING,
    private String div_open_dt; //DATE,
    private Long div_sk; //BIGINT,
    private String div_time_zone_time_zone_rf_dp_code;// STRING,
    private Long div_time_zone_time_zone_rf_sk;// BIGINT,
    private String full_reg_num;// STRING,
    private String legal_addr_dp_code; //STRING,
    private Long legal_addr_sk;// BIGINT,
    private String parent_div_dp_code;// STRING,
    private Long parent_div_sk;// BIGINT,
    private Long reg_ord;// BIGINT,
    private String short_div_code;// STRING,
    private String sid;// STRING,
    private String subbranch_code;// STRING

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Long getCtl_loading() {
        return ctl_loading;
    }

    public void setCtl_loading(Long ctl_loading) {
        this.ctl_loading = ctl_loading;
    }

    public Date getCtl_validfrom() {
        return ctl_validfrom;
    }

    public void setCtl_validfrom(Date ctl_validfrom) {
        this.ctl_validfrom = ctl_validfrom;
    }

    public boolean isDeleted_flag() {
        return deleted_flag;
    }

    public void setDeleted_flag(boolean deleted_flag) {
        this.deleted_flag = deleted_flag;
    }

    public boolean isDirty_flag() {
        return dirty_flag;
    }

    public void setDirty_flag(boolean dirty_flag) {
        this.dirty_flag = dirty_flag;
    }

    public Date getDiv_close_dt() {
        return div_close_dt;
    }

    public void setDiv_close_dt(Date div_close_dt) {
        this.div_close_dt = div_close_dt;
    }

    public String getDiv_dp_code() {
        return div_dp_code;
    }

    public void setDiv_dp_code(String div_dp_code) {
        this.div_dp_code = div_dp_code;
    }

    public String getDiv_open_dt() {
        return div_open_dt;
    }

    public void setDiv_open_dt(String div_open_dt) {
        this.div_open_dt = div_open_dt;
    }

    public Long getDiv_sk() {
        return div_sk;
    }

    public void setDiv_sk(Long div_sk) {
        this.div_sk = div_sk;
    }

    public String getDiv_time_zone_time_zone_rf_dp_code() {
        return div_time_zone_time_zone_rf_dp_code;
    }

    public void setDiv_time_zone_time_zone_rf_dp_code(String div_time_zone_time_zone_rf_dp_code) {
        this.div_time_zone_time_zone_rf_dp_code = div_time_zone_time_zone_rf_dp_code;
    }

    public Long getDiv_time_zone_time_zone_rf_sk() {
        return div_time_zone_time_zone_rf_sk;
    }

    public void setDiv_time_zone_time_zone_rf_sk(Long div_time_zone_time_zone_rf_sk) {
        this.div_time_zone_time_zone_rf_sk = div_time_zone_time_zone_rf_sk;
    }

    public String getFull_reg_num() {
        return full_reg_num;
    }

    public void setFull_reg_num(String full_reg_num) {
        this.full_reg_num = full_reg_num;
    }

    public String getLegal_addr_dp_code() {
        return legal_addr_dp_code;
    }

    public void setLegal_addr_dp_code(String legal_addr_dp_code) {
        this.legal_addr_dp_code = legal_addr_dp_code;
    }

    public Long getLegal_addr_sk() {
        return legal_addr_sk;
    }

    public void setLegal_addr_sk(Long legal_addr_sk) {
        this.legal_addr_sk = legal_addr_sk;
    }

    public String getParent_div_dp_code() {
        return parent_div_dp_code;
    }

    public void setParent_div_dp_code(String parent_div_dp_code) {
        this.parent_div_dp_code = parent_div_dp_code;
    }

    public Long getParent_div_sk() {
        return parent_div_sk;
    }

    public void setParent_div_sk(Long parent_div_sk) {
        this.parent_div_sk = parent_div_sk;
    }

    public Long getReg_ord() {
        return reg_ord;
    }

    public void setReg_ord(Long reg_ord) {
        this.reg_ord = reg_ord;
    }

    public String getShort_div_code() {
        return short_div_code;
    }

    public void setShort_div_code(String short_div_code) {
        this.short_div_code = short_div_code;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getSubbranch_code() {
        return subbranch_code;
    }

    public void setSubbranch_code(String subbranch_code) {
        this.subbranch_code = subbranch_code;
    }
}
