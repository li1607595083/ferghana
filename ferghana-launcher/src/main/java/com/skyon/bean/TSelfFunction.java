package com.skyon.bean;

import java.util.Date;

public class TSelfFunction {

    private Long self_function_id;
    private String self_function_name_cn;
    private String module_type;
    private String function_name;
    private String function_package_path;
    private String file_path;
    private String function_params;
    private String self_function_desc;
    private String input_param;
    private String output_param;
    private Date create_time;
    private Date update_time;

    public Long getSelf_function_id() {
        return self_function_id;
    }

    public void setSelf_function_id(Long self_function_id) {
        this.self_function_id = self_function_id;
    }

    public String getSelf_function_name_cn() {
        return self_function_name_cn;
    }

    public void setSelf_function_name_cn(String self_function_name_cn) {
        this.self_function_name_cn = self_function_name_cn;
    }

    public String getModule_type() {
        return module_type;
    }

    public void setModule_type(String module_type) {
        this.module_type = module_type;
    }

    public String getFunction_name() {
        return function_name;
    }

    public void setFunction_name(String function_name) {
        this.function_name = function_name;
    }

    public String getFunction_package_path() {
        return function_package_path;
    }

    public void setFunction_package_path(String function_package_path) {
        this.function_package_path = function_package_path;
    }

    public String getFile_path() {
        return file_path;
    }

    public void setFile_path(String file_path) {
        this.file_path = file_path;
    }

    public String getFunction_params() {
        return function_params;
    }

    public void setFunction_params(String function_params) {
        this.function_params = function_params;
    }

    public String getSelf_function_desc() {
        return self_function_desc;
    }

    public void setSelf_function_desc(String self_function_desc) {
        this.self_function_desc = self_function_desc;
    }

    public String getInput_param() {
        return input_param;
    }

    public void setInput_param(String input_param) {
        this.input_param = input_param;
    }

    public String getOutput_param() {
        return output_param;
    }

    public void setOutput_param(String output_param) {
        this.output_param = output_param;
    }

    public Date getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    public Date getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }

    @Override
    public String toString() {
        return "TSelfFunction{" +
                "self_function_id=" + self_function_id +
                ", self_function_name_cn='" + self_function_name_cn + '\'' +
                ", module_type='" + module_type + '\'' +
                ", function_name='" + function_name + '\'' +
                ", function_package_path='" + function_package_path + '\'' +
                ", file_path='" + file_path + '\'' +
                ", function_params='" + function_params + '\'' +
                ", self_function_desc='" + self_function_desc + '\'' +
                ", input_param='" + input_param + '\'' +
                ", output_param='" + output_param + '\'' +
                ", create_time=" + create_time +
                ", update_time=" + update_time +
                '}';
    }
}
