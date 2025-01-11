package com.itextos.beacon.queryprocessor.databaseconnector;

public class DatabaseInformationModel
{

    public String url;
    public String username;
    public String password;
    public String driver_class_name;
    public String validation_query;
    public int    num_tests_per_eviction_run;
    public int    min_evictable_idle_time_millis;
    public int    max_total;
    public String description;
    
    public String toString() {
    	
    	StringBuffer sb=new StringBuffer();
    	sb.append("url : "+url).append("\n");
    	sb.append("username : "+username).append("\n");
    	sb.append("password : "+password ).append("\n");
    	sb.append("driver_class_name : "+driver_class_name).append("\n");
    	sb.append("validation_query : "+ validation_query).append("\n");
    	sb.append("max_total : "+ max_total ).append("\n");
    	sb.append("description : "+ description ).append("\n");

    	return sb.toString();
    }

}
