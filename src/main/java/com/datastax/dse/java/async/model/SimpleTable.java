package com.datastax.dse.java.async.model;

import java.io.Serializable;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.Table;

@Table(name = "simple_table",keyspace = "java_sample")
public class SimpleTable implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3759424184026397780L;
	
	private UUID id;
	private String name;
	private String description;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}

	public UUID getId() {
		return id;
	}
	public void setId(UUID id) {
		this.id = id;
	}
	
	


}
