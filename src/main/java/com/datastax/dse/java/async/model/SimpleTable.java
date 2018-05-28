package com.datastax.dse.java.async.model;

import java.io.Serializable;

public class SimpleTable implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3759424184026397780L;
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
	@Override
	public String toString() {
		return "SimpleTable [name=" + name + ", description=" + description + "]";
	}
	
	


}
