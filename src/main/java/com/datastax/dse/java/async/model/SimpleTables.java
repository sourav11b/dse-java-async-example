package com.datastax.dse.java.async.model;

import java.io.Serializable;
import java.util.List;

public class SimpleTables implements Serializable {

	
	private List<SimpleTable> rows;

	public List<SimpleTable> getRows() {
		return rows;
	}

	public void setRows(List<SimpleTable> rows) {
		this.rows = rows;
	}
	 
	 
	
	
}
