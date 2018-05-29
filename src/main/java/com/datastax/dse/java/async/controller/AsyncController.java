package com.datastax.dse.java.async.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.dse.DseCluster;
import com.datastax.dse.java.async.model.SimpleTable;
import com.datastax.dse.java.async.model.SimpleTables;
import com.datastax.dse.java.async.repositories.SimpleRepository;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

@RestController
public class AsyncController {

	
	
	@Autowired
	SimpleRepository simpleRepository;

	@RequestMapping("/selectSync")
	public DeferredResult<ResponseEntity<?>> selectSync(@RequestParam("query") String query) {

		System.out.println("Query " + query);

		final DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<ResponseEntity<?>>(5000l);
		deferredResult.onTimeout(new Runnable() {

			public void run() { // Retry on timeout
				deferredResult.setErrorResult(
						ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Request timeout occurred."));
			}
		});
		
		simpleRepository.selectSync(deferredResult, query);
		return deferredResult;
	}

	@RequestMapping("/selectAll")
	public DeferredResult<ResponseEntity<?>> selectAll(@RequestParam("query") String query) {

		System.out.println("Query " + query);

		final DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<ResponseEntity<?>>(5000l);
		deferredResult.onTimeout(new Runnable() {

			public void run() { // Retry on timeout
				deferredResult.setErrorResult(
						ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Request timeout occurred."));
			}
		});
		
		simpleRepository.select(deferredResult, query);
		return deferredResult;
	}
	
	@RequestMapping("/select")
	public DeferredResult<ResponseEntity<?>> select(@RequestParam("pk") String pk) {

		System.out.println("PK " + pk);

		final DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<ResponseEntity<?>>(5000l);
		deferredResult.onTimeout(new Runnable() {

			public void run() { // Retry on timeout
				deferredResult.setErrorResult(
						ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Request timeout occurred."));
			}
		});
		
		simpleRepository.selectWithPK(deferredResult, pk);;
		return deferredResult;
	}
	
	

	@RequestMapping(value = "/insertOne", method = RequestMethod.POST)
	public DeferredResult<ResponseEntity<?>> insertOne(@RequestBody final SimpleTable row) {

		System.out.println("received request :" + row);
		final DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<ResponseEntity<?>>(5000l);

		deferredResult.onTimeout(new Runnable() {

			public void run() { // Retry on timeout
				deferredResult.setErrorResult(
						ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Request timeout occurred."));
			}
		});
		simpleRepository.insertOne(deferredResult, row);

		return deferredResult;
	}
	
	
	@RequestMapping(value = "/insertMany", method = RequestMethod.POST)
	public DeferredResult<ResponseEntity<?>> insertMany(@RequestBody final SimpleTables rows) {

		System.out.println("received request :" + rows);
		 DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<ResponseEntity<?>>(5000l);

		deferredResult.onTimeout(new Runnable() {

			public void run() { // Retry on timeout
				deferredResult.setErrorResult(
						ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Request timeout occurred."));
			}
		});
		simpleRepository.insertMany(deferredResult, rows);
		return deferredResult;
	}

	
}
