package com.datastax.dse.java.async.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import com.datastax.dse.java.async.model.SimpleTable;
import com.datastax.dse.java.async.model.SimpleTables;
import com.datastax.dse.java.async.repositories.SimpleRepository;

@RestController
public class AsyncController {

	static Logger logger = LoggerFactory.getLogger(AsyncController.class);

	
	@Autowired
	SimpleRepository simpleRepository;

	@RequestMapping("/syncSelectUsingQueryParam")
	public DeferredResult<ResponseEntity<?>> syncSelectUsingQueryParam(@RequestParam("query") String query) {

		 logger.info("Received Query " + query);

		final DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<ResponseEntity<?>>(5000l);
		deferredResult.onTimeout(new Runnable() {

			public void run() { // Retry on timeout
				deferredResult.setErrorResult(
						ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Request timeout occurred."));
			}
		});
		
		simpleRepository.syncSelectUsingQueryParam(deferredResult, query);
		logger.info(Thread.currentThread().getName() + "Done with main");

		return deferredResult;
	}

	@RequestMapping(value = "/selectUsingQueryParam" ,produces=MediaType.APPLICATION_JSON_VALUE )
	public @ResponseBody DeferredResult<ResponseEntity<?>> selectUsingQueryParam(@RequestParam("query") String query) {

		logger.info("Query " + query);

		final DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<ResponseEntity<?>>(5000l);
		deferredResult.onTimeout(new Runnable() {

			public void run() { // Retry on timeout
				deferredResult.setErrorResult(
						ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Request timeout occurred."));
			}
		});
		
		simpleRepository.selectUsingQueryParam(deferredResult, query);
		return deferredResult;
	}
	
	
	@RequestMapping(value = "/selectWithSolrQuery" ,produces=MediaType.APPLICATION_JSON_VALUE )
	public @ResponseBody DeferredResult<ResponseEntity<?>> selectWithSolrQuery(@RequestParam("solrQuery") String solrQuery) {

		logger.info("solrQuery " + solrQuery);

		final DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<ResponseEntity<?>>(5000l);
		deferredResult.onTimeout(new Runnable() {

			public void run() { // Retry on timeout
				deferredResult.setErrorResult(
						ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Request timeout occurred."));
			}
		});
		
		simpleRepository.selectWithSolrQuery(deferredResult, solrQuery);
		return deferredResult;
	}
	
	
	@RequestMapping("/selectWithPK")
	public DeferredResult<ResponseEntity<?>> selectWithPK(@RequestParam("pk") String pk) {

		logger.info("PK " + pk);

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

		logger.info("received request :" + row);
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

		logger.info("received request :" + rows);
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
	@RequestMapping(value = "/syncInsertMany", method = RequestMethod.POST)
	public DeferredResult<ResponseEntity<?>> syncInsertMany(@RequestBody final SimpleTables rows) {

		logger.info("received request :" + rows);
		DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<ResponseEntity<?>>(5000l);

		deferredResult.onTimeout(new Runnable() {

			public void run() { // Retry on timeout
				deferredResult.setErrorResult(
						ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Request timeout occurred."));
			}
		});
		simpleRepository.syncInsertMany(deferredResult, rows);
		return deferredResult;
	}

	
}
