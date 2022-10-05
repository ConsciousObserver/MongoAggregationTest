package com.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.validation.constraints.Min;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.SkipOperation;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@SpringBootApplication
@Slf4j
public class MongoAggregationTestApplication {

	public static void main(String[] args) {
		SpringApplication.run(MongoAggregationTestApplication.class, args);
	}

	private final MongoTemplate mongoTemplate;

	@PostConstruct
	void prepareData() {
		boolean collectionExists = mongoTemplate.collectionExists(Product.COLLECTION_NAME);

		log.info("####### product collection exists: {}", collectionExists);

		if (!collectionExists) {
			throw new RuntimeException(
			        String.format("Required collection {%s} does not exist", Product.COLLECTION_NAME));
		}

		Criteria.where("brandName").exists(true);

		boolean samplesAlreadyAdded = mongoTemplate
		        .exists(new Query().addCriteria(Criteria.where("brandName").exists(true)), Product.class);

		if (!samplesAlreadyAdded) {
			List<Product> products = Arrays.asList(new Product(null, "ACTIVE", "BRAND1", "CATEGORY1", "SUB_CATEGORY1"),
			        new Product(null, "INACTIVE", "BRAND2", "CATEGORY2", "SUB_CATEGORY2"),
			        new Product(null, "ACTIVE", "BRAND3", "CATEGORY3", "SUB_CATEGORY3"),
			        new Product(null, "ACTIVE", "BRAND4", "CATEGORY4", "SUB_CATEGORY4"),
			        new Product(null, "ACTIVE", "BRAND5", "CATEGORY5", "SUB_CATEGORY5"),
			        new Product(null, "ACTIVE", "BRAND6", "CATEGORY6", "SUB_CATEGORY6"));

			log.info("Saving sample products to database: {}", products);

			products.forEach(mongoTemplate::save);
		} else {
			log.info("Skipping sample insertion as they're already in DB");
		}
	}
}

@Slf4j
@RestController
@RequiredArgsConstructor
class ProductController {
	private final MongoTemplate mongoTemplate;

	@GetMapping("/products")
	List<Product> getProducts(@RequestParam String brandName, @RequestParam String categoryName,
	        @RequestParam String subCategoryName, @RequestParam @Min(0) int pageNumber,
	        @RequestParam @Min(1) int pageSize) {

		//Start query
		Criteria brandNameMatch = Criteria.where("brandName").is(brandName);
		Criteria categoryNameMatch = Criteria.where("categoryName").is(categoryName);
		Criteria subCategoryNameMatch = Criteria.where("subCategoryName").is(subCategoryName);

		MatchOperation match = Aggregation
		        .match(new Criteria().orOperator(brandNameMatch, categoryNameMatch, subCategoryNameMatch));

		SkipOperation skip = Aggregation.skip((long) pageNumber * pageSize);
		LimitOperation limit = Aggregation.limit(pageSize);

		Aggregation aggregation = Aggregation.newAggregation(match, skip, limit);
		
		//End query

		//Query execution
		AggregationResults<Product> aggregateResults = mongoTemplate.aggregate(aggregation, Product.COLLECTION_NAME,
		        Product.class);

		List<Product> products = new ArrayList<>();

		aggregateResults.iterator().forEachRemaining(products::add);

		log.info("Found products: {}", products);

		return products;
	}
}

@Data
@Document(Product.COLLECTION_NAME)
@NoArgsConstructor
@AllArgsConstructor
class Product {
	static final String COLLECTION_NAME = "product";

	@Id
	@Field("_id")
	private String id;

	@Field("status")
	private String status;

	@Field("brandName")
	private String brandName;

	@Field("categoryName")
	private String categoryName;

	@Field("subCategoryName")
	private String subCategoryName;
}