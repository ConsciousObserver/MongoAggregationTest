package com.example;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.SkipOperation;
import org.springframework.data.mongodb.core.index.TextIndexDefinition;
import org.springframework.data.mongodb.core.index.TextIndexDefinition.TextIndexDefinitionBuilder;
import org.springframework.data.mongodb.core.index.TextIndexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import jakarta.annotation.PostConstruct;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Builder;
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

    /**
     * This method inserts some documents into Product collection on startup
     */
    @PostConstruct
    void prepareData() {
        boolean collectionExists = mongoTemplate.collectionExists(Product.COLLECTION_NAME);

        log.info("####### product collection exists: {}", collectionExists);

        if (!collectionExists) {
            throw new RuntimeException(
                    String.format("Required collection {%s} does not exist", Product.COLLECTION_NAME));
        }

        //Adding index manually ------------- This is required for text search on productName
        TextIndexDefinition textIndex = new TextIndexDefinitionBuilder().onField("productName", 1F).build();
        mongoTemplate.indexOps(Product.class).ensureIndex(textIndex);

        boolean samplesAlreadyAdded = mongoTemplate
                .exists(new Query().addCriteria(Criteria.where("brandName").exists(true)), Product.class);

        //Uncomment to delete all rows from product collection
        //mongoTemplate.getCollection(Product.COLLECTION_NAME).deleteMany(new BsonDocument());

        if (!samplesAlreadyAdded) {
            for (int i = 1; i <= 5; i++) {
                //adds 3 words in productName
                //product name term1
                String productName = "product name term" + i;

                Product product = new Product(null, "ACTIVE", productName, "BRAND" + i, "CATEGORY" + i,
                        "SUB_CATEGORY" + 1);

                mongoTemplate.save(product);

                log.info("Saving sample product to database: {}", product);
            }
        } else {
            log.info("Skipping sample insertion as they're already in DB");
        }
    }
}

@Slf4j
@RestController
@RequiredArgsConstructor
@Validated
class ProductController {
    private final MongoTemplate mongoTemplate;

    /**
     * Invoke using following command
     * <p>
     * <code>http://localhost:8080/products?productName=product&brandName=BRAND1&categoryName=CATEGORY2&subCategoryName=SUB_CATEGORY3&pageNumber=0&pageSize=10</code>
     *
     * 
     * JSR 303 validations are returning 500 when validation fails, instead of
     * 400. That's why it's being handled in {@link RestExceptionHandler}
     * 
     * @param productName
     * @param brandName
     * @param categoryName
     * @param subCategoryName
     * @param pageNumber
     * @param pageSize
     * @return
     */
    @GetMapping("/products")
    public List<Product> getProducts(@RequestParam String productName, @RequestParam String brandName,
            @RequestParam String categoryName, @RequestParam String subCategoryName,
            @RequestParam @Min(0) int pageNumber, @RequestParam @Min(1) @Max(100) int pageSize) {

        log.info(
                "Request parameters: productName: {}, brandName: {}, categoryName: {}, subCategoryName: {}, pageNumber: {}, pageSize: {}",
                productName, brandName, categoryName, subCategoryName, pageNumber, pageSize);
        //Query Start

        TextCriteria productNameTextCriteria = new TextCriteria().matchingAny(productName).caseSensitive(false);
        TextCriteriaHack textCriteriaHack = new TextCriteriaHack();
        textCriteriaHack.addCriteria(productNameTextCriteria);

        //Needs this hack to combine TextCriteria with Criteria in a single query
        //See TextCriteriaHack for details
        MatchOperation productNameTextMatch = new MatchOperation(textCriteriaHack);

        //Exact match
        Criteria brandNameMatch = Criteria.where("brandName").is(brandName);
        Criteria categoryNameMatch = Criteria.where("categoryName").is(categoryName);
        Criteria subCategoryNameMatch = Criteria.where("subCategoryName").is(subCategoryName);

        MatchOperation orMatch = Aggregation
                .match(new Criteria().orOperator(brandNameMatch, categoryNameMatch, subCategoryNameMatch));

        //Pagination setup
        SkipOperation skip = Aggregation.skip((long) pageNumber * pageSize);
        LimitOperation limit = Aggregation.limit(pageSize);

        Aggregation aggregation = Aggregation.newAggregation(productNameTextMatch, orMatch, skip, limit)
                //AllowDiskUse is needed in case query runs out of fixed memory configured in Mongo
                .withOptions(AggregationOptions.builder().allowDiskUse(true).build());

        //Query end

        //Query execution
        AggregationResults<Product> aggregateResults = mongoTemplate.aggregate(aggregation, Product.COLLECTION_NAME,
                Product.class);

        List<Product> products = new ArrayList<>();

        aggregateResults.iterator().forEachRemaining(products::add);

        log.info("Found products: {}", products);

        return products;
    }

    @GetMapping("/group-by-status")
    public List<org.bson.Document> getProducts() {

        //Query Start
        GroupOperation groupByStatus = Aggregation.group("status").count().as("statusCount");

        Aggregation aggregation = Aggregation.newAggregation(groupByStatus)
                //AllowDiskUse is needed in case query runs out of fixed memory configured in Mongo
                .withOptions(AggregationOptions.builder().allowDiskUse(true).build());
        //Query end

        //Query execution
        AggregationResults<org.bson.Document> aggregateResults = mongoTemplate.aggregate(aggregation,
                Product.COLLECTION_NAME,
                org.bson.Document.class);

        List<org.bson.Document> results = new ArrayList<>();

        aggregateResults.iterator().forEachRemaining(results::add);

        log.info("Found products: {}", results);

        return results;
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

    @TextIndexed
    @Field("productName")
    private String productName;

    @Field("brandName")
    private String brandName;

    @Field("categoryName")
    private String categoryName;

    @Field("subCategoryName")
    private String subCategoryName;
}

/**
 * https://stackoverflow.com/a/29925876 There is no way to combine
 * CriteriaDefinition and Criteria in one query This hack converts
 * CriteriaDefinition to Query which can be converted to Criteria
 */
class TextCriteriaHack extends Query implements CriteriaDefinition {
    @Override
    public org.bson.Document getCriteriaObject() {
        return this.getQueryObject();
    }

    @Override
    public String getKey() {
        return null;
    }
}

/***
 * {@link ConstraintViolationException} is not handled by Spring and results in
 * 500 responses. Handling it here to return 400.
 *
 */
@RestControllerAdvice
class RestExceptionHandler extends ResponseEntityExceptionHandler {
    @ExceptionHandler({ ConstraintViolationException.class })
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    protected ResponseEntity<Object> handleConstraintViolationException(
            ConstraintViolationException e) {

        ErrorResponse errorResponse = ErrorResponse.builder()
                .message(e.getMessage())
                .timestamp(LocalDateTime.now())
                .statusCode(HttpStatus.BAD_REQUEST.value())
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
    }
}

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
class ErrorResponse {
    private LocalDateTime timestamp;

    private String message;

    private int statusCode;
}

