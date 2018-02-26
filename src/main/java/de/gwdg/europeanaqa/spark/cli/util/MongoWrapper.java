package de.gwdg.europeanaqa.spark.cli.util;

import com.mongodb.client.MongoDatabase;

import java.io.Serializable;

public class MongoWrapper implements Serializable {

	MongoDatabase mongoDb;

	public MongoDatabase getMongoDb() {
		return mongoDb;
	}

	public void setMongoDb(MongoDatabase mongoDb) {
		this.mongoDb = mongoDb;
	}

	public MongoWrapper(MongoDatabase mongoDb) {

		this.mongoDb = mongoDb;
	}
}
