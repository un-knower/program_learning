package demo.mongo;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import java.util.ArrayList;
import java.util.List;
public class MongoDBJDBC {
  public static void main(String[] args) {
    /**建立连接*/
    MongoClient mongoClient = new MongoClient("localhost", 27017);
    MongoDatabase mongoDatabase = mongoClient.getDatabase("mycol");
    System.out.println(mongoDatabase);
    
    /**创建集合*/
    if(mongoDatabase.getCollection("test") == null) {
      mongoDatabase.createCollection("test");
    }
    /**连接集合*/
    MongoCollection<Document> collection = mongoDatabase.getCollection("test");
    /**插入数据*/
    Document document = new Document("title", "MongoDB")
        .append("description", "database")
        .append("likes", 100)
        .append("by", "fly");
    
    List<Document> documents = new ArrayList<Document>();
    documents.add(document);
    collection.insertMany(documents);  //插入数据
    
    /**
     * 检索所有文档
     */
    FindIterable<Document> findIterable = collection.find();
    MongoCursor<Document> mongoCursor = findIterable.iterator();
    while(mongoCursor.hasNext()) {
      System.out.println(mongoCursor.next());
    }
    
    /**
     * 更新文档
     * //更新文档   将文档中likes=100的文档修改为likes=200
     */
    collection.updateMany(Filters.eq("likes", 100), new Document("$set", new Document("likes", 200)));
    mongoCursor = findIterable.iterator();
    while(mongoCursor.hasNext()) {
      System.out.println(mongoCursor.next());
    }
    
    
    /**
     * 删除文档
     */
    //删除满足条件的第一个文档
    collection.deleteOne(Filters.eq("likes", 200));
    //删除所有满足条件的文档
    collection.deleteMany(Filters.eq("likes", 200));
    
    
    
    
  }

}
