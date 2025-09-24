const dbName = 'iot';
const collName = 'measurements';
const db = db.getSiblingDB(dbName);

if (!db.getCollectionNames().includes(collName)) {
  db.createCollection(collName);
}

db[collName].createIndex({ timestamp: 1 });
db[collName].createIndex({ device: 1 });
db[collName].createIndex({ device: 1, timestamp: 1 });
// db[collName].createIndex({ location: "2dsphere" });  // if you add geo fields
 