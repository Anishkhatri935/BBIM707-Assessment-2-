// BBIM707 Assessment 2 - MongoDB
db = db.getSiblingDB("bbim707_nyc_taxi");

printjson(db.getCollectionNames());
print("Total taxi records:");
print(db.taxi_trips.countDocuments({}));
printjson(db.taxi_trips.getIndexes());
printjson(db.taxi_trips.findOne());
