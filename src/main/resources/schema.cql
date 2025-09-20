CREATE KEYSPACE IF NOT EXISTS directory
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS directory.user_profile (
    id uuid PRIMARY KEY,
    name text,
    age int,
    tags set<text>,
    created_at timestamp
);
