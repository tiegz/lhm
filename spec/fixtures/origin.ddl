CREATE TABLE origin (
  id serial NOT NULL,
  origin integer DEFAULT NULL,
  common character varying(255) DEFAULT NULL,
  PRIMARY KEY (id)
)
