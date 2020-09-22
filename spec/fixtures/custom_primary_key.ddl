CREATE TABLE custom_primary_key (
  id serial NOT NULL,
  pk character varying(255),
  PRIMARY KEY (pk),
  UNIQUE(id)
)
