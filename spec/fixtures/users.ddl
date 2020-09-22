CREATE TABLE public.users (
  id serial primary key NOT NULL,
  reference integer DEFAULT NULL,
  username character varying(255) DEFAULT NULL,
  groupname character varying(255) DEFAULT 'Superfriends',
  created_at timestamp DEFAULT NULL,
  comment character varying(20) DEFAULT NULL,
  description text,
  UNIQUE(reference)
);

CREATE INDEX index_users_on_username_and_created_at ON public.users USING btree (username,created_at);
CREATE INDEX index_with_a_custom_name ON public.users USING btree (username,groupname);

