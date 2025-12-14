@'
CREATE TABLE IF NOT EXISTS tdsp (
  duns TEXT PRIMARY KEY,
  utility_id INTEGER,
  utility_name TEXT,
  state TEXT,
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS brand (
  id TEXT PRIMARY KEY,
  name TEXT,
  puct_number TEXT,
  legal_name TEXT
);

CREATE TABLE IF NOT EXISTS product (
  id TEXT PRIMARY KEY,
  brand_id TEXT REFERENCES brand(id),
  name TEXT,
  term INTEGER,
  family TEXT,
  percent_green INTEGER,
  headline TEXT,
  early_termination_fee NUMERIC,
  description TEXT,
  is_pre_pay BOOLEAN,
  is_time_of_use BOOLEAN
);

CREATE TABLE IF NOT EXISTS plan_listing (
  id TEXT PRIMARY KEY,                  -- top-level "_id"
  product_id TEXT REFERENCES product(id),
  tdsp_duns TEXT REFERENCES tdsp(duns),
  grp TEXT,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS expected_price (
  plan_listing_id TEXT REFERENCES plan_listing(id) ON DELETE CASCADE,
  usage INTEGER,
  price NUMERIC,
  actual NUMERIC,
  valid BOOLEAN,
  PRIMARY KEY(plan_listing_id, usage)
);

CREATE TABLE IF NOT EXISTS document_link (
  plan_listing_id TEXT REFERENCES plan_listing(id) ON DELETE CASCADE,
  doc_type TEXT,
  language TEXT,
  link TEXT,
  snapshot_url TEXT,
  PRIMARY KEY(plan_listing_id, doc_type, language)
);

CREATE TABLE IF NOT EXISTS zip_tdsp_map (
  zip TEXT PRIMARY KEY,
  duns TEXT REFERENCES tdsp(duns),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
'@ | Out-File -Encoding utf8 sql\schema.sql
