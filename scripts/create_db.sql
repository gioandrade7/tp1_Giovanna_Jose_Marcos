CREATE TABLE product (
	product_id INT NOT NULL,
	product_asin VARCHAR(10) NOT NULL UNIQUE, 
	product_title VARCHAR(460),
	product_group VARCHAR(13),
	product_salesrank INT,
	product_n_similars INT,
	product_n_categories INT DEFAULT 0,
	product_reviews_total INT DEFAULT 0,
    product_reviews_downloaded INT DEFAULT 0,
    product_reviews_avg FLOAT DEFAULT 0.0, 
	PRIMARY KEY(product_id)
);

CREATE TABLE similar_product (
    product_asin VARCHAR(10) NOT NULL,
    similar_asin VARCHAR(10) NOT NULL CHECK ( similar_asin <> product_asin),
    PRIMARY KEY(product_asin, similar_asin),
    FOREIGN KEY(product_asin) REFERENCES product(product_asin)
    --FOREIGN KEY(similar_asin) REFERENCES product(product_asin)
);

CREATE TABLE category (
    category_id INT NOT NULL,
    category_description VARCHAR(60),
    super_category_id INT,
    PRIMARY KEY(category_id),
    FOREIGN  KEY(super_category_id) REFERENCES category(category_id)
);

CREATE TABLE product_category (
	product_id INT NOT NULL,
	category_id INT NOT NULL,
	PRIMARY KEY (product_id, category_id),
    FOREIGN  KEY(product_id) REFERENCES product(product_id),
	FOREIGN  KEY(category_id) REFERENCES category(category_id)
);

CREATE TABLE review (
	review_id SERIAL NOT NULL,
	product_id INT NOT NULL,
    customer_id VARCHAR(15) NOT NULL,
	review_date DATE NOT NULL,
	review_rating INT DEFAULT 0,
	review_votes INT DEFAULT 0,
	review_helpful INT DEFAULT 0,
	PRIMARY KEY (review_id),
	FOREIGN KEY(product_id) REFERENCES product(product_id) 
);
