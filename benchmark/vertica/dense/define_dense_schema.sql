CREATE TABLE dense_50000x20000_2500x1000 (
X         INTEGER NOT NULL ENCODING GZIP_COMP,
Y         INTEGER NOT NULL ENCODING GZIP_COMP,
A1        INTEGER NOT NULL ENCODING GZIP_COMP,
PRIMARY KEY (X,Y)
);
