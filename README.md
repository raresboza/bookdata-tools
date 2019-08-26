This repository contains the code to import and integrate the book and rating data that we work with.
It imports and integrates data from several sources in a single PostgreSQL database; import scripts
are primarily in Python, with Rust code for high-throughput processing of raw data files.

If you use these scripts in any published reseaerch, cite [our paper](https://md.ekstrandom.net/pubs/book-author-gender):

> Michael D. Ekstrand, Mucun Tian, Mohammed R. Imran Kazi, Hoda Mehrpouyan, and Daniel Kluver. 2018. Exploring Author Gender in Book Rating and Recommendation. In *Proceedings of the 12th ACM Conference on Recommender Systems* (RecSys '18). ACM, pp. 242–250. DOI:[10.1145/3240323.3240373](https://doi.org/10.1145/3240323.3240373). arXiv:[1808.07586v1](https://arxiv.org/abs/1808.07586v1) [cs.IR].

**Note:** the limitations section of the paper contains important information about
the limitations of the data these scripts compile.  **Do not use this data or tools
without understanding those limitations**.  In particular, VIAF's gender information
is incomplete and, in a number of cases, incorrect.

## Requirements

- PostgreSQL 10 or later with [orafce](https://github.com/orafce/orafce) and `pg_prewarm` (from the
  PostgreSQL Contrib package) installed.
- Python 3.6 or later with the following packages:
    - psycopg2
    - invoke
    - numpy
    - tqdm
    - pandas
    - numba
    - colorama
    - chromalog
    - humanize
- The Rust compiler (available from Anaconda)
- `psql` executable on the machine where the import scripts will run
- 2TB disk space for the database
- 100GB disk space for data files

It is best if you do not store the data files on the same disk as your PostgreSQL database.

It is best if you do not store the data files on the same disk as your PostgreSQL database.

The `environment.yml` file defines an Anaconda environment that contains all the required packages except for the PostgreSQL server. It can be set up with:

    conda env create -f environment.yml

All scripts read database connection info from the standard PostgreSQL client environment variables:

- `PGDATABASE`
- `PGHOST`
- `PGUSER`
- `PGPASSWORD`

## Initializing and Configuring the Database

After creating your database, initialize the extensions (as the database superuser):

    CREATE EXTENSION orafce;
    CREATE EXTENSION pg_prewarm;

The default PostgreSQL performance configuration settings will probably not be
very effective; we recommend turning on parallelism and increasing work memory,
at a minimum.

## Downloading Data Files

This imports the following data sets:

-   Library of Congress MDSConnect Open MARC Records — get the XML files from <https://www.loc.gov/cds/products/MDSConnect-books_all.html>
    and save them into the `data/LOC` directory.
-   LoC MDSConnect Name Authorities - get the Combined XML file from <https://www.loc.gov/cds/products/MDSConnect-name_authorities.html>
    and save it in `data/LOC`.
-   Virtual Internet Authority File - get the MARC 21 XML data file from <http://viaf.org/viaf/data/> and save it into the `data` directory.
-   OpenLibrary Dump - get the editions, works, and authors dumps from <https://openlibrary.org/developers/dumps> and save them in `data`.
-   Amazon Ratings - get the 'ratings only' data for _Books_ from <http://jmcauley.ucsd.edu/data/amazon/> and save it in `data`.
-   BookCrossing - get the BX-Book-Ratings CSV file from <http://www2.informatik.uni-freiburg.de/~cziegler/BX/> and save it in `data`

## Running Import Tasks

The import process is scripted with [invoke](http://www.pyinvoke.org).  The first tasks to run are
the import tasks:

    invoke loc.import-books
    invoke loc.import-names
    invoke viaf.import
    invoke openlib.import-authors openlib.import-works openlib.import-editions
    invoke goodreads.import
    invoke ratings.import-az
    invoke ratings.import-bx

Once all the data is imported, you can begin to run the indexing and linking tasks:

    invoke viaf.index
    invoke loc.index-books
    invoke loc.index-names
    invoke openlib.index
    invoke goodreads.index-books
    invoke analyze.cluster --scope loc
    invoke analyze.cluster --scope ol
    invoke analyze.cluster --scope gr
    invoke analyze.cluster
    invoke ratings.index
    invoke goodreads.index-ratings
    invoke analyze.authors

The tasks keep track of the import status in an `import_status` table, and will
keep you from running tasks in the wrong order.

## Setting Up Schemas

The `-schema` files contain the base schemas for the data:

- `common-schema.sql` — common tables
- `loc-mds-schema.sql` — Library of Congress catalog tables
- `ol-schema.sql` — OpenLibrary book data
- `viaf-schema.sql` — VIAF tables
- `az-schema.sql` — Amazon rating schema
- `bx-schema.sql` — BookCrossing rating data schema
- `gr-schema.sql` — GoodReads data schema
- `loc-ids-schema.sql` - LOC ID schemas