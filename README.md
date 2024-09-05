wiki2graph
==========

This tool builds a graph in neo4j from a wiki-dump.

## Overview

The idea is to download the dump, parse it to get an easier to work with json,
convert it do different CSV files and import them into neo4j.

It builds a graph, with `:ARTICLE` and `:AUTHOR` and the relationsships `LINKS_TO` and `:AUTHORED`

## What these scripts do?

- Take a wiki-dump file (which you can download)
- Extract the media-wiki-markup files (and add metadata as JSON in the first line)
- Parse these into easy-to-use JSON files and convert the markup to HTML.
- Create a graph from the JSON-Files as CSV-Files, ready to import to a graph-DB.
- Import the CSV-Files to neo4j by mounting them into the docker-container and running cypher-import commands.

These are the steps.

## Download a dump

I tested everything with the german wikidump (`dewiki`)

1. Go to this site: https://dumps.wikimedia.org/dewiki/
2. Select something except latest. For example https://dumps.wikimedia.org/dewiki/20240901/ but you probably want a newer one
3. Check, that the status is "done". Click on the download link, e.g. dewiki-20240901-pages-articles-multistream.xml.bz2
   This is around 7GB big.

## Parse the Wiki Dump

Use poetry to enter an virtual-env and install the dependencies.

1. Go to the root of this project
2. Extract the bz2, so that you have an xml file
2. `python3 wiki2graph/parse_wiki.py extract ~/Downloads/dewiki-20240820-pages-articles-multistream.xml data/wiki`
   This takes around 18 Minutes. It extracts the mediawiki-markup into seperate files (in buckets) in `data/wiki`. 
   Look at `data/wiki/000/1970.wiki`. The first line is some metadata in JSON, the rest is the content
3. `python3 wiki2graph/parse_wiki.py parse data/wiki data/wiki_json --processes=-1`
   This takes around 1-2 hours. It parses the 5 Mio articles (2.9 Mio Main-Articles, the rest is stuff like Categories, Maintainence stuff, etc.).
   Not all articles can be converted to HTML, which is printed while converting. 
   If you set `--processes=-1` all cores are used. I get around 1000it/s


### Note on article-types

Articles can be very different. Most of them are main articles (a normal article which
you would expect when browsing). But they can also articles like categories or internal 
articles for maintainence, e.g. internal discussions. You can distinguish them by 
namespace. To get a list of the different namespaces, give the variable `namespaces` in `wiki2graph/parse_wiki.py` a look.

## Import Articles to neo4j

To bring it to neo4j we create CSVs and import them. This is much faster. 

Note: Creating an index in neo4j first makes everything 10-40x faster!
 

1. `python3 wiki2graph/import_neo4j.py create-csv data/wiki_json data/csv` 
   Takes the extracted JSON files. For every bucket, it creates the CSV files for the graph database. 
   This is the nodes and relationships in csv-files to import into neo4j. Takes around 2 Minutes. 
2. `docker-compose up -d` 
   Start neo4j and mount the csv-directory (see the `docker-compose.yaml`)
   Double-check, that the mount-path in the file `./data/csv:` is correct.
3. Refresh `http://localhost:7474/` and wait until neo4j is read. As set in the 
   `docker-compose.yaml`, username/password is `neo4j/password`
   It is important that you follow the schema:
   `./<relative path to csv dir>:/import/<relative path to csv dir>`
4. `python3 wiki2graph/import_neo4j.py import-csv data/csv`
   This performs multiple steps, which you can check in `wiki2graph/import_neo4j.py` `import_csv` (check the `click.echo`)
   This takes around 56:00
   (3 minutes for the articles, <1min for authors, 2:30 Minutes for the links between authors and articles, 35 min for the links betweenthe articles)
   You can check the progress on the neo4j web-ui.
   In the end, you should have (for the german wiki x articles and x relationhsips)