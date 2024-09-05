import logging
import click
from neo4j import GraphDatabase, Driver
from pathlib import Path
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map
import multiprocessing as mp
import json
import pandas as pd 

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_cypher_query(driver: Driver, query: str) -> None:
    """
    Run a Cypher query on the provided Neo4j driver.

    :param driver: Neo4j driver instance.
    :param query: Cypher query string.
    """
    with driver.session() as session:
        session.run(query)

def clear_database(driver: Driver) -> None:
    """
    Clear all nodes and relationships in the Neo4j database.
    This may not work with too many nodes as Neo4j may run out of memory.

    :param driver: Neo4j driver instance.
    """
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    logger.info("All nodes and relationships deleted from the database.")

def import_data(driver: Driver, csv_dir: str, pattern: str, query_template: str) -> None:
    """
    Generic function to import data from CSV files using a Cypher query.

    :param driver: Neo4j driver instance.
    :param csv_dir: Directory containing CSV files.
    :param pattern: Glob pattern to match the relevant CSV files.
    :param query_template: Cypher query template with placeholders for file paths.
    """
    csv_files = list(Path(csv_dir).rglob(pattern))
    if not csv_files:
        logger.warning(f"No files found for pattern: {pattern}")
        return

    for csv_file in tqdm(csv_files, desc=f"Importing {pattern}"):
        csv_file_path = csv_file.as_posix()
        query = query_template.format(csv_file_path=csv_file_path)
        try:
            run_cypher_query(driver, query)
            logger.info(f"Successfully imported {csv_file_path}")
        except Exception as e:
            logger.error(f"Error importing {csv_file_path}: {e}")

@click.group()
def cli():
    """
    Command Line Interface group function to register commands.
    """
    ...

@cli.command()
def clear():
    """
    Clear all nodes and relationships in the Neo4j database.
    """
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
    click.echo("Clearing the database...")
    clear_database(driver)
    click.echo("Database cleared.")


@cli.command()
@click.argument('csv_dir', type=click.Path(exists=True))
@click.option('--uri', default="bolt://localhost:7687", help="Neo4j URI")
@click.option('--username', default="neo4j", help="Neo4j Username")
@click.option('--password', default="password", help="Neo4j Password")
def import_csv(csv_dir: str, uri: str, username: str, password: str) -> None:
    """
    Perform a full data import. This includes:
    1. Clearing the Neo4j database.
    2. Creating necessary indexes.
    3. Importing articles, authors, author links, and article links from CSV files.

    :param csv_dir: Directory containing the CSV files for import.
    :param uri: URI for the Neo4j database connection.
    :param username: Username for Neo4j authentication.
    :param password: Password for Neo4j authentication.
    """
    click.echo("Starting data import process...")

    # Inline driver creation
    driver = GraphDatabase.driver(uri, auth=(username, password))

    click.echo("Creating indexes...")
    # Inline create_indexes
    with driver.session() as session:
        try:
            session.run('CREATE INDEX FOR (a:Article) ON (a.title);')
        except:
            ...
        try:
            session.run('CREATE INDEX FOR (a:Author) ON (a.id);')
        except:
            ...
    logger.info("Indexes created for Article (title) and Author (id).")

    click.echo("Importing articles...")
    article_query_template = """
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    MERGE (a:Article {{title: row.title}})
    ON CREATE SET 
        a.title = row.title,
        a.namespace_id = row.namespace_id,
        a.namespace_name = row.namespace_name,
        a.namespace_type = row.namespace_type,
        a.parent_id = toInteger(row.parent_id),
        a.timestamp = row.timestamp,
        a.sha1 = row.sha1,
        a.path = row.path;
    """
    import_data(driver, csv_dir, 'articles.csv', article_query_template)

    click.echo("Importing authors...")
    author_query_template = """
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    MERGE (p:Author {{id: row.id}})
    ON CREATE SET
        p.id = row.id,
        p.name = row.name;
    """
    import_data(driver, csv_dir, 'persons.csv', author_query_template)

    click.echo("Importing author links...")
    author_links_query_template = """
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    MATCH (article:Article {{title: row.article}})
    MATCH (author:Author {{id: row.person}})
    MERGE (author)-[:AUTHORED]->(article)
    """
    import_data(driver, csv_dir, 'author_links.csv', author_links_query_template)

    click.echo("Importing article links...")
    article_links_query_template = """
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    MATCH (a:Article {{title: row.from}})
    MATCH (b:Article {{title: row.to}})
    MERGE (a)-[:LINKS_TO]->(b)
    """
    import_data(driver, csv_dir, 'article_links.csv', article_links_query_template)

    click.echo("Data import process completed successfully.")

def get_authors(info):
    """
    Extract author information from a JSON 'info' dictionary.

    :param info: JSON dictionary containing 'authors' data.
    :return: List of tuples (author_id, author_name).
    """
    res = []
    for author in info["authors"]:
        author_id = author.get('id') or author.get('name')
        author_name = author.get('name') or author.get('id')
        if author_id:
            res.append((author_id, author_name))
    return res

def save_csv(df, path):
    """
    Save a pandas DataFrame to a CSV file.

    :param df: Pandas DataFrame to be saved.
    :param path: Destination path for the CSV file.
    """
    df.to_csv(path, index=False, escapechar='\\')

def process_path(args: tuple[Path, Path]):
    """
    Process a directory to extract article, author, and link information from JSON files 
    and save them as CSV files.

    :param args: Tuple containing the source path and destination CSV directory.
    """
    try:
        path, csv_dir = args

        articles = []
        persons = set()
        article_links = []
        author_links = []

        # Iterate through all JSON files in the directory
        for file in path.rglob('*.json'):
            try:
                data = json.loads(file.read_text())
            except json.decoder.JSONDecodeError as e:
                logger.error(f"Error decoding JSON {file}: {e}")

            # Skip redirect type articles
            if data["type"] == "redirect":
                continue

            info = data["info"]

            # Collect article data
            articles.append({
                "id": info["info"]["id"],
                "title": info["title"],
                "type": data["type"],
                "namespace_id": info["info"]["namespace"],
                "namespace_name": info["namespace"]["name"],
                "namespace_type": info["namespace"]["type"],
                "parent_id": info["parent_id"],
                "timestamp": info["timestamp"],
                "sha1": info["sha1"],
                "path": info["bucket"] + '/' + info["file_name"]
            })

            # Collect author data
            authors = get_authors(info)
            persons.update(authors)
            for author in authors:
                author_links.append({
                    "article": info["title"],
                    "person": author[0]
                })

            # Collect article links
            for target, _ in data["links"]:
                if '#' in target:
                    target = target.split('#')[0]
                    if not target:
                        continue
                article_links.append({
                    "from": info["title"],
                    "to": target
                })

        # Save the collected data as CSV files
        articles_df = pd.DataFrame(articles)
        target = csv_dir / path.name
        target.mkdir(exist_ok=True, parents=True)
        save_csv(articles_df, target / 'articles.csv')

        persons_df = pd.DataFrame(list(persons), columns=["id", "name"])
        save_csv(persons_df, target / 'persons.csv')

        article_links_df = pd.DataFrame(article_links)
        save_csv(article_links_df, target / 'article_links.csv')

        author_links_df = pd.DataFrame(author_links)
        save_csv(author_links_df, target / 'author_links.csv')
    except Exception as e:
        logger.error(f"Error processing {path}: {e}")

@cli.command()
@click.argument('dir', type=click.Path(exists=True, path_type=Path))
@click.argument('csv_dir', type=click.Path(path_type=Path))
def create_csv(dir: Path, csv_dir: Path):
    """
    Process a directory containing subdirectories of JSON files and 
    generate CSV files for articles, authors, article links, and author links.

    :param dir: Directory containing subdirectories of JSON files.
    :param csv_dir: Directory where the CSV files will be saved.
    """
    csv_dir.mkdir(exist_ok=True, parents=True)

    # Prepare job tuples for multiprocessing
    jobs = [(p, csv_dir) for p in dir.iterdir() if p.is_dir()]
    process_map(process_path, jobs, max_workers=mp.cpu_count())

if __name__ == '__main__':
    cli()

