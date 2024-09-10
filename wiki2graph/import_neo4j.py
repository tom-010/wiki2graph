import logging
import click
from neo4j import GraphDatabase, Driver
from pathlib import Path
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map
import multiprocessing as mp
import json
import pandas as pd 
import timeit
import datetime

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

class Importer:
    '''
    A wrapper around import_data_ to defer execution.
    In this way, I can first declare all the work, get an overview and then actually work

    Note: It does not make to parallelize the import as Neo4j uses transactions, which forces 
    the workers to wait for each other, thus only worker is working at a time.
    Therefore I opted to import via CSV to optimize on the bottleneck (and have a checkpoint between
    using the JSON files and adding the data)
    '''

    def __init__(self, driver: Driver):
        self.driver = driver
        self.jobs = []
        self.current_step: tuple['step', tuple['args', 'kwargs']] = None

    def add(self, *args, **kwargs):
        '''
        Add a call to import_data_ to the worker
        '''
        self.jobs.append((self.current_step, (args, kwargs)))

    def run(self):
        '''
        After declaring your work, run it. 
        '''
        last_step = None
        click.echo("Starting data import process...")

        total_seconds = sum([step['expected_seconds'] for step, _ in self.jobs if step.get('expected_seconds')])
        seconds_done = 0

        start = timeit.default_timer()
        start_datetime = datetime.datetime.now()

        for step, (args, kwargs) in self.jobs:

            if step and step != last_step:
                percent_done = seconds_done / total_seconds
                seconds_since_start = timeit.default_timer() - start
                expected_minutes_for_this_step = f"{int(step['expected_seconds'] // 60)}:{int(step['expected_seconds'] % 60)}"
                if percent_done != 0:
                    estimated_time = seconds_since_start / percent_done if percent_done > 0 else 0
                    estimated_remaining = estimated_time - seconds_since_start
                    estimated_remaining_minutes_str = f"{int(estimated_remaining // 60)}:{int(estimated_remaining % 60)}"

                    finish_datetime = start_datetime + datetime.timedelta(seconds=estimated_time)
                    finish_datetime_str = finish_datetime.strftime('%X')


                    click.echo(f"{step['name']}, {estimated_remaining_minutes_str } min remaining until {finish_datetime_str} h, expecting {expected_minutes_for_this_step} min for this step")
                else:
                    # done == 0%
                    finish_datetime = start_datetime + datetime.timedelta(seconds=total_seconds)
                    finish_datetime_str = finish_datetime.strftime('%X')
                    total_minutes = f'{int(total_seconds // 60)}:{int(total_seconds % 60)}'
                    # click.echo(f"{step['name']}, {total_minutes} min remaining until {finish_datetime_str} h, expecting {expected_minutes_for_this_step} min for this step")
                    click.echo(f"{step['name']}") # We need one step to calculate an estimate, don't give a fake-estimate

                last_step = step

            self.import_data(*args, **kwargs)
            seconds_done += step['expected_seconds']

            

        stop = timeit.default_timer()

    def import_data(self, csv_files: list[Path], query_template: str) -> None:
        """
        Generic function to import data from CSV files using a Cypher query.
        -> You should use `Importer.add` to use this method!

        :param csv_dir: Directory containing CSV files.
        :param pattern: Glob pattern to match the relevant CSV files.
        :param query_template: Cypher query template with placeholders for file paths.
        """

        driver = self.driver

        paths = list(csv_files)

        for csv_file in tqdm(paths):
            csv_file_path = csv_file.as_posix()
            query = query_template.format(csv_file_path=csv_file_path)
            try:
                res = run_cypher_query(driver, query)
                # logger.info(f"Successfully imported {csv_file_path}")
            except Exception as e:
                logger.error(f"Error importing {csv_file_path}: {e}")


    def step(self, name:str, expected_time:str=None):
        '''
        Declare a step. After this you can add stuff to the importer. They are then part of 
        the step. You can set an expected time on your machine. Only the proportions are used. 
        Time is then measured of the steps and and time estimate is given to the used along with the 
        name of the step.
        '''

        parts = expected_time.split(':', maxsplit=1)
        expected_seconds = int(parts[0]) * 60 + int(parts[1])

        self.current_step = {
            'name': name,
            'expected_seconds': expected_seconds
        }

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
    path = Path(csv_dir)

    # Inline driver creation
    driver = GraphDatabase.driver(uri, auth=(username, password))

    click.echo("Creating indexes...")
    # Inline create_indexes
    with driver.session() as session:
        queries = [
            'CREATE INDEX FOR (a:Article) ON (a.title);',
            'CREATE INDEX FOR (c:Category) ON (c.title);',
            'CREATE INDEX FOR (a:Author) ON (a.id);',
            'CREATE INDEX FOR (s:Section) ON (s.id);'
        ]
        for query in queries:
            try:
                session.run(query)
            except Exception as e:
                print(e) # OK if index already exists


    importer = Importer(driver)

    logger.info("Indexes created")

    importer.step("Importing articles...", '2:30')
    t = """
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
    importer.add(path.rglob('**/articles.csv'), t)


    importer.step("Importing authors...", '01:09')
    t = """
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    MERGE (p:Author {{id: row.id}})
    ON CREATE SET
        p.id = row.id,
        p.name = row.name;
    """
    importer.add(path.rglob('**/persons.csv'), t)


    importer.step("Importing author links...", '02:17')
    t = """
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    MATCH (article:Article {{title: row.article}})
    MATCH (author:Author {{id: row.person}})
    MERGE (author)-[:AUTHORED]->(article)
    """
    importer.add(path.rglob('**/author_links.csv'), t)

    importer.step("Importing redirects...", '00:59')
    t = """
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    MATCH (a:Article {{title: row.from}})
    MATCH (b:Article {{title: row.to}})
    MERGE (a)-[:REDIRECTS_TO]->(b)
    """
    importer.add(path.rglob('**/redirects.csv'), t)

    importer.step("Importing sections...", '04:42')
    t = """
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    MERGE (s:Section {{id: row.id}})
    ON CREATE SET
        s.id = row.id,
        s.title = row.title,
        s.level = row.level,
        s.idx = row.idx;
    """
    importer.add(path.rglob('**/sections.csv'), t)


    importer.step("Importing categories...", '01:38')
    t = """
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    MERGE (c:Category {{title: row.title}})
    ON CREATE SET
        c.id = row.id,
        c.title = row.title,
        c.type = row.type,
        c.namespace_id = row.namespace_id,
        c.namespace_name = row.namespace_name,
        c.namespace_type = row.namespace_type,
        c.parent_id = row.parent_id,
        c.timestamp = row.timestamp,
        c.sha1 = row.sha1,
        c.path = row.path;
    """
    importer.add(path.rglob('**/categories.csv'), t)


    importer.step("Importing category/article relationships...", '04:58')
    t = """
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    MATCH (a:Article {{title: row.article}})
    MATCH (c:Category {{title: row.category}})
    MERGE (a)-[:IS_IN_CATEGORY]->(c)
    """
    importer.add(path.rglob('**/article_to_category.csv'), t)


    importer.step("Importing article to section links...", '02:05')
    # The links to specific sections
    t = """
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    MATCH (a:Article {{title: row.article}})
    MATCH (s:Section {{id: row.section}})
    MERGE (a)-[:LINKS_TO]->(s)
    """
    importer.add(path.rglob('**/article_to_section_links.csv'), t)

    importer.step("Importing the part-of relationships between sections and articles...", '06:00')
    t = """
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    MATCH (s:Section {{id: row.from}})
    MATCH (a:Article {{title: row.to}})
    MERGE (s)-[:PART_OF]->(a)
    """
    importer.add(path.rglob('**/section_links.csv'), t)


    importer.step("Importing the links from sections to articles...", '50:00')
    t = """
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    MATCH (s:Section {{id: row.from}})
    MATCH (a:Article {{title: row.to}})
    MERGE (s)-[:LINKS_TO]->(a)
    """
    importer.add(path.rglob('**/section_to_article_links.csv'), t)



    importer.step("Importing article links...", '50:00')
    t = """
    LOAD CSV WITH HEADERS FROM 'file:///{csv_file_path}' AS row
    MATCH (a:Article {{title: row.from}})
    MATCH (b:Article {{title: row.to}})
    MERGE (a)-[:LINKS_TO]->(b)
    """
    importer.add(path.rglob('**/article_links.csv'), t)

    importer.run()

    # click.echo("Data import process completed successfully.")

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

def save_csv(objects, path, *args, **kwargs):
    """
    Converts a list of dicts to a pandas DataFrame and saves it

    :param objects: list[dict] data to be stored
    :param path: Destination path for the CSV file.
    :param *args: Passed to pd.DataFrame
    :param **kwargs: Passed to pd.DataFrame
    """
    pd.DataFrame(objects, *args, **kwargs).to_csv(path, index=False, escapechar='\\')

def process_path(args: tuple[Path, Path]):
    """
    Process a directory to extract article, author, and link information from JSON files 
    and save them as CSV files.

    :param args: Tuple containing the source path and destination CSV directory.
    """
    try:
        path, csv_dir = args

        articles = [] # the article-nodes
        persons = set() # authors of articles (their revisions)
        article_links = [] # links between two articles
        article_to_section_links = [] # the link of an article to a specific section, not the article
        author_links = [] # links between the articles and their authors 
        redirects = [] # redirectects between two articles
        categories = [] # categories of articles (nodes)
        article_to_category = [] # links between articles and categories
        sections = [] # an article consits of many sections. The secttions as nodes
        section_links = [] # links the sections to their articles, part-of

        section_to_article_links = [] # links from sections (nodes) to articles (nodes)
        section_to_section_links = [] # if a section links to a specific other section in an article

        # Iterate through all JSON files in the directory
        for file in path.rglob('*.json'):
            try:
                data = json.loads(file.read_text())
            except json.decoder.JSONDecodeError as e:
                logger.error(f"Error decoding JSON {file}: {e}")

            info = data["info"]

            # Special case for redirect pages
            if data["type"] == "redirect":
                target = data["target"].split('#')[0]
                redirects.append({
                    "from": data["info"]["title"],
                    "to": target
                })
                continue

            if info["info"]["namespace"] == 14:
                # handle category
                categories.append({
                    "id": info["info"]["id"],
                    "title": 'Kategorie:' + info["title"],
                    "type": data["type"],
                    "namespace_id": info["info"]["namespace"],
                    "namespace_name": info["namespace"]["name"],
                    "namespace_type": info["namespace"]["type"],
                    "parent_id": info["parent_id"],
                    "timestamp": info["timestamp"],
                    "sha1": info["sha1"],
                    "path": info["bucket"] + '/' + info["file_name"]
                })
                continue
            
            for section in data.get('sections', []):
                s = section['section']
                s['title'] = s['title'].strip()
                s['id'] = f"{info['title']}#{s['title']}"[:400]

                sections.append(s)

                section_links.append({
                    "from": s['id'],
                    "to": info['title']
                })

                for link in section['links']:
                    target = link['target']
                    if '#' in link:
                        section_to_section_links.append({
                            "from": s['id'],
                            "to": target,
                            "text": link['text'] or target
                        })
                        target = target.split('#')[0]

                    section_to_article_links.append({
                        "from": s['id'],
                        "to": target,
                        "text": link['text'] or target
                    })



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

                # Special case for section links
                if '#' in target:
                    article_to_section_links.append({
                        "article": info["title"],
                        "section": target[:400]
                    })

                    target = target.split('#')[0]
                    if not target:
                        continue

                
                article_links.append({
                    "from": info["title"],
                    "to": target
                })


            for category in data.get('categories', []):
                category_title = f'Kategorie:{category}'
                article_to_category.append({
                    "article": info["title"],
                    "category": category_title
                })


        # Save the collected data as CSV files
        target = csv_dir / path.name
        target.mkdir(exist_ok=True, parents=True)

        save_csv(articles, target / 'articles.csv')
        save_csv(list(persons), target / 'persons.csv', columns=["id", "name"])
        save_csv(article_links, target / 'article_links.csv')
        save_csv(author_links, target / 'author_links.csv')
        save_csv(redirects, target / 'redirects.csv')
        save_csv(categories, target / 'categories.csv')
        save_csv(article_to_category, target / 'article_to_category.csv')
        save_csv(sections, target / 'sections.csv')
        save_csv(section_links, target / 'section_links.csv')
        save_csv(article_to_section_links, target / 'article_to_section_links.csv')
        save_csv(section_to_article_links, target / 'section_to_article_links.csv')
        save_csv(section_to_section_links, target / 'section_to_section_links.csv')

    except Exception as e:
        logger.error(f"Error processing {path}: {e}")

@cli.command()
@click.argument('dir', type=click.Path(exists=True, path_type=Path))
@click.argument('csv_dir', type=click.Path(path_type=Path))
@click.option('--max-items', default=None, help="Maximum number of items to process.")
def create_csv(dir: Path, csv_dir: Path, max_items: int = None):
    """
    Process a directory containing subdirectories of JSON files and 
    generate CSV files for articles, authors, article links, and author links.

    :param dir: Directory containing subdirectories of JSON files.
    :param csv_dir: Directory where the CSV files will be saved.
    """
    csv_dir.mkdir(exist_ok=True, parents=True)

    # Prepare job tuples for multiprocessing
    jobs = [(p, csv_dir) for p in dir.iterdir() if p.is_dir()]
    if max_items:
        jobs = jobs[:max_items]
    process_map(process_path, jobs, max_workers=mp.cpu_count(), chunksize=10)

if __name__ == '__main__':
    cli()

