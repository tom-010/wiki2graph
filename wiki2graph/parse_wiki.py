"""
This module processes Wikipedia dump files by extracting individual articles and parsing them into a structured 
format, making them easier to work with for downstream tasks like data analysis, machine learning, or further processing.

### Overview:
- The script primarily handles Wikipedia dump files (large archives) and facilitates the extraction of individual 
  Wikipedia articles from these dumps.
- After extraction, the articles can be parsed and converted into JSON files, which include metadata and structured 
  content such as sections, links, and categories.

### Commands:
1. **`extract`**: Extracts articles from a Wikipedia dump file.
    - **Usage**: 
      ```
      python script_name.py extract <DUMP_FILE> <OUTPUT_DIR> [OPTIONS]
      ```
    - **Arguments**:
      - `dump_file`: Path to the Wikipedia dump file.
      - `output_dir`: Directory where the extracted articles should be saved.
    - **Options**:
      - `--force`: Overwrite existing files if they already exist.
      - `--max-pages`: Limit the number of pages to extract.
    - **Description**: 
      This command reads the Wikipedia dump file and extracts each article into a separate file. Each file contains a 
      JSON metadata header and the article's mediawiki markup. Files are organized into buckets to avoid overloading 
      directories with too many files.

2. **`parse`**: Parses the extracted articles into a structured JSON format.
    - **Usage**: 
      ```
      python script_name.py parse <INPUT_DIR> <OUTPUT_DIR> [OPTIONS]
      ```
    - **Arguments**:
      - `input_dir`: Directory containing the extracted articles.
      - `output_dir`: Directory where the parsed JSON files should be saved.
    - **Options**:
      - `--max-articles`: Limit the number of articles to process.
      - `--processes`: Number of parallel processes to use.
      - `--force`: Overwrite existing files if they already exist.
    - **Description**: 
      This command processes the extracted articles, converting them into JSON files that are more structured and easier 
      to work with. The JSON files include metadata, section information, and links, making it straightforward to use the 
      data in downstream tasks.

### Why:
This module is designed to facilitate the processing of large Wikipedia dump files. By extracting and parsing articles 
into a structured format, it enables easier and more efficient analysis, search, and manipulation of Wikipedia content. 
This is particularly useful for research, building knowledge graphs, or any application that requires access to structured 
data from Wikipedia.
"""

import mwxml # To extract the pages
import os
from tqdm import tqdm
from pathlib import Path
from slugify import slugify # To normalize to filenames
from hashlib import md5
import json
import click
from pathlib import Path
import mwcomposerfromhell # mediawiki markup to html
import mwparserfromhell # Parse the mediawiki markup 
import re
from multiprocessing import Pool
from rich import print
import traceback

@click.group()
def cli():
    """Command-line interface for processing Wikipedia dump files."""
    pass

def save_page(page, output_dir: Path, force=False):
    """
    Saves an extracted Wikipedia page to a file with metadata.

    Parameters
    ----------
    page : mwxml.Page
        A page object extracted from the Wikipedia dump using mwparserfromhell.
    output_dir : Path
        The directory where the page will be stored.
    force : bool, optional
        If True, overwrite existing files. Default is False.

    Notes
    -----
    The file is saved in a subdirectory based on a hash of the title to avoid 
    creating too many files in a single directory. The first line of the file 
    contains metadata in JSON format, and the remainder is the mediawiki markup.
    """
    title = slugify(page.title)
    bucket = md5(title.encode('utf-8')).hexdigest()[:3]
    file_name = f"{title}.wiki"
    file_path = Path(output_dir) / bucket / file_name

    if file_path.exists() and not force:
        return

    file_path.parent.mkdir(parents=True, exist_ok=True)

    revision = None
    authors = []
    for rev in page: # TODO: handle multiple revisions
        revision = rev
        authors.append({
            'id': rev.user.id,
            'name': rev.user.text           
        })

    if revision is None:
        print(f"Skipping page '{title}' because no revisions are found.")
        return

    metadata_str = json.dumps({
        'title': page.title,
        'authors': authors,
        'bucket': bucket,
        'file_name': file_name,
        'info': page.to_json(),
        'sha1': revision.sha1,
        'timestamp': str(revision.timestamp),
        'parent_id': revision.parent_id,
    })

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(metadata_str + '\n' + (revision.text or ''))

@cli.command()
@click.argument('dump_file', type=click.Path(exists=True, path_type=Path, dir_okay=False))
@click.argument('output_dir', type=click.Path(path_type=Path, dir_okay=True, file_okay=False))
@click.option('--force', is_flag=True, help='Overwrite existing files')
@click.option('--max-pages', type=int, help='Maximum number of pages to extract')
def extract(dump_file: Path, output_dir: Path, force: bool = False, max_pages: int = None):
    """
    Extracts Wikipedia articles from a dump file and saves them as individual files.

    Parameters
    ----------
    dump_file : Path
        Path to the Wikipedia dump file.
    output_dir : Path
        Directory where the extracted files should be saved.
    force : bool, optional
        If True, overwrite existing files. Default is False.
    max_pages : int, optional
        Maximum number of pages to extract. If not set, all pages are extracted.

    Notes
    -----
    Each extracted file contains a JSON metadata header followed by the 
    article's mediawiki markup. The process may take significant time depending 
    on the size of the dump and the number of pages.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(dump_file, 'rb') as f:
        dump = mwxml.Dump.from_file(f)
        for idx, page in enumerate(tqdm(dump, total=5_691_832 if max_pages is None else max_pages)):
            if max_pages is not None and idx >= max_pages:
                break
            try:
                save_page(page, output_dir, force)
            except Exception as e:
                print(f"Error processing page '{page.title}': {e}")
                traceback.print_exc()

def list_paths_by_suffix(directory='.', suffix='.wiki'):
    """
    Generator that finds all file paths with the given suffix in a directory.

    Parameters
    ----------
    directory : str, optional
        Directory to search in. Default is the current directory.
    suffix : str, optional
        File suffix to search for. Default is '.wiki'.

    Yields
    ------
    Path
        Paths to files with the specified suffix.
    """
    path = Path(directory)
    for file in path.rglob(f'*{suffix}'):
        yield file

namespaces = {
    0: {'name': '(Main/Article)', 'type': 'subject'},
    1: {'name': 'Talk', 'type': 'talk'},
    2: {'name': 'User', 'type': 'subject'},
    3: {'name': 'User talk', 'type': 'talk'},
    4: {'name': 'Wikipedia', 'type': 'subject'},
    5: {'name': 'Wikipedia talk', 'type': 'talk'},
    6: {'name': 'File', 'type': 'subject'},
    7: {'name': 'File talk', 'type': 'talk'},
    8: {'name': 'MediaWiki', 'type': 'subject'},
    9: {'name': 'MediaWiki talk', 'type': 'talk'},
    10: {'name': 'Template', 'type': 'subject'},
    11: {'name': 'Template talk', 'type': 'talk'},
    12: {'name': 'Help', 'type': 'subject'},
    13: {'name': 'Help talk', 'type': 'talk'},
    14: {'name': 'Category', 'type': 'subject'},
    15: {'name': 'Category talk', 'type': 'talk'},
    100: {'name': 'Portal', 'type': 'subject'},
    101: {'name': 'Portal talk', 'type': 'talk'},
    118: {'name': 'Draft', 'type': 'subject'},
    119: {'name': 'Draft talk', 'type': 'talk'},
    710: {'name': 'TimedText', 'type': 'subject'},
    711: {'name': 'TimedText talk', 'type': 'talk'},
    828: {'name': 'Module', 'type': 'subject'},
    829: {'name': 'Module talk', 'type': 'talk'},
}

def parse_extracted_article(info: dict, text: str) -> dict:
    """
    Parses a Wikipedia article's text and metadata into a structured format.

    Parameters
    ----------
    info : dict
        Metadata about the article extracted from the dump.
    text : str
        The raw mediawiki markup text of the article.

    Returns
    -------
    dict
        A structured representation of the article, including sections, links, 
        and categories.

    Notes
    -----
    This function identifies redirects, parses sections, and extracts links 
    and categories from the article. The output can be used for further 
    processing or analysis.
    """
    info['namespace'] = namespaces.get(info.get('info', {}).get('namespace'), {'name': 'Unknown', 'type': 'unknown'})

    # Check if the article is a redirect
    t = text[:100].lower().strip()
    if t.startswith('#redirect') or t.startswith('#weiterleitung'):
        # Fast parsing using regex for redirects
        pattern = re.compile('\[\[(.*)\]\]')
        matches = pattern.findall(text)
        if matches:
            return {
                'info': info,
                'title': info['title'],
                'type': 'redirect',
                'target': matches[0]
            }

    # Parse the mediawiki markup
    wikicode = mwparserfromhell.parse(text)

    # Extract sections
    sections = wikicode.get_sections(include_lead=True, levels=[1, 2, 3, 4, 5, 6])

    res = {
        'info': info,
        'type': info['namespace']['name'],
        'title': info['title'],
        'sections': [],
        'categories': [],
        'type': 'article'
    }

    seen_links = set()

    for idx, section in enumerate(sections):
        section_info = {
            'idx': idx,
        }

        headings = section.filter_headings()
        if headings:
            heading = headings[0]
            section_info['title'] = str(heading.title)
            section_info['level'] = str(heading.level)
        elif idx == 0:
            try:
                section_info['title'] = str(section.nodes[0].contents)
                res['title'] = section_info['title']
            except:
                section_info['title'] = 'Introduction'
            section_info['level'] = 1

        for link in section.filter_wikilinks():
            seen_links.add((str(link.title), str(link.text) if link.text else None))

        section_links = [{
            'target': str(link.title),
            'text': str(link.text) if link.text else None,
        } for link in section.filter_wikilinks()]

        html = ''
        try:
            html = str(mwcomposerfromhell.compose(section))
        except Exception as e:
            print(e, '(Error parsing HTML)')
            pass
        res['sections'].append({
            'section': section_info,
            "html": html,
            "wiki": str(section),
            "links": section_links
        })

    links = []
    for link in wikicode.filter_wikilinks():
        links.append((str(link.title), str(link.text) if link.text else None))

    article_links = set(links) - seen_links

    res['links'] = links
    res['non_section_links'] = sorted(article_links)
    res['categories'] = []

    for link in links:
        link, _ = link

        if ':' not in link:
            continue

        link_type, target = link.split(':', 1)

        if link_type.lower() == 'kategorie':
            res['categories'].append(target)

    return res

def parse_extracted_article_path(article_path: Path, articles_dir: Path, output_dir: Path, force=False):
    """
    Parses and saves a structured JSON from an extracted article file.

    Parameters
    ----------
    article_path : Path
        Path to the extracted article file.
    articles_dir : Path
        Directory containing the extracted articles.
    output_dir : Path
        Directory where the parsed JSON files will be saved.
    force : bool, optional
        If True, overwrite existing files. Default is False.

    Notes
    -----
    The main logic is handled by `parse_extracted_article`, while this function 
    manages file input/output.
    """
    text = article_path.read_text()
    target = output_dir / article_path.relative_to(articles_dir)
    target = target.with_suffix('.json')

    if target.exists() and not force:
        return

    info, text = text.split('\n', 1)
    info = json.loads(info)

    res = parse_extracted_article(info, text)
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, 'w') as f:
        json.dump(res, f, indent=2)

def __parse_extracted_article_path_wrapper(args):
    """
    Wrapper for multiprocessing to handle argument unpacking.

    Parameters
    ----------
    args : tuple
        Arguments for `parse_extracted_article_path`.

    Notes
    -----
    This wrapper is necessary for multiprocessing pool map, as it only handles 
    one argument. Lambdas can't be used with multiprocessing because they are 
    not picklable.
    """
    try:
        article_path, articles_dir, output_dir, force = args
        return parse_extracted_article_path(article_path, articles_dir, output_dir, force)
    except Exception as e:
        print(e)
        traceback.print_exc()

def augment_iterator(it, *args, max_articles=None):
    """
    Augments an iterator with additional arguments for parallel processing.

    Parameters
    ----------
    it : iterable
        The original iterator.
    *args : any
        Additional arguments to append to each item.
    max_articles : int, optional
        Maximum number of items to yield. If None, yield all items.

    Yields
    ------
    tuple
        A tuple where the first item is from the original iterator, followed by 
        the additional arguments.
    """
    for idx, e in enumerate(it):
        if max_articles and idx >= max_articles:
            break
        yield (e,) + args

def _process_articles_in_parallel(articles_dir: Path, output_dir: Path, max_articles=None, force: bool = False, processes=3):
    """
    Processes articles in parallel, parsing them into structured JSON files.

    Parameters
    ----------
    articles_dir : Path
        Directory containing the extracted article files.
    output_dir : Path
        Directory where the parsed JSON files will be saved.
    max_articles : int, optional
        Maximum number of articles to process. If None, process all articles.
    force : bool, optional
        If True, overwrite existing files. Default is False.
    processes : int, optional
        Number of parallel processes to use. Default is 3.

    Notes
    -----
    This function uses a multiprocessing pool to parse articles concurrently, 
    improving efficiency on large datasets.
    """
    article_paths = list_paths_by_suffix(articles_dir, '.wiki')

    article_paths = augment_iterator(article_paths, articles_dir, output_dir, force, max_articles=max_articles)

    total = max_articles if max_articles is not None else 5273102
    with Pool(processes=processes) as pool:
        list(tqdm(pool.imap_unordered(__parse_extracted_article_path_wrapper, article_paths), total=total))

@cli.command()
@click.argument('input_dir', type=click.Path(exists=True, path_type=Path, dir_okay=True, file_okay=False))
@click.argument('output_dir', type=click.Path(path_type=Path, dir_okay=True, file_okay=False))
@click.option('--max-articles', type=int, help='Maximum number of articles to process')
@click.option('--processes', type=int, default=3, help='Number of processes to use')
@click.option('--force', is_flag=True, help='Overwrite existing files')
def parse(input_dir: Path, output_dir: Path, max_articles: int = None, processes: int = 3, force: bool = False):
    """
    Parses extracted Wikipedia articles into structured JSON files.

    Parameters
    ----------
    input_dir : Path
        Directory containing the extracted articles.
    output_dir : Path
        Directory where the parsed JSON files should be saved.
    max_articles : int, optional
        Maximum number of articles to process. If None, process all articles.
    processes : int, optional
        Number of parallel processes to use. Default is 3.
    force : bool, optional
        If True, overwrite existing files. Default is False.

    Notes
    -----
    The resulting JSON files contain metadata and structured content like 
    sections, links, and categories, making them easier to work with in 
    downstream tasks.
    """
    if processes < 1:
        import multiprocessing as mp
        processes = mp.cpu_count()

    articles_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    output_dir.mkdir(exist_ok=True, parents=True)
    _process_articles_in_parallel(articles_dir, output_dir, max_articles, force, processes)

if __name__ == '__main__':
    cli()
