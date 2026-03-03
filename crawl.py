import shutil, time, threading, os, glob, json
import click
from rich.progress import Progress, TaskID, Console, track
from rich.live import Live
from rich.progress import SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from GoodreadsScraper.items import BookItem, AuthorItem


@click.group()
@click.option("--log_file",
              help="Log file for scrapy logs",
              type=str,
              default="scrapy.log",
              show_default=True)
@click.pass_context
def crawl(ctx, log_file="scrapy.log"):
    ctx.ensure_object(dict)
    ctx.obj['LOG_FILE'] = log_file
    ctx.max_content_width = 100
    ctx.show_default = True


@crawl.command()
@click.option("--list_name",
              required=True,
              help="Goodreads Listopia list name.",
              prompt=True,
              type=str)
@click.option("--start_page", help="Start page number", default=1, type=int)
@click.option("--end_page",
              required=True,
              help="End page number",
              prompt=True,
              type=int)
@click.option("--output_file_suffix",
              help="The suffix for the output file. [default: list name]",
              type=str)
@click.pass_context
def list(ctx, list_name: str, start_page: int, end_page: int,
         output_file_suffix: str):
    """Crawl a Goodreads Listopia List.

    Crawl all pages between start_page and end_page (inclusive) of a Goodreads Listopia List.

    \b
    The Listopia list name can be determined from the URL of the list.
    For e.g. the name of the list https://www.goodreads.com/list/show/1.Best_Books_Ever is '1.Best_Books_Ever'

    \b
    By default, two files will be created:
      1.   book_{output_file_suffix}.jl, for books from the given list
      2.   author_{output_file_suffix}.jl, for authors of the above books from the given list
    """
    if not output_file_suffix:
        output_file_suffix = list_name
    click.echo(
        f"Crawling Goodreads list {list_name} for pages [{start_page}, {end_page}]"
    )

    # On Goodreads, each page of a list has about 100 books
    # The last page may have less
    books_per_page = 100
    estimated_no_of_books = (end_page - start_page + 1) * books_per_page

    progress_updater = ProgressUpdater()

    with progress_updater.progress:
        progress_updater.add_task_for(BookItem,
                                      description="[red]Scraping books...",
                                      total=estimated_no_of_books)
        progress_updater.add_task_for(AuthorItem,
                                      description="[green]Scraping authors...",
                                      total=estimated_no_of_books)

        _crawl('list',
               ctx.obj["LOG_FILE"],
               output_file_suffix,
               list_name=list_name,
               start_page_no=start_page,
               end_page_no=end_page,
               item_scraped_callback=progress_updater)


@crawl.command()
@click.option("--output_file_suffix",
              help="The suffix for the output file. Defaults to 'all'",
              type=str)
@click.pass_context
def author(ctx, output_file_suffix='all'):
    """Crawl all authors on Goodreads.

    [IMPORTANT]: This command will only complete after it has crawled
    ALL the authors on Goodreads, which may be a long time.
    For all intents and purposes, treat this command as a never-terminating one
    that will block the command-line forever.

    It is STRONGLY RECOMMENDED that you either terminate it manually (with an interrupt) or
    run it in the background.
    """
    click.echo("Crawling Goodreads for all authors")
    click.echo(
        click.style(
            "[WARNING] This command will block the CLI, and never complete (unless interrupted).\n"
            "Run it in the background if you don't want to block your command line.",
            fg='red'))

    progress_updater = ProgressUpdater(infinite=True)

    with progress_updater.progress:
        progress_updater.add_task_for(AuthorItem,
                                      description="[green]Scraping authors...")

        _crawl('author',
               ctx.obj["LOG_FILE"],
               output_file_suffix,
               author_crawl=True,
               item_scraped_callback=progress_updater)


@crawl.command()
@click.option(
    "--user_id",
    required=True,
    help="The user ID. This can be determined from the URL in your profile, and is of the form '123456-foo-bar'",
    prompt=True,
    type=str)
@click.option("--shelf",
              type=click.Choice(
                  ["read", "to-read", "currently-reading", "all"]),
              help="A shelf from the user's 'My Books' tab.",
              default="all")
@click.option("--output_file_suffix",
              help="The suffix for the output file. [default: user_id]",
              type=str)
@click.pass_context
def my_books(ctx, user_id: str, shelf: str, output_file_suffix: str):
    """Crawl shelves from the "My Books" tab for a user."""
    if not output_file_suffix:
        output_file_suffix = user_id
    click.echo(f"Crawling Goodreads profile {user_id} for shelf {shelf}")

    # On "My Books", each page of has about ~30 books
    # The last page may have less
    # However, we don't know how many total books there could be on a shelf
    # So until we can figure it out, show an infinite spinner
    progress_updater = ProgressUpdater(infinite=True)

    with progress_updater.progress:
        progress_updater.add_task_for(BookItem,
                                    description=f"[red]Scraping books for shelf '{shelf}'...")
        progress_updater.add_task_for(AuthorItem,
                                    description=f"[green]Scraping authors for shelf '{shelf}'...")

        _crawl('mybooks',
            ctx.obj["LOG_FILE"],
            f"{output_file_suffix}",
            user_id=user_id,
            shelf=shelf,
            item_scraped_callback=progress_updater)

def backup_scheduler(job_directory):
    """
    Creates a backup every 10 mins.
    Ensures ONLY the 3 most recent backups exist.
    """
    while True:
        time.sleep(600) # Wait 10 minutes

        timestamp = int(time.time())
        backup_path = f"{job_directory}_backup_{timestamp}"

        try:
            if os.path.exists(job_directory):
                # 1. Create the new backup
                shutil.copytree(job_directory, backup_path)

                # 2. Enforce the limit: Delete oldest if we have > 3
                # Get all backup folders and sort them (Oldest -> Newest)
                all_backups = sorted(glob.glob(f"{job_directory}_backup_*"))

                # Delete until only 3 remain
                while len(all_backups) > 3:
                    oldest_backup = all_backups.pop(0) # Get the first (oldest) item
                    shutil.rmtree(oldest_backup)       # Delete it from disk

        except Exception as e:
            # If files are locked, just try again next cycle
            pass

def _crawl(spider_name, log_file, output_file_suffix, **crawl_kwargs):
    settings = get_project_settings()

    # used by the JsonLineItem pipeline
    settings.set("OUTPUT_FILE_SUFFIX", output_file_suffix)

    # do not flood with messages
    settings.set("LOG_LEVEL", "DEBUG")

    # Creates a unique folder for this specific crawl's state
    job_directory = f"crawls/{spider_name}_{output_file_suffix}"
    settings.set("JOBDIR", job_directory)

    #settings.set("HTTPCACHE_ENABLED", True)
    #settings.set("HTTPCACHE_EXPIRATION_SECS", 0) # Never expire
    #settings.set("HTTPCACHE_DIR", 'httpcache')

    # --- START BACKUP LOGIC ---
    backup_thread = threading.Thread(
        target=backup_scheduler,
        args=(job_directory,),
        daemon=True
    )
    backup_thread.start()
    # --- END BACKUP LOGIC ---

    process = CrawlerProcess(settings)

    process.crawl(spider_name, **crawl_kwargs)

    # CLI will block until this call completes
    process.start()

class ProgressUpdater():
    """Callback class for updating the progress on the console.

        Internally, this maintains a map from the item type to a TaskID.
        When the callback is invoked, it tries to find a match for the scraped item,
        and advance the corresponding task progress.
    """

    def __init__(self, infinite=False):
        if infinite:
            self.progress = Progress(
                "[progress.description]{task.description}",
                TimeElapsedColumn(),
                TextColumn("{task.completed} items scraped"), SpinnerColumn())
        else:
            self.progress = Progress(
                "[progress.description]{task.description}", BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TimeRemainingColumn(), "/", TimeElapsedColumn())
        self.item_type_to_task = {}

    def add_task_for(self, item_type, *args, **kwargs) -> TaskID:
        task = self.progress.add_task(*args, **kwargs)
        self.item_type_to_task[item_type] = task
        return task

    def __call__(self, item, spider):
        item_type = type(item)
        task = self.item_type_to_task.get(item_type, None)
        if task is not None:
            self.progress.advance(task)


@crawl.command()
@click.option("--author_file", required=True, help="Path to the author .jl file")
@click.option("--output_suffix", default="books_batch", help="Suffix for output file")
@click.pass_context
def crawl_books(ctx, author_file, output_suffix):
    """Scrapes books found in an existing author file."""

    # 1. Extract URLs from the author file
    book_urls = set()
    click.echo(f"Reading {author_file}...")

    try:
        with open(author_file, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    # Use the new field 'book_urls' we created earlier
                    if 'bookURLs' in data:
                        for url in data['bookURLs']:

                          # 1. SKIP non-book links (like /series/ or /genres/)
                            if "/book/show" not in url:
                                continue

                          # 2. Fix relative URLs
                            if not url.startswith('http'):
                                url = "https://www.goodreads.com" + url
                            book_urls.add(url)
                except ValueError:
                    continue
    except FileNotFoundError:
        click.echo(f"Error: File {author_file} not found.")
        return

    if not book_urls:
        click.echo("No book URLs found. Did you update AuthorSpider to save 'book_urls'?")
        return

    click.echo(f"Found {len(book_urls)} unique books. Starting scrape...")

    # 2. Run the BookSpider with these URLs
    # We pass crawl_author=False to prevent infinite loops
    progress_updater = ProgressUpdater(infinite=True)

    with progress_updater.progress:
        progress_updater.add_task_for(BookItem, description="Scraping books...")

        _crawl('book',
               ctx.obj["LOG_FILE"],
               output_suffix,
               book_urls=list(book_urls), # Passes the list to __init__
               crawl_author="False",      # IMPORTANT: Disable author recursion
               item_scraped_callback=progress_updater)

if __name__ == "__main__":
    crawl()
