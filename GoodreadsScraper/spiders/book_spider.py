"""Spider to extract information from a /book/show type page on Goodreads"""

import scrapy

from .author_spider import AuthorSpider
from ..items import BookItem, BookLoader

class BookSpider(scrapy.Spider):
    name = "book"

    def __init__(self, book_urls=None, crawl_author="True", *args, **kwargs):
        """
        :param book_urls: A comma-separated string or list of book URLs to scrape.
        :param crawl_author: "True" or "False". If False, stops the spider from
                             jumping back to the author page (prevents loops).
        """
        super().__init__(*args, **kwargs)
        # 1. Handle the URL input
        # Scrapy arguments often come as strings, so we handle both formats
        self.start_urls = []
        if book_urls:
            if isinstance(book_urls, str):
                # If passed from command line as a single string
                self.start_urls = book_urls.split(',')
            else:
                # If passed from python script as a list
                self.start_urls = book_urls

        # 2. Setup Author Spider logic
        self.should_crawl_author = str(crawl_author).lower() in ['true', '1', 'yes']
        if self.should_crawl_author:
            self.author_spider = AuthorSpider()

    def parse(self, response, loader=None):
        if not loader:
            loader = BookLoader(BookItem(), response=response)

        loader.add_value('url', response.request.url)

        # The new Goodreads page sends JSON in a script tag
        # that has these values

        loader.add_css('title', 'script#__NEXT_DATA__::text')
        loader.add_css('titleComplete', 'script#__NEXT_DATA__::text')
        loader.add_css('description', 'script#__NEXT_DATA__::text')
        loader.add_css('imageUrl', 'script#__NEXT_DATA__::text')
        loader.add_css('genres', 'script#__NEXT_DATA__::text')
        loader.add_css('asin', 'script#__NEXT_DATA__::text')
        loader.add_css('isbn', 'script#__NEXT_DATA__::text')
        loader.add_css('isbn13', 'script#__NEXT_DATA__::text')
        loader.add_css('publisher', 'script#__NEXT_DATA__::text')
        loader.add_css('series', 'script#__NEXT_DATA__::text')
        loader.add_css('author', 'script#__NEXT_DATA__::text')
        loader.add_css('publishDate', 'script#__NEXT_DATA__::text')

        loader.add_css('characters', 'script#__NEXT_DATA__::text')
        loader.add_css('places', 'script#__NEXT_DATA__::text')
        loader.add_css('ratingHistogram', 'script#__NEXT_DATA__::text')
        loader.add_css("ratingsCount", 'script#__NEXT_DATA__::text')
        loader.add_css("reviewsCount", 'script#__NEXT_DATA__::text')
        loader.add_css('numPages', 'script#__NEXT_DATA__::text')
        loader.add_css("format", 'script#__NEXT_DATA__::text')

        loader.add_css('language', 'script#__NEXT_DATA__::text')
        loader.add_css("awards", 'script#__NEXT_DATA__::text')

        yield loader.load_item()

        if self.should_crawl_author:
            author_url = response.css('a.ContributorLink::attr(href)').extract_first()
            if author_url:
                yield response.follow(author_url, callback=self.author_spider.parse)
