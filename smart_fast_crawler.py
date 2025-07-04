import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import xml.etree.ElementTree as ET
import pandas as pd
import logging
import sys
import time

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(level=logging.INFO)

class SmartBFSAsyncCrawler:
    def __init__(self, base_url, max_tasks=500, max_depth=3):
        self.base_url = base_url if base_url.endswith("/") else base_url + "/"
        self.parsed_base = urlparse(self.base_url)
        self.base_domain = self.parsed_base.netloc
        self.max_tasks = max_tasks
        self.max_depth = max_depth
        self.session = None
        self.urls_to_visit = asyncio.Queue()
        self.visited_urls = set()
        self.result_urls = []
        self.sem = asyncio.Semaphore(max_tasks)
        self.robot_parser = RobotFileParser()
        self.robots_loaded = False
        self.sitemap_loaded = False

    def is_internal(self, url):
        parsed = urlparse(url)
        return parsed.netloc == "" or parsed.netloc == self.base_domain

    def is_valid_url(self, url):
        irrelevant_exts = (".jpg", ".jpeg", ".png", ".gif", ".css", ".js", ".ico",
                           ".svg", ".woff", ".ttf", ".otf", ".eot", ".pdf", ".zip", ".txt",
                           ".xml", ".mp4", ".webm", ".avi", ".mp3", ".json")
        return not url.lower().endswith(irrelevant_exts)

    def can_fetch(self, url):
        if not self.robots_loaded:
            return True
        try:
            return self.robot_parser.can_fetch("*", url)
        except:
            return True

    async def fetch(self, url):
        try:
            async with self.sem:
                async with self.session.get(url, timeout=10, allow_redirects=True) as response:
                    if response.status == 200:
                        html = await response.text()
                        if html and len(html.strip()) > 100:
                            logging.info(f"Fetched: {url}")
                            return html
        except Exception as e:
            logging.warning(f"Fetch failed: {url} - {e}")
        return None

    async def parse_links(self, html, base_url):
        soup = BeautifulSoup(html, "html.parser")
        links = set()
        for tag in soup.find_all("a", href=True):
            href = tag.get("href").strip()
            full_url = urljoin(base_url, href).split("#")[0].rstrip("/")
            if self.is_internal(full_url) and self.is_valid_url(full_url) and self.can_fetch(full_url):
                links.add(full_url)
        return links

    async def parse_sitemap(self, sitemap_url):
        urls = set()
        try:
            async with self.session.get(sitemap_url, timeout=10) as response:
                if response.status == 200:
                    text = await response.text()
                    root = ET.fromstring(text)
                    for loc in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                        url = loc.text.strip()
                        if self.is_internal(url) and self.is_valid_url(url):
                            urls.add(url)
                    self.sitemap_loaded = True
                    logging.info(f"sitemap.xml found ({len(urls)} URLs)")
        except Exception as e:
            logging.warning(f"Failed to parse sitemap.xml: {e}")
        return urls

    async def crawl(self):
        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            self.session = session

            try:
                robots_url = urljoin(self.base_url, "/robots.txt")
                self.robot_parser.set_url(robots_url)
                self.robot_parser.read()
                self.robots_loaded = True
                logging.info("robots.txt found and loaded")
            except Exception as e:
                logging.warning(f"robots.txt not found or failed: {e}")

            sitemap_url = urljoin(self.base_url, "/sitemap.xml")
            sitemap_urls = await self.parse_sitemap(sitemap_url)

            if self.sitemap_loaded and sitemap_urls:
                for url in sitemap_urls:
                    await self.urls_to_visit.put((url, 0))
            else:
                logging.info("No sitemap found. Falling back to homepage crawl.")
                await self.urls_to_visit.put((self.base_url, 0))

            while not self.urls_to_visit.empty():
                tasks = []
                for _ in range(min(self.max_tasks, self.urls_to_visit.qsize())):
                    url, depth = await self.urls_to_visit.get()
                    if url not in self.visited_urls and depth <= self.max_depth:
                        tasks.append(asyncio.create_task(self.handle_url(url, depth)))
                if tasks:
                    await asyncio.gather(*tasks)

    async def handle_url(self, url, depth):
        self.visited_urls.add(url)
        html = await self.fetch(url)
        if html:
            self.result_urls.append(url)
            if depth < self.max_depth:
                links = await self.parse_links(html, url)
                for link in links:
                    if link not in self.visited_urls:
                        await self.urls_to_visit.put((link, depth + 1))

    def run(self):
        start = time.time()
        asyncio.run(self.crawl())
        end = time.time()
        print(f"\nTotal crawl time: {round(end - start, 2)} seconds")
        return self.result_urls

    def export_to_csv(self, filename="depth_crawled_urls.csv"):
        df = pd.DataFrame(self.result_urls, columns=["URL"])
        df.to_csv(filename, index=False)
        print(f"\nExported {len(df)} URLs to {filename}")

if __name__ == "__main__":
    print("Enter a site URL (e.g., https://books.toscrape.com)")
    target = input("Target: ").strip()
    crawler = SmartBFSAsyncCrawler(base_url=target, max_tasks=500, max_depth=3)
    print(f"\n Starting depth-limited crawl for: {target}")
    urls = crawler.run()
    crawler.export_to_csv()
