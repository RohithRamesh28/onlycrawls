# onlycrawls
# Smart Async Web Crawler

This is a fast and intelligent asynchronous web crawler built with Python. It combines sitemap parsing (via robots.txt) and fallback crawling to collect internal URLs efficiently.

## Features

- Automatically detects and parses `robots.txt` and sitemaps.
- Recursively follows nested sitemaps if present.
- Falls back to async crawling if sitemap is not available.
- Performs depth-limited, breadth-first crawling.
- Skips irrelevant links (e.g., images, CSS, JS, videos, fonts).
- Outputs results to `smart_crawled_urls.csv`.

## Setup

1. Clone this repo:

```bash
git clone https://github.com/your-username/async-webcrawler.git
cd async-webcrawler
