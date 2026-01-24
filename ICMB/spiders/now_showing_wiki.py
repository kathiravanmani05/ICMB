import scrapy
import json
import urllib.parse


class PvrNowShowingWikiSpider(scrapy.Spider):
    name = "pvr_now_showing_wiki"
    allowed_domains = ["api3.pvrcinemas.com", "en.wikipedia.org"]

    # =========================
    # CONFIG
    # =========================
    city = "Bengaluru"

    PVR_URL = "https://api3.pvrcinemas.com/api/v1/booking/content/nowshowing"

    LANGUAGES = [
        "TAMIL", "TELUGU", "HINDI", "KANNADA","MALAYALAM",
    ]

    PVR_HEADERS  = {
  'accept': 'application/json, text/plain, */*',
  'accept-language': 'en-US,en;q=0.9',
  'appversion': '1.0',
  'cache-control': 'no-cache',
  'chain': 'PVR',
  'city': 'Bengaluru',
  'content-type': 'application/json',
  'country': 'INDIA',
  'origin': 'https://www.pvrcinemas.com',
  'platform': 'WEBSITE',
  'pragma': 'no-cache',
  'priority': 'u=1, i',
  'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-dest': 'empty',
  'sec-fetch-mode': 'cors',
  'sec-fetch-site': 'same-site',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
}

    WIKI_HEADERS = {
        "User-Agent": "ICMBMovieBot/1.0 (contact: admin@icmb.in)",
        "Accept": "application/json"
    }

    # =========================
    # START
    # =========================
    def start_requests(self):
        payload = json.dumps({"city": self.city})

        yield scrapy.Request(
            url=self.PVR_URL,
            method="POST",
            headers=self.PVR_HEADERS,
            body=payload,
            callback=self.parse_pvr
        )

    # =========================
    # PARSE PVR RESPONSE
    # =========================
    def parse_pvr(self, response):
        data = json.loads(response.text)

        seen = set()

        for block in data.get("output", {}).get("mv", []):
            for film in block.get("films", []):
                raw_name = film.get("filmName", "").upper().strip()
                if not raw_name:
                    continue

                # MOVIE NAME
                movie_name = raw_name.split("(")[0].strip()

                # LANGUAGE
                language = "UNKNOWN"
                for lang in self.LANGUAGES:
                    if f"({lang}" in raw_name or f" {lang} " in raw_name:
                        language = lang
                        break

                key = (movie_name, language)
                if key in seen:
                    continue
                seen.add(key)

                yield self.request_wiki(movie_name.title(), language.title())

    # =========================
    # WIKI SEARCH REQUEST
    # =========================
    def request_wiki(self, movie_name, language):
        queries = []

        if language.lower() != "unknown":
            queries.append(f"{movie_name} {language} film")
        queries.append(f"{movie_name} film")
        queries.append(movie_name)

        return scrapy.Request(
            url="https://en.wikipedia.org/w/api.php",
            headers=self.WIKI_HEADERS,
            callback=self.parse_wiki,
            dont_filter=True,
            meta={
                "movie_name": movie_name,
                "language": language,
                "queries": queries,
                "query_index": 0
            },
            cb_kwargs={"params": self.build_params(queries[0])}
        )

    def build_params(self, query):
        return {
            "action": "query",
            "list": "search",
            "srsearch": query,
            "format": "json",
            "utf8": 1,
            "srlimit": 1
        }

    # =========================
    # PARSE WIKI RESPONSE
    # =========================
    def parse_wiki(self, response, params):
        data = json.loads(response.text)

        movie_name = response.meta["movie_name"]
        language = response.meta["language"]
        queries = response.meta["queries"]
        index = response.meta["query_index"]

        results = data.get("query", {}).get("search", [])

        if results:
            title = results[0]["title"]
            wiki_url = (
                "https://en.wikipedia.org/wiki/"
                + urllib.parse.quote(title.replace(" ", "_"))
            )

            yield {
                "movie_name": movie_name,
                "language": language,
                "wikipedia": wiki_url
            }
            return

        # TRY NEXT QUERY
        index += 1
        if index < len(queries):
            yield scrapy.Request(
                url="https://en.wikipedia.org/w/api.php",
                headers=self.WIKI_HEADERS,
                callback=self.parse_wiki,
                dont_filter=True,
                meta={
                    "movie_name": movie_name,
                    "language": language,
                    "queries": queries,
                    "query_index": index
                },
                cb_kwargs={"params": self.build_params(queries[index])}
            )
        else:
            yield {
                "movie_name": movie_name,
                "language": language,
                "wikipedia": "Not Found"
            }
