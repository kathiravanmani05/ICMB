import scrapy
import json
import requests
import urllib.parse
from lxml import html
from datetime import date, timedelta


class OttplayLatestSpider(scrapy.Spider):
    name = "ottplay_latest"
    allowed_domains = ["api2.ottplay.com"]

    # ======================
    # AUTO DATE RANGE
    # ======================
    today = date.today()
    FROM_DATE = today
    TO_DATE = today + timedelta(days=7)

    # ======================
    # START URL
    # ======================
    start_urls = [
        (
            "https://api2.ottplay.com/api/v4.7/web/new-release"
            f"?limit=20"
            f"&from_date={FROM_DATE.isoformat()}"
            f"&to_date={TO_DATE.isoformat()}"
            f"&content_type=movie"
            f"&language="
            f"&provider="
        )
    ]

    # ======================
    # HEADERS
    # ======================
    api_headers = {
        "accept": "application/json",
        "apiversion": "1",
        "platform": "web",
        "source": "web",
        "user-agent": "Mozilla/5.0"
    }

    search_headers = {
        "User-Agent": "Mozilla/5.0"
    }

    # ======================
    # OTT PRIORITY & LOGOS
    # ======================
    OTT_PRIORITY = [
        "hotstar.com",
        "zee5.com",
        "sonyliv.com",
        "primevideo.com",
        "lionsgateplay.com",
        "aha.video",
        "tataplaybinge.com",
        "airtelxstream.in",
        "sunnxt.com",
        "netflix.com",
    ]

    OTT_LOGO_MAP = {
        "hotstar.com": "https://icmb.in/wp-content/uploads/2026/01/hotstar.webp",
        "zee5.com": "https://icmb.in/wp-content/uploads/2026/01/zee5.jpeg",
        "sonyliv.com": "https://icmb.in/wp-content/uploads/2026/01/SonyLIV.png",
        "primevideo.com": "https://icmb.in/wp-content/uploads/2026/01/prime_video.jpg",
        "aha.video": "https://icmb.in/wp-content/uploads/2026/01/aha.png",
        "netflix.com": "https://icmb.in/wp-content/uploads/2026/01/netflix.png",
        "airtelxstream.in": "https://icmb.in/wp-content/uploads/2026/01/airtel-xstream.webp",
        "sunnxt.com": "https://icmb.in/wp-content/uploads/2026/01/sunnxt.jpg"
    }

    # ======================
    # REQUEST START
    # ======================
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                headers=self.api_headers,
                callback=self.parse
            )

    # ======================
    # DUCKDUCKGO HELPERS
    # ======================
    def extract_uddg_url(self, link):
        if "uddg=" not in link:
            return None

        parsed = urllib.parse.urlparse(link)
        qs = urllib.parse.parse_qs(parsed.query)
        uddg = qs.get("uddg", [None])[0]

        return urllib.parse.unquote(uddg) if uddg else None

    def get_best_ott_link(self, item):
        release_date = date.fromisoformat(item["ott_release_date"])

        # Only today or past releases
        if release_date > date.today():
            return None

        query = f'{item["title"]} {item["language"]} {item["ott_platform"]} OTT movie'

        r = requests.get(
            "https://duckduckgo.com/html/",
            params={"q": query},
            headers=self.search_headers,
            timeout=15
        )

        tree = html.fromstring(r.text)
        raw_links = tree.xpath("//a[contains(@class,'result__a')]/@href")

        decoded_links = []
        for link in raw_links:
            decoded = self.extract_uddg_url(link)
            if decoded:
                decoded_links.append(decoded)

        # Priority match
        for domain in self.OTT_PRIORITY:
            for url in decoded_links:
                if domain in url:
                    return url

        return decoded_links[0] if decoded_links else None

    def build_ott_html(self, ott_url):
        if not ott_url:
            return None

        for domain, logo in self.OTT_LOGO_MAP.items():
            if domain in ott_url:
                return (
                    f'<a href="{ott_url}" target="_blank" rel="noopener noreferrer">'
                    f'<img src="{logo}" alt="Watch on OTT" '
                    f'style="max-width:150px;display:block;margin:0 auto 10px;" />'
                    f'</a>'
                )

        return None

    # ======================
    # PARSER
    # ======================
    def parse(self, response):
        data = json.loads(response.text)
        seen = set()

        for movie in data.get("result", []):
            title = movie.get("display_name") or movie.get("name")
            ottplay_id = movie.get("ottplay_id")
            language = movie.get("primary_language", {}).get("logo_text")

            if language in ("English", "E"):
                continue

            for w in movie.get("where_to_watch", []):
                available_from = w.get("available_from")
                if not available_from:
                    continue

                release_date = date.fromisoformat(available_from[:10])

                if not (self.FROM_DATE <= release_date <= self.TO_DATE):
                    continue

                provider = w.get("provider", {}).get("name")
                unique_key = f"{ottplay_id}_{language}_{provider}"

                if unique_key in seen:
                    continue
                seen.add(unique_key)

                item = {
                    "title": title,
                    "language": language,
                    "ott_platform": provider,
                    "ott_release_date": release_date.isoformat(),
                }

                ott_url = self.get_best_ott_link(item)

                item["ott_link"] = ott_url
                item["ott_html"] = self.build_ott_html(ott_url)

                yield item
