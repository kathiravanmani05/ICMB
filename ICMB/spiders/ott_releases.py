import scrapy,json
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
    # CUSTOM HEADERS
    # ======================
    custom_headers = {
        "accept": "application/json",
        "apiversion": "1",
        "platform": "web",
        "source": "web",
        "user-agent": "Mozilla/5.0"
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                headers=self.custom_headers,
                callback=self.parse
            )

    def parse(self, response):
        data = json.loads(response.text)

        seen = set()

        for movie in data.get("result", []):
            title = movie.get("display_name") or movie.get("name")
            ottplay_id = movie.get("ottplay_id")
            poster = movie.get("posters", [None])[0]

            language = movie.get("primary_language", {}).get("logo_text")

            # ❌ Skip English
            if language in ("English", "E"):
                continue

            for w in movie.get("where_to_watch", []):
                available_from = w.get("available_from")
                if not available_from:
                    continue

                release_date = date.fromisoformat(available_from[:10])

                # ✅ STRICT DATE WINDOW
                if not (self.FROM_DATE <= release_date <= self.TO_DATE):
                    continue

                provider = w.get("provider", {}).get("name")

                unique_key = f"{ottplay_id}_{language}_{provider}"
                if unique_key in seen:
                    continue

                seen.add(unique_key)

                yield {
                    "title": title,
                    "language": language,
                    "ott_platform": provider,
                    "ott_release_date": release_date.isoformat(),
                    
                }
