import scrapy
import re
import html
import json
import time
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import quote_plus
from pathlib import Path


class WikiMovieFullSpider(scrapy.Spider):
    name = "wiki_movie_full"
    allowed_domains = ["wikipedia.org", "google.com", "youtube.com"]

    # -------------------- CONFIG --------------------
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0"
    }

    REQUEST_TIMEOUT = 12
    SEARCH_SLEEP = 1.0
    VIDEO_SLEEP = 0.6

    URLS = [
        "https://en.wikipedia.org/wiki/Sirai_%282025_film%29",
        "https://en.wikipedia.org/wiki/The_RajaSaab",
        "https://en.wikipedia.org/wiki/Mana_Shankara_Vara_Prasad_Garu",
        "https://en.wikipedia.org/wiki/Anaganaga_Oka_Raju",
        "https://en.wikipedia.org/wiki/Nari_Nari_Naduma_Murari_(2026_film)",
        "https://te.wikipedia.org/wiki/%E0%B0%AD%E0%B0%B0%E0%B1%8D%E0%B0%A4_%E0%B0%AE%E0%B0%B9%E0%B0%BE%E0%B0%B6%E0%B0%AF%E0%B1%81%E0%B0%B2%E0%B0%95%E0%B1%81_%E0%B0%B5%E0%B0%BF%E0%B0%9C%E0%B1%8D%E0%B0%9E%E0%B0%AA%E0%B1%8D%E0%B0%A4%E0%B0%BF",
        "https://en.wikipedia.org/wiki/Mark_(2025_film)",
        "https://en.wikipedia.org/wiki/45_(2025_film)",
        "https://en.wikipedia.org/wiki/The_Devil_(2025_film)",
        "https://en.wikipedia.org/wiki/Theertharoopa_Thandeyavarige",
        "https://en.wikipedia.org/wiki/Sarvam_Maya",
        "https://en.wikipedia.org/wiki/Maareesan",
        "https://en.wikipedia.org/wiki/Dhurandhar",
        "https://en.wikipedia.org/wiki/Rahu_Ketu_(2026_film)",
        "https://en.wikipedia.org/wiki/Ikkis",
        "https://en.wikipedia.org/wiki/Border_2",
        "https://en.wikipedia.org/wiki/One_Two_Cha_Cha_Chaa",
        "https://en.wikipedia.org/wiki/Laalo_%E2%80%93_Krishna_Sada_Sahaayate",
        "https://en.wikipedia.org/wiki/Sky_Force_(film)",
    ]

    FINAL_COLUMNS = [
        "Genres","Director","Writer","Producer","Screenplay","Starring",
        "Cinematography","Edited by","Music by","Production Company",
        "Release Date","Runtime","Budget","Box Office","OTT Platform",
        "Censorship Rating","Trailer YouTube Link","Plot","Soundtrack",
        "OTT Platfrom Link","Movie_name","Other Languages","Original Language",
        "Actors"," Directors"," Languages"," Genres"," Years"," OTT Platforms",
        " Musics"," Running in Cinemas"," Upcoming Movies"
    ]

    # -------------------- START --------------------
    def start_requests(self):
        for url in self.URLS:
            yield scrapy.Request(
                url=url,
                headers=self.HEADERS,
                callback=self.parse_movie,
                dont_filter=True
            )

    # -------------------- PARSE MOVIE --------------------
    def parse_movie(self, response):
        soup = BeautifulSoup(response.text, "lxml")
        out = {c: "" for c in self.FINAL_COLUMNS}

        # Movie name
        title_tag = soup.select_one("#firstHeading")
        raw_title = title_tag.get_text(strip=True) if title_tag else response.url.split("/")[-1]
        out["Movie_name"] = self.clean_movie_title(raw_title)

        # Poster
        img = soup.select_one('table.infobox a.image img')
        if img and img.get("src"):
            src = img["src"]
            out["poster"] = ("https:" + src) if src.startswith("//") else src

        # Genres
        genres = self.extract_anchor_texts_from_td(self.first_infobox_td(soup, "Genre"))
        out[" Genres"] = ", ".join(genres)

        # Director
        dirs = self.extract_anchor_texts_from_td(self.first_infobox_td(soup, "Director"))
        out[" Directors"] = ", ".join(dirs)

        # Writer / Producer
        out["Writer"] = ", ".join(self.extract_anchor_texts_from_td(self.first_infobox_td(soup, "Writer")))
        out["Producer"] = ", ".join(self.extract_anchor_texts_from_td(self.first_infobox_td(soup, "Producer")))

        # Actors
        starring = self.extract_anchor_texts_from_td(self.first_infobox_td(soup, "Starring"))
        out["Actors"] = ", ".join(starring)

        # Release date
        raw_release = self.extract_text_from_td(self.first_infobox_td(soup, "Release"))
        out["Release Date"] = self.format_release_date(raw_release)
        m_year = re.search(r"\b(19|20)\d{2}\b", raw_release or "")
        out["Years"] = m_year.group(0) if m_year else ""

        # Runtime
        out["Runtime"] = self.normalize_runtime(
            self.extract_text_from_td(self.first_infobox_td(soup, "Running time"))
        )

        # Budget / Box office
        out["Budget"] = self.extract_text_from_td(self.first_infobox_td(soup, "Budget"))
        out["Box Office"] = self.extract_text_from_td(self.first_infobox_td(soup, "Box office"))

        # Plot
        paras = soup.select("div.mw-parser-output > p")
        for p in paras:
            text = p.get_text(" ", strip=True)
            if len(text) > 80:
                out["Plot"] = re.sub(r'\[\d+\]', '', text)
                break

        yield out

    # -------------------- HELPERS (UNCHANGED LOGIC) --------------------
    def first_infobox_td(self, soup, label):
        for tr in soup.select("table.infobox tr"):
            th = tr.find("th")
            if th and label.lower() in th.get_text(strip=True).lower():
                return tr.find("td")
        return None

    def extract_anchor_texts_from_td(self, td):
        if td is None:
            return []
        anchors = [a.get_text(strip=True) for a in td.find_all("a")]
        return [a for a in anchors if a]

    def extract_text_from_td(self, td):
        if td is None:
            return ""
        return re.sub(r'\s+', ' ', td.get_text(" ", strip=True)).strip()

    def clean_movie_title(self, title):
        title = title.replace("_", " ")
        year_match = re.search(r"\b(19|20)\d{2}\b", title)
        year = year_match.group(0) if year_match else ""
        name = title.split("(")[0].strip()
        return f"{name} ({year})" if year else name

    def format_release_date(self, raw):
        if not raw:
            return ""
        cleaned = re.sub(r"\(.*?\)", "", raw).strip()
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                return datetime.strptime(cleaned, fmt).strftime("%d-%b-%Y")
            except Exception:
                pass
        return cleaned

    def normalize_runtime(self, raw):
        if not raw:
            return ""
        m = re.search(r"(\d+)\s*min", raw.lower())
        if m:
            return f"{m.group(1)} min"
        m = re.search(r"(\d+)\s*h", raw.lower())
        if m:
            return f"{int(m.group(1))*60} min"
        return raw
