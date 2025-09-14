import asyncio
import hashlib
import html
import json
import logging
import os
import random
import re
from collections import deque
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import aiohttp
import feedparser
import pytz
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telegram import Bot, error

# ===================== ENV & LOG =====================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ===================== CONST =====================

RSS_SOURCES = [
    "https://www.finam.ru/analytics/rsspoint/",
    "https://www.cbr.ru/rss/eventrss",
    "https://www.vedomosti.ru/rss/rubric/finance/banks.xml",
    "https://arb.ru/rss/news/",
    "https://www.bfm.ru/news.rss?rubric=28",
    "https://ria.ru/export/rss2/archive/index.xml",
    "https://www.vestifinance.ru/rss/news",
]

BACKUP_SOURCES = [
    "https://www.interfax.ru/rss.asp",
]

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не задан в .env файле!")
if not CHANNEL_ID:
    raise ValueError("❌ CHANNEL_ID не задан в .env файле!")

if CHANNEL_ID.lstrip('-').isdigit() and len(CHANNEL_ID.lstrip('-')) >= 10 and not CHANNEL_ID.startswith('-100'):
    CHANNEL_ID = '-100' + CHANNEL_ID.lstrip('-')

MAX_POSTS_PER_DAY = 3
MAX_CONTENT_LENGTH = 800
MIN_CONTENT_LENGTH = 50

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
]

HEADERS = {'User-Agent': random.choice(USER_AGENTS)}

FINANCE_KEYWORDS = [
    'банк', 'кредит', 'ипотека', 'вклад', 'депозит', 'акция', 'облигация',
    'рубль', 'доллар', 'евро', 'инфляция', 'ставка', 'цб', 'фрс', 'биржа',
    'криптовалюта', 'нефть', 'газ', 'экономика', 'рынок', 'инвест', 'финанс',
    'ликвидность', 'дивиденд', 'кризис', 'санкции', 'регулятор', 'центральный банк',
    'кредитная', 'заем', 'займ', 'рефинансирование', 'сбережения', 'фондовый',
    'валютный', 'курс', 'обмен', 'платеж', 'перевод', 'карта', 'брокер',
    'котировки', 'индекс', 'капитализация', 'актив', 'пассив', 'баланс', 'отчетность',
    'прибыль', 'убыток', 'выплаты', 'квартальный', 'годовой', 'надзор', 'лицензия',
    'санация', 'банкротство', 'форекс', 'инвестор', 'портфель', 'риск', 'доходность',
    'процент', 'ключевая ставка', 'монетарный', 'фискальный', 'бюджет', 'налог', 'тариф',
    'страхование', 'пенсионный', 'лизинг', 'факторинг'
]

KEYWORDS_TO_HASHTAGS = {
    "банк": ["#банки", "#финансы", "#банковскийСектор"],
    "кредит": ["#кредит", "#кредитование", "#займы"],
    "ипотека": ["#ипотека", "#недвижимость", "#жилье"],
    "вклад": ["#вклады", "#депозиты", "#сбережения"],
    "акция": ["#акции", "#инвестиции", "#фондовыйРынок"],
    "облигация": ["#облигации", "#бондс", "#долговыеИнструменты"],
    "рубль": ["#рубль", "#валюта", "#курсРубля"],
    "доллар": ["#доллар", "#USD", "#валюта"],
    "евро": ["#евро", "#EUR", "#валюта"],
    "инфляция": ["#инфляция", "#цены", "#экономика"],
    "ключевая ставка": ["#ключеваяСтавка", "#ЦБ", "#процентнаяСтавка"],
    "цб": ["#Центробанк", "#регулятор", "#банкРоссии"],
    "фрс": ["#ФРС", "#США"],
    "биржа": ["#биржа", "#трейдинг", "#фондовыйРынок"],
    "криптовалюта": ["#криптовалюта", "#биткоин", "#блокчейн"],
    "нефть": ["#нефть", "#энергетика"],
    "газ": ["#газ", "#энергетика"],
    "экономика": ["#экономика", "#макроэкономика"],
}

TOPIC_TO_EMOJI = {
    "банк": "🏦", "кредит": "💳", "ипотека": "🏠", "вклад": "💰",
    "акция": "📈", "облигация": "📊", "рубль": "₽", "доллар": "💵", "евро": "💶",
    "инфляция": "📉", "ключевая ставка": "📌", "цб": "🇷🇺", "фрс": "🇺🇸",
    "биржа": "📊", "криптовалюта": "₿", "нефть": "🛢️", "газ": "🔥",
    "экономика": "🌐", "рынок": "🤝", "инвест": "💼", "ликвидность": "💧",
    "дивиденд": "🎁", "кризис": "⚠️", "санкции": "🚫", "регулятор": "👮",
}

RUS_MONTHS = r'(январ[ья]|феврал[ья]|март[ае]?|апрел[ья]|ма[ея]|июн[ья]|июл[ья]|август[ае]?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])'

# ===================== UTILS =====================

def canon_url(url: str) -> str:
    """Удаляем UTM и прочие мусорные параметры, нормализуем ссылку."""
    try:
        u = urlparse(url)
        q = [(k, v) for k, v in parse_qsl(u.query, keep_blank_values=True)
             if not k.lower().startswith('utm')
             and k.lower() not in {'fbclid', 'gclid', 'yclid', 'utm_referrer'}]
        return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), ''))
    except Exception:
        return url

def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return url

def normalize_title(title: str) -> str:
    """Убираем даты/хвосты из заголовка."""
    if not title:
        return ""
    # 12 мая 2025, 12 мая 2025 г., 12.05.2025 и т.п.
    title = re.sub(rf'\b\d{{1,2}}\s+{RUS_MONTHS}\s+\d{{4}}\s*г?\.?,?\s*', ' ', title, flags=re.IGNORECASE)
    title = re.sub(r'\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b', ' ', title)
    # Хвосты вида "— Ведомости", "- РИА Новости"
    title = re.sub(r'\s*[-—–]\s*[^\n]+$', '', title).strip()
    return re.sub(r'\s+', ' ', title).strip()

def strip_byline_dates_everywhere(text: str) -> str:
    """Убираем авторов/даты/служебные вставки в любом месте текста."""
    if not text:
        return ""
    patterns = [
        rf'\b\d{{1,2}}\s+{RUS_MONTHS}\s+\d{{4}}\b',      # 12 мая 2025
        r'\b\d{1,2}[:.]\d{2}\b',                         # 12:34
        r'\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b',         # 12.05.2025
        r'(?:Автор|Корреспондент|Редакция|Источник|Фото|Иллюстрация)\s*:\s*[^\n]+',
        r'Читайте также[^\n]*',
        r'Подпис(ывайтесь|ка)[^\n]*',
        r'Материал.*партнеров[^\n]*',
        r'Реклама[^\n]*',
        r'Комментар(ий|ии)[^\n]*',
        r'Мы в соцсетях[^\n]*',
        r'Прислать новость[^\n]*',
        r'Обсудить в телеграме[^\n]*',
        r'https?://\S+',
    ]
    for p in patterns:
        text = re.sub(p, ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'[!?]{3,}', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()

# ===================== CLASS =====================

class NewsBot:
    def __init__(self, bot_token: str, channel_id: str):
        self.bot = Bot(token=bot_token)
        self.channel_id = channel_id
        self.session: Optional[aiohttp.ClientSession] = None
        self.posted_hashes: Set[str] = set()
        self.failed_sources: Set[str] = set()
        self.source_priority: Dict[str, int] = {}
        self.deleted_posts_tracker: Dict[str, datetime] = {}
        self.last_publication_time: Optional[datetime] = None

        # ротация по источникам между циклами
        self.recent_sources: deque[str] = deque(maxlen=12)

        self.load_hashes()
        self.load_source_stats()
        self.load_recent_sources()

    # ---------- persistence ----------

    def load_hashes(self):
        try:
            if os.path.exists('posted_hashes.txt'):
                with open('posted_hashes.txt', 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    recent_lines = lines[-1000:] if len(lines) > 1000 else lines
                    self.posted_hashes = set(line.strip() for line in recent_lines if line.strip())
                logger.info(f"Загружено {len(self.posted_hashes)} хешей.")
        except Exception as e:
            logger.error(f"Ошибка при загрузке хешей: {e}")

    def load_source_stats(self):
        try:
            if os.path.exists('source_stats.json'):
                with open('source_stats.json', 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.source_priority = data.get('priority', {})
                    deleted_data = data.get('deleted', {})
                    self.deleted_posts_tracker = {
                        source: datetime.fromisoformat(date_str)
                        for source, date_str in deleted_data.items()
                    }
        except Exception as e:
            logger.error(f"Ошибка при загрузке статистики источников: {e}")

    def save_source_stats(self):
        try:
            data = {
                'priority': self.source_priority,
                'deleted': {
                    source: date.isoformat()
                    for source, date in self.deleted_posts_tracker.items()
                }
            }
            with open('source_stats.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка при сохранении статистики источников: {e}")

    def load_recent_sources(self):
        try:
            if os.path.exists('recent_sources.json'):
                with open('recent_sources.json', 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    items = data.get('recent', [])
                    self.recent_sources = deque(items, maxlen=self.recent_sources.maxlen)
                logger.info(f"Загружено недавних источников: {len(self.recent_sources)}")
        except Exception as e:
            logger.error(f"Ошибка при загрузке recent_sources: {e}")

    def save_recent_sources(self):
        try:
            with open('recent_sources.json', 'w', encoding='utf-8') as f:
                json.dump({'recent': list(self.recent_sources)}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка при сохранении recent_sources: {e}")

    # ---------- duplicates ----------

    def _hash_pair(self, url: str, title: str) -> str:
        u = canon_url(url)
        t = normalize_title(title).lower()
        return hashlib.md5((u + '|' + t).encode('utf-8')).hexdigest()

    def save_hash(self, url: str, title: str):
        h = self._hash_pair(url, title)
        if h not in self.posted_hashes:
            self.posted_hashes.add(h)
            try:
                with open('posted_hashes.txt', 'a', encoding='utf-8') as f:
                    f.write(h + '\n')
            except Exception as e:
                logger.error(f"Не удалось сохранить хеш: {e}")

    def is_duplicate(self, url: str, title: str) -> bool:
        return self._hash_pair(url, title) in self.posted_hashes

    # ---------- quality ----------

    def is_finance_related(self, title: str, content: str) -> bool:
        text = f"{title} {content}".lower()
        return any(kw in text for kw in FINANCE_KEYWORDS)

    @staticmethod
    def clean_text(text: str) -> str:
        if not text:
            return ""
        soup = BeautifulSoup(text, "html.parser")
        for elem in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside', 'advertisement', 'iframe', 'form']):
            elem.decompose()
        for clip in soup.find_all(class_=lambda x: x and any(w in str(x).lower() for w in ['clip', 'ad', 'banner', 'promo', 'recommended', 'social', 'share'])):
            clip.decompose()
        for ul in soup.find_all('ul'):
            t = ul.get_text(" ")
            if any(k in t.lower() for k in ['банк', 'вклад', 'кредит', 'карта', 'ипотека', 'реклам']):
                ul.decompose()
        txt = soup.get_text(" ")
        txt = html.unescape(txt)
        txt = strip_byline_dates_everywhere(txt)
        return txt

    def extract_hashtags(self, title: str, content: str) -> List[str]:
        text = f"{title} {content}".lower()
        hashtags = set()
        for keyword, tags in KEYWORDS_TO_HASHTAGS.items():
            if keyword in text:
                hashtags.update(tags)
        if any(w in text for w in ["биржа", "трейдинг", "инвест"]):
            hashtags.add("#инвестиции")
        if any(w in text for w in ["крипто", "биткоин", "блокчейн", "криптовалюта"]):
            hashtags.add("#криптовалюты")
        if any(w in text for w in ["нефть", "газ", "энергетика"]):
            hashtags.add("#энергетика")
        if any(w in text for w in ["санкции", "эмбарго", "ограничения"]):
            hashtags.add("#международныеОтношения")
        return sorted(hashtags)[:5]

    def get_relevant_emoji(self, title: str, content: str) -> str:
        text = f"{title} {content}".lower()
        for keyword, emoji in sorted(TOPIC_TO_EMOJI.items(), key=lambda x: len(x[0]), reverse=True):
            if keyword in text:
                return emoji
        return "📰"

    async def fetch_full_article_text(self, url: str) -> str:
        max_retries = 3
        u = canon_url(url)
        for attempt in range(max_retries):
            try:
                headers = HEADERS.copy()
                headers['User-Agent'] = random.choice(USER_AGENTS)
                async with self.session.get(u, headers=headers, timeout=15) as resp:
                    if resp.status != 200:
                        logger.warning(f"Не удалось загрузить {u}: {resp.status}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt)
                        continue
                    html_text = await resp.text()
                    soup = BeautifulSoup(html_text, "html.parser")
                    for elem in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside', 'advertisement', 'iframe', 'form']):
                        elem.decompose()
                    for ad in soup.find_all(class_=lambda x: x and any(w in str(x).lower() for w in ['ad', 'banner', 'promo', 'recommended', 'social', 'share'])):
                        ad.decompose()

                    dom = domain_of(u)
                    selectors = []
                    if "finam.ru" in dom:
                        selectors = ['.article__body', '.content', 'article']
                    elif "cbr.ru" in dom:
                        selectors = ['.content', '.text', 'article']
                    elif "vedomosti.ru" in dom:
                        selectors = ['.article__body', '.article-body', 'article']
                    elif "arb.ru" in dom:
                        selectors = ['.news-detail', '.content', 'article']
                    elif "kommersant.ru" in dom or "rbc.ru" in dom or "banki.ru" in dom:
                        selectors = ['.article__text', '.article__content', '.news-text', '.article-content', 'article']
                    else:
                        selectors = ['.article-content', '.post-content', '.entry-content', '.article__body', '.article-body', 'article', '.content']

                    content = None
                    for sel in selectors:
                        try:
                            if sel.startswith('.'):
                                content = soup.find(class_=sel[1:])
                            elif sel.startswith('#'):
                                content = soup.find(id=sel[1:])
                            else:
                                content = soup.find(sel)
                            if content:
                                break
                        except Exception:
                            continue
                    if not content:
                        all_text = soup.get_text(" ")
                        return self.clean_text(all_text)
                    return self.clean_text(content.get_text(" "))

            except asyncio.TimeoutError:
                logger.warning(f"Таймаут при парсинге {u}, попытка {attempt + 1}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Ошибка при парсинге {u}: {e}, попытка {attempt + 1}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
        return ""

    def smart_truncate(self, text: str, max_length: int = MAX_CONTENT_LENGTH) -> str:
        if len(text) <= max_length:
            return text
        truncated = text[:max_length + 1]
        last_end = max(truncated.rfind('.'), truncated.rfind('!'), truncated.rfind('?'))
        if last_end > max_length - 100:
            return truncated[:last_end + 1]
        else:
            return truncated[:max_length].rstrip() + "…"

    def format_message(self, title: str, content: str, url: str) -> str:
        title = normalize_title(title)
        full_text = self.clean_text(content)
        if full_text.startswith(title):
            full_text = full_text[len(title):].lstrip(":.- ")
        truncated = self.smart_truncate(full_text, MAX_CONTENT_LENGTH)

        # Делим на 1–2 предложения в абзаце
        sentences = re.split(r'(?<=[.!?])\s+', truncated)
        paragraphs, buf = [], ""
        for s in sentences:
            if not buf:
                buf = s
            elif len(re.findall(r'[.!?]', buf)) < 2:
                buf += " " + s
            else:
                paragraphs.append(buf.strip())
                buf = s
        if buf:
            paragraphs.append(buf.strip())
        formatted = "\n\n".join(p for p in paragraphs if len(p) > 20)

        hashtags = self.extract_hashtags(title, content)
        hashtag_line = "\n\n" + " ".join(hashtags) if hashtags else ""
        emoji = self.get_relevant_emoji(title, content)

        message = (
            f"<b>{emoji} {html.escape(title)}</b>\n\n"
            f"{html.escape(formatted)}\n\n"
            f"👉 <a href='{html.escape(canon_url(url))}'>Читать далее</a>"
            f"{hashtag_line}"
        )
        if len(message) > 3900:
            message = message[:3897] + "..."
        return message

    # ---------- fetching ----------

    async def fetch_feed(self, url: str) -> List[Dict]:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                headers = HEADERS.copy()
                headers['User-Agent'] = random.choice(USER_AGENTS)
                async with self.session.get(url, headers=headers, timeout=15) as response:
                    if response.status != 200:
                        logger.warning(f"HTTP {response.status} на {url}")
                        self.failed_sources.add(url)
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt)
                        continue
                    content = await response.text()
                    feed = await asyncio.to_thread(feedparser.parse, content)
                    entries = []
                    for entry in feed.entries:
                        title = (entry.get("title") or "").strip()
                        if not title:
                            continue
                        if "видео" in title.lower() or "video" in title.lower():
                            continue
                        title = normalize_title(title)
                        link = canon_url((entry.get("link") or "").strip())
                        description = entry.get("description", "") or entry.get("summary", "") or ""
                        if "<![CDATA[" in description:
                            description = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', description, flags=re.DOTALL)

                        soup_desc = BeautifulSoup(description, "html.parser")
                        description = strip_byline_dates_everywhere(soup_desc.get_text(" "))

                        # Авторов и метки — не используем в выводе, чтобы не «засорять»
                        creator = ""

                        if title and link and self.is_finance_related(title, description):
                            entries.append({
                                "title": title,
                                "url": link,
                                "content": description,
                                "creator": creator,
                                "source": url,
                                "domain": domain_of(link),
                            })
                    logger.info(f"{urlparse(url).netloc}: {len(entries)} новостей")
                    return entries
            except asyncio.TimeoutError:
                logger.warning(f"Таймаут RSS {url}, попытка {attempt + 1}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Ошибка RSS {url}: {e}, попытка {attempt + 1}")
                self.failed_sources.add(url)
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
        return []

    # ---------- selection & rotation ----------

    def select_news_fair(self, news_items: List[Dict], k: int) -> List[Dict]:
        """
        Справедливый отбор по источникам/доменам:
        1) Сначала берём новости из доменов, которых нет в recent_sources.
        2) Затем — заполняем остаток, избегая подряд одинаковых доменов.
        """
        if not news_items:
            return []

        # Уберём явные дубликаты по (url,title)
        uniq = {}
        for n in news_items:
            key = (canon_url(n["url"]), normalize_title(n["title"]).lower())
            if key not in uniq:
                uniq[key] = n
        items = list(uniq.values())

        # Сортируем по времени появления в сборке (как пришли) — можно перемешать
        random.shuffle(items)

        recent = set(self.recent_sources)
        first_pass = [n for n in items if n["domain"] not in recent]
        second_pass = [n for n in items if n["domain"] in recent]

        result: List[Dict] = []
        used_domains: Set[str] = set()

        def take_from(bucket: List[Dict]):
            nonlocal result, used_domains
            for n in bucket:
                if len(result) >= k:
                    break
                d = n["domain"]
                if result and result[-1]["domain"] == d:
                    continue  # не ставим подряд
                result.append(n)
                used_domains.add(d)

        take_from(first_pass)
        if len(result) < k:
            take_from(second_pass)

        # если всё ещё не хватает — добираем чем есть, аккуратно чередуя
        if len(result) < k:
            leftovers = [n for n in items if n not in result]
            for n in leftovers:
                if len(result) >= k:
                    break
                if not result or result[-1]["domain"] != n["domain"]:
                    result.append(n)

        # обновим недавние источники
        for n in result:
            self.recent_sources.append(n["domain"])
        self.save_recent_sources()

        return result[:k]

    # ---------- publish ----------

    async def publish_post(self, title: str, content: str, url: str, source: str = "") -> bool:
        if self.is_duplicate(url, title):
            logger.info(f"Пропущено (дубликат): {title[:60]}...")
            return False
        if not self.is_finance_related(title, content):
            logger.info(f"Пропущено (не финтематика): {title[:60]}...")
            return False

        full_text = await self.fetch_full_article_text(url)
        use_text = full_text if full_text.strip() else content
        cleaned = self.clean_text(use_text)
        if len(cleaned) < MIN_CONTENT_LENGTH:
            logger.info(f"Пропущено (мало текста): {title[:60]}...")
            return False

        max_retries = 3
        for attempt in range(max_retries):
            try:
                message = self.format_message(title, use_text, url)
                await self.bot.send_message(
                    chat_id=self.channel_id,
                    text=message,
                    parse_mode='HTML',
                    disable_web_page_preview=True  # важный флаг против «мусора» из превью
                )
                logger.info(f"✅ Опубликовано: {title[:60]}...")
                self.save_hash(url, title)
                if source:
                    self.source_priority[source] = self.source_priority.get(source, 0) + 1
                    self.save_source_stats()
                return True
            except error.RetryAfter as e:
                logger.warning(f"Rate limit, ждём {e.retry_after} сек...")
                await asyncio.sleep(e.retry_after)
            except error.TimedOut:
                logger.warning(f"Таймаут при публикации, попытка {attempt + 1}/{max_retries}")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"❌ Ошибка публикации '{title}': {e}")
                if attempt == max_retries - 1:
                    if source:
                        self.deleted_posts_tracker[source] = datetime.now()
                        self.source_priority[source] = self.source_priority.get(source, 0) - 2
                        self.save_source_stats()
                    return False
                await asyncio.sleep(2 ** attempt)
        return False

    # ---------- scheduling ----------

    def generate_post_schedule(self) -> List[datetime]:
        try:
            msk = pytz.timezone('Europe/Moscow')
            now = datetime.now(msk)
            start_hour, end_hour = 9, 21
            if now.hour < start_hour:
                base = now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
            elif now.hour >= end_hour:
                base = (now + timedelta(days=1)).replace(hour=start_hour, minute=0, second=0, microsecond=0)
            else:
                base = now

            times = []
            available = (end_hour - start_hour) * 60
            slots = MAX_POSTS_PER_DAY
            for i in range(slots):
                minute_position = available * (i + 1) / (slots + 1)
                random_variation = random.uniform(-25, 25)
                total_minutes = minute_position + random_variation
                h = int(total_minutes // 60)
                m = int(total_minutes % 60)
                t = base.replace(hour=start_hour + h, minute=m, second=0, microsecond=0)
                if t.hour < start_hour:
                    t = t.replace(hour=start_hour, minute=random.randint(0, 30))
                elif t.hour >= end_hour:
                    t = t.replace(hour=end_hour - 1, minute=random.randint(30, 59))
                times.append(t)
            return sorted(times)
        except Exception as e:
            logger.error(f"Ошибка расписания: {e}")
            return [datetime.now() + timedelta(minutes=30 * i) for i in range(MAX_POSTS_PER_DAY)]

    # ---------- main ----------

    async def run(self):
        connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=20)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            self.session = session

            # Параллельно собираем новости
            async def _fetch(url):
                if url in self.failed_sources:
                    return []
                try:
                    return await self.fetch_feed(url)
                except Exception as e:
                    logger.error(f"Ошибка источника {url}: {e}")
                    self.failed_sources.add(url)
                    return []

            tasks = [_fetch(src) for src in RSS_SOURCES]
            results = await asyncio.gather(*tasks, return_exceptions=False)
            all_news = [item for sub in results for item in sub]

            if len(all_news) < MAX_POSTS_PER_DAY:
                logger.info("Пробуем резервные источники…")
                tasks_b = [_fetch(src) for src in BACKUP_SOURCES]
                results_b = await asyncio.gather(*tasks_b, return_exceptions=False)
                all_news.extend(item for sub in results_b for item in sub)

            # Фильтрация по тематике и длине
            filtered = []
            seen_urls = set()
            for it in all_news:
                url_c = canon_url(it["url"])
                if url_c in seen_urls:
                    continue
                if (not self.is_duplicate(it["url"], it["title"])
                    and self.is_finance_related(it["title"], it["content"])
                    and len(self.clean_text(it["content"])) >= MIN_CONTENT_LENGTH):
                    filtered.append(it)
                    seen_urls.add(url_c)

            if not filtered:
                logger.info("Нет подходящих новостей.")
                return

            # Справедливая ротация по доменам/источникам
            final_news = self.select_news_fair(filtered, MAX_POSTS_PER_DAY)

            if not final_news:
                logger.info("Нечего публиковать после ротации.")
                return

            schedule = self.generate_post_schedule()
            logger.info(f"Сгенерировано расписание на {len(schedule)} публикаций.")
            for i, t in enumerate(schedule, 1):
                logger.info(f"  {i}. {t.strftime('%Y-%m-%d %H:%M:%S')} МСК")

            for i, (news_item, pub_time) in enumerate(zip(final_news, schedule)):
                now = datetime.now(pytz.timezone('Europe/Moscow'))
                if pub_time > now:
                    wait_seconds = (pub_time - now).total_seconds()
                    logger.info(f"Ожидание до публикации {i+1}: {int(wait_seconds)} сек")
                    await asyncio.sleep(wait_seconds)

                ok = await self.publish_post(
                    title=news_item["title"],
                    content=news_item["content"],
                    url=news_item["url"],
                    source=news_item["source"]
                )
                # записываем домен в историю ротации даже при неудаче — чтобы попытаться другой
                self.recent_sources.append(news_item["domain"])
                self.save_recent_sources()

                if not ok and i < len(final_news) - 1:
                    logger.warning("Публикация не удалась, переходим к следующей.")

                if i < len(final_news) - 1:
                    await asyncio.sleep(random.uniform(5, 12))

            logger.info("✅ Цикл публикаций завершён.")

            if len(self.failed_sources) > 0 and datetime.now().hour == 0:
                logger.info("Очистка списка неудачных источников")
                self.failed_sources.clear()


async def main():
    try:
        bot = NewsBot(BOT_TOKEN, CHANNEL_ID)
        await bot.run()
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
