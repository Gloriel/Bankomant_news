import asyncio
import hashlib
import html
import logging
import os
import random
import re
from datetime import datetime, time, timedelta
from typing import List, Dict, Set, Optional
from urllib.parse import urlparse

import aiohttp
import feedparser
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telegram import Bot, error

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Константы
RSS_SOURCES = [
    "https://www.finam.ru/analytics/rsspoint/",
    "https://www.cbr.ru/rss/eventrss",
    "https://www.vedomosti.ru/rss/rubric/finance/banks.xml",
    "https://arb.ru/rss/news/",
    "https://www.kommersant.ru/RSS/news.xml",
    "https://www.rbc.ru/rss/finances.xml",
    "https://www.banki.ru/xml/news.rss"
]

# Альтернативные источники на случай блокировки основных
BACKUP_SOURCES = [
    "https://www.kommersant.ru/RSS/bank.xml",
    "https://www.rbc.ru/rss/economics.xml",
    "https://www.interfax.ru/rss.asp",
    "https://www.vestifinance.ru/rss/news"
]

# Удаляем лишние пробелы в URL
RSS_SOURCES = [url.strip() for url in RSS_SOURCES]
BACKUP_SOURCES = [url.strip() for url in BACKUP_SOURCES]

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
MAX_POSTS_PER_DAY = 4
MAX_CONTENT_LENGTH = 800
MIN_CONTENT_LENGTH = 50

# User-Agent для обхода блокировок
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Обновленные ключевые слова → хештеги для финансовой тематики
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
    "ЦБ": ["#Центробанк", "#регулятор", "#банкРоссии"],
    "ФРС": ["#ФРС", "#ФедеральнаяРезервнаяСистема", "#США"],
    "биржа": ["#биржа", "#трейдинг", "#фондовыйРынок"],
    "криптовалюта": ["#криптовалюта", "#биткоин", "#блокчейн"],
    "нефть": ["#нефть", "#нефтяныеКотировки", "#энергетика"],
    "газ": ["#газ", "#энергетика", "#Газпром"],
    "экономика": ["#экономика", "#макроэкономика", "#ВВП"],
    "рынок": ["#финансовыйРынок", "#рынок", "#трейдеры"],
    "инвест": ["#инвестиции", "#инвесторы", "#капиталовложения"],
    "ликвидность": ["#ликвидность", "#денежныйРынок", "#финансы"],
    "дивиденд": ["#дивиденды", "#доходность", "#акционеры"],
    "кризис": ["#кризис", "#экономическийКризис", "#рецессия"],
    "санкции": ["#санкции", "#экономическиеСанкции", "#международныеОтношения"],
    "регулятор": ["#регулятор", "#надзор", "#финансовыйНадзор"],
}

# Обновленные эмодзи для финансовой тематики
TOPIC_TO_EMOJI = {
    "банк": "🏦",
    "кредит": "💳",
    "ипотека": "🏠",
    "вклад": "💰",
    "акция": "📈",
    "облигация": "📊",
    "рубль": "₽",
    "доллар": "💵",
    "евро": "💶",
    "инфляция": "📉",
    "ключевая ставка": "📌",
    "ЦБ": "🇷🇺",
    "ФРС": "🇺🇸",
    "биржа": "📊",
    "криптовалюта": "₿",
    "нефть": "🛢️",
    "газ": "🔥",
    "экономика": "🌐",
    "рынок": "🤝",
    "инвест": "💼",
    "ликвидность": "💧",
    "дивиденд": "🎁",
    "кризис": "⚠️",
    "санкции": "🚫",
    "регулятор": "👮",
}


class NewsBot:
    def __init__(self, bot_token: str, channel_id: str):
        self.bot = Bot(token=bot_token)
        self.channel_id = channel_id
        self.session = None
        self.posted_hashes: Set[str] = set()
        self.failed_sources: Set[str] = set()
        self.load_hashes()

    def load_hashes(self):
        """Загружает хеши опубликованных новостей"""
        try:
            if os.path.exists('posted_hashes.txt'):
                with open('posted_hashes.txt', 'r', encoding='utf-8') as f:
                    self.posted_hashes = set(line.strip() for line in f)
                logger.info(f"Загружено {len(self.posted_hashes)} хешей из истории.")
        except Exception as e:
            logger.error(f"Ошибка при загрузке хешей: {e}")

    def save_hash(self, url: str, title: str):
        """Сохраняет хеш новости"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        title_hash = hashlib.md5(title.encode()).hexdigest()
        combined = f"{url_hash}_{title_hash}"
        self.posted_hashes.add(combined)
        try:
            with open('posted_hashes.txt', 'a', encoding='utf-8') as f:
                f.write(combined + '\n')
        except Exception as e:
            logger.error(f"Не удалось сохранить хеш: {e}")

    def cleanup_old_hashes(self, days_to_keep: int = 30):
        """Очищает старые хеши для предотвращения разрастания файла"""
        try:
            if os.path.exists('posted_hashes.txt'):
                if len(self.posted_hashes) > 1000:
                    self.posted_hashes = set(list(self.posted_hashes)[-500:])
                    with open('posted_hashes.txt', 'w', encoding='utf-8') as f:
                        for h in self.posted_hashes:
                            f.write(h + '\n')
                    logger.info("Очищены старые хеши")
        except Exception as e:
            logger.error(f"Ошибка при очистке хешей: {e}")

    def is_duplicate(self, url: str, title: str) -> bool:
        """Проверяет дубликаты по хешу URL и заголовка"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        title_hash = hashlib.md5(title.encode()).hexdigest()
        combined = f"{url_hash}_{title_hash}"
        return combined in self.posted_hashes

    @staticmethod
    def clean_text(text: str) -> str:
        """Очищает текст от HTML, авторов, рекламы и лишних пробелов"""
        if not text:
            return ""

        text = BeautifulSoup(text, "html.parser").get_text()
        text = html.unescape(text)

        # Удаляем информацию об авторе, источнике и дате
        patterns = [
            r'^\s*[А-Я][а-я]+\s+[А-Я]\.[А-Я]\.?',
            r'^\s*[А-Я][а-я]+(?:\s+[А-Я][а-я]+)?\s*/\s*[А-Я][а-я]+',
            r'^\s*—\s*[^\n]+',
            r'(Фото|Иллюстрация|Источник|Комментарий|Автор)[:\s].*?(?=\s*[А-Я])',
            r'^\s*[А-Я]\.\s*',
            r'^\s*Цены на.*?(?=\s*[А-Я])',
            r'^\s*По.*?на прошлой неделе.*?(?=\s*[А-Я])',
            r'^\s*\d{1,2}\s+[А-Яа-я]+\s+\d{4}[^А-Яа-я]*',
            r'Ещё видео.*?(?=\s*[А-Я])',
            r'^\s*[А-Яа-я]+\s+(?:новости|пресс-релиз|стратегии)\b'
        ]

        for pattern in patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)

        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def extract_hashtags(self, title: str, content: str, creator: str = "") -> List[str]:
        """Извлекает хештеги на основе ключевых слов (макс. 5)"""
        text = f"{title} {content} {creator}".lower()

        hashtags = set()
        for keyword, tags in KEYWORDS_TO_HASHTAGS.items():
            if keyword.lower() in text:
                hashtags.update(tags)

        # Дополнительные правила для финансовой тематики
        extra = []
        if any(w in text for w in ["биржа", "трейдинг", "инвест"]):
            extra.append("#инвестиции")
        if any(w in text for w in ["крипто", "биткоин", "блокчейн"]):
            extra.append("#криптовалюты")
        if any(w in text for w in ["нефть", "газ", "энергетика"]):
            extra.append("#энергетика")
        if any(w in text for w in ["санкции", "эмбарго", "ограничения"]):
            extra.append("#международныеОтношения")

        hashtags.update(extra)
        return sorted(set(hashtags))[:5]

    def get_relevant_emoji(self, title: str, content: str, creator: str = "") -> str:
        """Возвращает релевантный эмодзи"""
        text = f"{title} {content} {creator}".lower()
        for keyword, emoji in sorted(TOPIC_TO_EMOJI.items(), key=lambda x: len(x[0]), reverse=True):
            if keyword.lower() in text:
                return emoji
        return "📰"

    async def fetch_full_article_text(self, url: str) -> str:
        """Парсит полный текст статьи с веб-страницы"""
        try:
            async with self.session.get(url, headers=HEADERS, timeout=15) as response:
                if response.status != 200:
                    logger.warning(f"Не удалось загрузить статью {url}: статус {response.status}")
                    return ""
                html_text = await response.text()
                soup = BeautifulSoup(html_text, "html.parser")

                for elem in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside', 'advertisement', 'iframe']):
                    elem.decompose()

                domain = urlparse(url).netloc
                selectors = []

                if "finam.ru" in domain:
                    selectors = ['.article__body', '.content', 'article']
                elif "cbr.ru" in domain:
                    selectors = ['.content', '.text', 'article']
                elif "vedomosti.ru" in domain:
                    selectors = ['.article__body', '.article-body', 'article']
                elif "arb.ru" in domain:
                    selectors = ['.news-detail', '.content', 'article']
                elif "kommersant.ru" in domain:
                    selectors = ['.article__text', '.article-text', 'article']
                elif "rbc.ru" in domain:
                    selectors = ['.article__text', '.article__content', 'article']
                elif "banki.ru" in domain:
                    selectors = ['.news-text', '.article-content', 'article']
                else:
                    selectors = ['.article-content', '.post-content', '.entry-content', 
                                '.article__body', '.article-body', 'article', '.content']

                content = None
                for selector in selectors:
                    try:
                        if selector.startswith('.'):
                            content = soup.find(class_=selector[1:])
                        elif selector.startswith('#'):
                            content = soup.find(id=selector[1:])
                        else:
                            content = soup.find(selector)
                        if content:
                            break
                    except:
                        continue

                if not content:
                    all_texts = soup.find_all(text=True)
                    text_blocks = [t.parent for t in all_texts if len(t.strip()) > 50]
                    text_blocks.sort(key=lambda x: len(x.get_text()), reverse=True)
                    content = text_blocks[0] if text_blocks else soup.find('body')

                if content:
                    return self.clean_text(content.get_text())
                return ""
        except Exception as e:
            logger.error(f"Ошибка при парсинге статьи {url}: {e}")
            return ""

    def smart_truncate(self, text: str, max_length: int = MAX_CONTENT_LENGTH) -> str:
        """Обрезает текст по границе предложений"""
        if len(text) <= max_length:
            return text
        truncated = text[:max_length + 1]
        last_end = max(truncated.rfind('.'), truncated.rfind('!'), truncated.rfind('?'))
        if last_end > max_length - 100:
            return truncated[:last_end + 1]
        else:
            return truncated[:max_length] + "…"

    def format_message(self, title: str, content: str, url: str, creator: str = "") -> str:
        """Форматирует сообщение с HTML-разметкой"""
        title = re.sub(r'\s*\d{1,2}\s+[А-Яа-я]+\s+\d{4}\s*года?\b', '', title, flags=re.IGNORECASE)
        
        full_text = self.clean_text(content)
        if full_text.startswith(title):
            full_text = full_text[len(title):].lstrip(":.- ")
        
        truncated_content = self.smart_truncate(full_text, MAX_CONTENT_LENGTH)

        sentences = re.split(r'(?<=[.!?])\s+', truncated_content)
        paragraphs = []
        current_paragraph = ""
        
        for sentence in sentences:
            if len(current_paragraph) + len(sentence) <= 120:
                current_paragraph += " " + sentence if current_paragraph else sentence
            else:
                if current_paragraph and len(current_paragraph.strip()) > 20:
                    paragraphs.append(current_paragraph.strip())
                current_paragraph = sentence
        
        if current_paragraph and len(current_paragraph.strip()) > 20:
            paragraphs.append(current_paragraph.strip())

        formatted_content = "\n\n".join(f"  {p}" for p in paragraphs)
        
        hashtags = self.extract_hashtags(title, content, creator)
        hashtag_line = " ".join(hashtags) if hashtags else ""
        emoji = self.get_relevant_emoji(title, content, creator)

        message = (
            f"<b>{emoji} {title}</b>\n\n"
            f"{formatted_content}\n\n"
            f"👉 <a href='{url}'>Читать далее</a>\n\n"
            f"{hashtag_line}"
        )
        return message

    async def fetch_feed(self, url: str) -> List[Dict]:
        """Парсинг RSS-ленты с улучшенной очисткой"""
        try:
            async with self.session.get(url, headers=HEADERS, timeout=15) as response:
                if response.status != 200:
                    logger.warning(f"Ошибка HTTP {response.status} при запросе {url}")
                    self.failed_sources.add(url)
                    return []
                
                content = await response.text()
                feed = feedparser.parse(content)
                entries = []
                
                for entry in feed.entries:
                    title = entry.get("title", "").strip()
                    if "видео" in title.lower() or "video" in title.lower():
                        continue

                    title = re.sub(r'^(.*?)(?:\s*-\s*[А-Яа-я]+)*$', r'\1', title)
                    link = entry.get("link", "").strip()
                    description = entry.get("description", "") or entry.get("summary", "")

                    if "<![CDATA[" in description:
                        description = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', description, flags=re.DOTALL)
                    
                    soup_desc = BeautifulSoup(description, "html.parser")
                    description = soup_desc.get_text()

                    description = re.sub(r'^\s*[А-Я]\.\s*[А-Я][а-я]+\s*/\s*[А-Я][а-я]+', '', description)
                    description = re.sub(r'^\s*[А-Я][а-я]+\s+[А-Я]\.\s*', '', description)
                    description = re.sub(r'^\s*(?:Автор|Источник):?\s*[^\s]+\s*', '', description, flags=re.IGNORECASE)

                    creator = ""
                    if hasattr(entry, "tags") and entry.tags:
                        creator = " ".join([tag.term for tag in entry.tags[:2]])
                    elif hasattr(entry, "author"):
                        creator = entry.author
                    elif hasattr(entry, "dc") and hasattr(entry.dc, "creator"):
                        creator = entry.dc.creator
                    elif hasattr(entry, "creator"):
                        creator = entry.creator

                    if title and link:
                        entries.append({
                            "title": title,
                            "url": link,
                            "content": description,
                            "creator": creator or "",
                            "source": url
                        })
                
                logger.info(f"Получено {len(entries)} новостей из {urlparse(url).netloc}")
                return entries
                
        except Exception as e:
            logger.error(f"Ошибка при парсинге {url}: {e}")
            self.failed_sources.add(url)
            return []

    async def publish_post(self, title: str, content: str, url: str, creator: str = "") -> bool:
        """Публикует пост с повторными попытками"""
        if self.is_duplicate(url, title):
            logger.info(f"Пропущено (дубликат): {title[:50]}...")
            return False

        full_text = await self.fetch_full_article_text(url)
        use_text = full_text if full_text.strip() else content

        if len(self.clean_text(use_text)) < MIN_CONTENT_LENGTH:
            logger.info(f"Пропущено (мало текста): {title[:50]}...")
            return False

        max_retries = 3
        for attempt in range(max_retries):
            try:
                message = self.format_message(title, use_text, url, creator)
                await self.bot.send_message(
                    chat_id=self.channel_id,
                    text=message,
                    parse_mode='HTML',
                    disable_web_page_preview=False
                )
                logger.info(f"✅ Опубликовано: {title[:50]}...")
                self.save_hash(url, title)
                return True
            except error.RetryAfter as e:
                logger.warning(f"⚠️ Превышен лимит, ожидание {e.retry_after} сек...")
                await asyncio.sleep(e.retry_after)
            except error.TimedOut:
                logger.warning(f"⚠️ Таймаут при публикации, попытка {attempt + 1}/{max_retries}")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"❌ Ошибка при публикации '{title}' (попытка {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    return False
                await asyncio.sleep(2 ** attempt)
        
        return False

    def generate_post_schedule(self) -> List[datetime]:
        """Генерирует расписание: первая публикация сразу, остальные случайно"""
        now = datetime.now()
        times = [now]  # Первая публикация сразу
        
        for _ in range(MAX_POSTS_PER_DAY - 1):
            random_hours = random.randint(1, 6)
            random_minutes = random.randint(0, 59)
            next_time = now + timedelta(hours=random_hours, minutes=random_minutes)
            
            if next_time.hour >= 22:
                next_time = next_time.replace(hour=21, minute=0)
            
            times.append(next_time)
            now = next_time
        
        return sorted(times)

    def avoid_consecutive_sources(self, posts: List[Dict]) -> List[Dict]:
        """Перемешивает посты, чтобы не было >2 подряд из одного источника"""
        if len(posts) < 3:
            return posts

        source_groups = {}
        for post in posts:
            source = post["source"]
            if source not in source_groups:
                source_groups[source] = []
            source_groups[source].append(post)

        sorted_groups = sorted(source_groups.items(), key=lambda x: len(x[1]), reverse=True)
        
        result = []
        
        while any(groups for _, groups in sorted_groups):
            for i, (source, group) in enumerate(sorted_groups):
                if group and (not result or result[-1]["source"] != source):
                    result.append(group.pop(0))
                    break
            else:
                for i, (source, group) in enumerate(sorted_groups):
                    if group:
                        result.append(group.pop(0))
                        break
        
        return result

    async def run(self):
        """Основной цикл бота с обходом блокировок"""
        connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=20)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            self.session = session
            self.cleanup_old_hashes()

            all_news = []
            seen_urls = set()

            # Сначала пробуем основные источники
            for source in RSS_SOURCES:
                try:
                    if source in self.failed_sources:
                        continue
                        
                    news = await self.fetch_feed(source)
                    for item in news:
                        if item["url"] not in seen_urls:
                            all_news.append(item)
                            seen_urls.add(item["url"])
                    await asyncio.sleep(2)  # Задержка между запросами
                except Exception as e:
                    logger.error(f"Ошибка при обработке источника {source}: {e}")
                    self.failed_sources.add(source)

            # Если новостей мало, пробуем резервные источники
            if len(all_news) < MAX_POSTS_PER_DAY:
                logger.info("Пробуем резервные источники...")
                for backup_source in BACKUP_SOURCES:
                    try:
                        if backup_source in self.failed_sources:
                            continue
                            
                        news = await self.fetch_feed(backup_source)
                        for item in news:
                            if item["url"] not in seen_urls:
                                all_news.append(item)
                                seen_urls.add(item["url"])
                        await asyncio.sleep(2)
                    except Exception as e:
                        logger.error(f"Ошибка при обработке резервного источника {backup_source}: {e}")
                        self.failed_sources.add(backup_source)

            # Фильтрация дубликатов
            all_news = [n for n in all_news if not self.is_duplicate(n["url"], n["title"])]
            
            if not all_news:
                logger.warning("Не найдено новых новостей для публикации")
                return
                
            if len(all_news) < MAX_POSTS_PER_DAY:
                logger.warning(f"Недостаточно новостей: {len(all_news)} из {MAX_POSTS_PER_DAY}")

            random.shuffle(all_news)
            selected_posts = all_news[:MAX_POSTS_PER_DAY]
            final_posts = self.avoid_consecutive_sources(selected_posts)

            schedule = self.generate_post_schedule()
            logger.info(f"Расписание публикаций: {', '.join(t.strftime('%H:%M') for t in schedule)}")

            # Публикация по расписанию
            published_count = 0
            for i, post_time in enumerate(schedule):
                now = datetime.now()
                if i > 0:
                    if now < post_time:
                        wait_seconds = (post_time - now).total_seconds()
                        logger.info(f"⏳ Ожидание до {post_time.strftime('%H:%M')} — {int(wait_seconds)} сек...")
                        await asyncio.sleep(wait_seconds)

                if i < len(final_posts):
                    post = final_posts[i]
                    success = await self.publish_post(
                        title=post["title"],
                        content=post["content"],
                        url=post["url"],
                        creator=post.get("creator", "")
                    )
                    if success:
                        published_count += 1
                else:
                    logger.warning(f"Нет поста для публикации в слот {i}")

            logger.info(f"🔚 Цикл завершён. Опубликовано {published_count} из {len(schedule)} запланированных публикаций.")


# === Запуск бота ===
async def main():
    if not BOT_TOKEN:
        logger.critical("❌ BOT_TOKEN не задан в .env файле!")
        return
    if not CHANNEL_ID:
        logger.critical("❌ CHANNEL_ID не задан в .env файле!")
        return

    bot = NewsBot(BOT_TOKEN, CHANNEL_ID)
    try:
        await bot.run()
    except Exception as e:
        logger.critical(f"💥 Критическая ошибка: {e}")


if __name__ == "__main__":
    asyncio.run(main())