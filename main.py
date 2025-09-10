import asyncio
import hashlib
import html
import json
import logging
import os
import random
import re
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional, Tuple
from urllib.parse import urlparse

import aiohttp
import feedparser
import pytz
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

# Отключаем логирование URL запросов к Telegram API (чтобы не светить токен)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Константы
RSS_SOURCES = [
    "https://www.finam.ru/analytics/rsspoint/",
    "https://www.cbr.ru/rss/eventrss",
    "https://www.vedomosti.ru/rss/rubric/finance/banks.xml",
    "https://arb.ru/rss/news/",
    "https://www.kommersant.ru/RSS/news.xml",
    "https://www.bfm.ru/news.rss?rubric=28",
    "https://ria.ru/export/rss2/archive/index.xml"
]

# Альтернативные источники на случай блокировки основных
BACKUP_SOURCES = [
    "https://www.kommersant.ru/RSS/bank.xml",
    "https://www.rbc.ru/rss/economics.xml",
    "https://www.interfax.ru/rss.asp",
    "https://www.vestifinance.ru/rss/news"
]

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

# Проверка критически важных переменных окружения
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не задан в .env файле!")
if not CHANNEL_ID:
    raise ValueError("❌ CHANNEL_ID не задан в .env файле!")

# Валидация CHANNEL_ID: для супергрупп и каналов должен начинаться с -100
if CHANNEL_ID.lstrip('-').isdigit() and len(CHANNEL_ID.lstrip('-')) >= 10 and not CHANNEL_ID.startswith('-100'):
    CHANNEL_ID = '-100' + CHANNEL_ID.lstrip('-')

MAX_POSTS_PER_DAY = 3
MAX_CONTENT_LENGTH = 800
MIN_CONTENT_LENGTH = 50

# User-Agent для обхода блокировок
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
]

HEADERS = {
    'User-Agent': random.choice(USER_AGENTS)
}

# Ключевые слова для фильтрации финансовых новостей
FINANCE_KEYWORDS = [
    'банк', 'кредит', 'ипотека', 'вклад', 'депозит', 'акция', 'облигация',
    'рубль', 'доллар', 'евро', 'инфляция', 'ставка', 'ЦБ', 'ФРС', 'биржа',
    'криптовалюта', 'нефть', 'газ', 'экономика', 'рынок', 'инвест', 'финанс',
    'ликвидность', 'дивиденд', 'кризис', 'санкции', 'регулятор', 'центральный банк',
    'кредитная', 'заем', 'займ', 'рефинансирование', 'ипотечный', 'вкладной',
    'сбережения', 'инвестиционный', 'фондовый', 'валютный', 'курс', 'обмен',
    'платеж', 'перевод', 'карта', 'дебетовая', 'кредитная карта', 'брокер',
    'трейдинг', 'котировки', 'индекс', 'рыночная', 'капитализация', 'актив',
    'пассив', 'баланс', 'отчетность', 'прибыль', 'убыток', 'дивиденды',
    'выплаты', 'квартальный', 'годовой', 'отчет', 'аудит', 'надзор', 'лицензия',
    'отзыв лицензии', 'санация', 'банкротство', 'форекс', 'трейдер', 'инвестор',
    'портфель', 'диверсификация', 'риск', 'доходность', 'процент', 'ставка рефинансирования',
    'ключевая ставка', 'монетарный', 'фискальный', 'бюджет', 'налог', 'сбор',
    'тариф', 'страхование', 'страховая', 'пенсионный', 'накопительный', 'ипотечное кредитование',
    'потребительское кредитование', 'микрокредит', 'МФО', 'лизинг', 'факторинг'
]

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
        self.session: Optional[aiohttp.ClientSession] = None
        self.posted_hashes: Set[str] = set()
        self.failed_sources: Set[str] = set()
        self.source_priority: Dict[str, int] = {}
        self.deleted_posts_tracker: Dict[str, datetime] = {}
        self.last_publication_time: Optional[datetime] = None
        
        self.load_hashes()
        self.load_source_stats()

    def load_hashes(self):
        """Загружает хеши опубликованных новостей"""
        try:
            if os.path.exists('posted_hashes.txt'):
                with open('posted_hashes.txt', 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    # Берем последние 500 строк для предотвращения разрастания
                    recent_lines = lines[-500:] if len(lines) > 500 else lines
                    self.posted_hashes = set(line.strip() for line in recent_lines if line.strip())
                logger.info(f"Загружено {len(self.posted_hashes)} хешей из истории.")
        except Exception as e:
            logger.error(f"Ошибка при загрузке хешей: {e}")

    def load_source_stats(self):
        """Загружает статистику по источникам"""
        try:
            if os.path.exists('source_stats.json'):
                with open('source_stats.json', 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.source_priority = data.get('priority', {})
                    # Конвертируем строки дат обратно в datetime
                    deleted_data = data.get('deleted', {})
                    self.deleted_posts_tracker = {
                        source: datetime.fromisoformat(date_str)
                        for source, date_str in deleted_data.items()
                    }
                logger.info(f"Загружена статистика для {len(self.source_priority)} источников")
        except Exception as e:
            logger.error(f"Ошибка при загрузке статистики источников: {e}")

    def save_source_stats(self):
        """Сохраняет статистику по источникам"""
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

    def is_duplicate(self, url: str, title: str) -> bool:
        """Проверяет дубликаты по хешу URL и заголовка"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        title_hash = hashlib.md5(title.encode()).hexdigest()
        combined = f"{url_hash}_{title_hash}"
        return combined in self.posted_hashes

    def track_deleted_post(self, source: str):
        """Отслеживает удаленные посты для балансировки источников"""
        now = datetime.now()
        self.deleted_posts_tracker[source] = now
        
        # Понижаем приоритет источника, если его посты удаляются
        self.source_priority[source] = self.source_priority.get(source, 0) - 2
        self.save_source_stats()
        logger.info(f"Понижен приоритет источника {urlparse(source).netloc}")

    def get_source_weight(self, source: str) -> float:
        """Возвращает вес источника с учетом приоритета и времени последнего удаления"""
        base_weight = 1.0
        
        # Учет приоритета
        priority = self.source_priority.get(source, 0)
        priority_factor = max(0.1, 1.0 + (priority * 0.1))
        
        # Учет времени с последнего удаления
        if source in self.deleted_posts_tracker:
            last_deleted = self.deleted_posts_tracker[source]
            hours_since_deletion = (datetime.now() - last_deleted).total_seconds() / 3600
            # Чем больше времени прошло, тем выше вес
            time_factor = min(2.0, 1.0 + (hours_since_deletion / 24))
        else:
            time_factor = 1.0
        
        return base_weight * priority_factor * time_factor

    def prioritize_sources(self, news_items: List[Dict]) -> List[Dict]:
        """Приоритизирует новости с учетом веса источников"""
        if not news_items:
            return []
        
        # Группируем новости по источникам
        news_by_source = {}
        for item in news_items:
            source = item["source"]
            if source not in news_by_source:
                news_by_source[source] = []
            news_by_source[source].append(item)
        
        # Вычисляем веса для каждого источника
        source_weights = {}
        for source in news_by_source.keys():
            source_weights[source] = self.get_source_weight(source)
        
        # Сортируем источники по весу (в порядке убывания)
        sorted_sources = sorted(news_by_source.keys(), 
                               key=lambda x: source_weights[x], 
                               reverse=True)
        
        # Отбираем лучшие новости из каждого источника
        selected_news = []
        for source in sorted_sources:
            # Берем не более 1 новости из источника за цикл
            if news_by_source[source]:
                selected_news.append(news_by_source[source][0])
        
        return selected_news[:MAX_POSTS_PER_DAY]

    def is_finance_related(self, title: str, content: str) -> bool:
        """Проверяет, относится ли новость к банковской/финансовой тематике"""
        text = f"{title} {content}".lower()
        # Проверяем наличие хотя бы одного финансового ключевого слова
        return any(keyword in text for keyword in FINANCE_KEYWORDS)

    @staticmethod
    def clean_text(text: str) -> str:
        """Очищает текст от HTML, авторов, рекламы, лишних пробелов и мусора"""
        if not text:
            return ""

        soup = BeautifulSoup(text, "html.parser")

        # Удаляем рекламные и навигационные блоки
        for elem in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside', 'advertisement', 'iframe', 'form']):
            elem.decompose()

        # Удаляем специфические рекламные вставки
        for clip in soup.find_all(class_=lambda x: x and any(word in str(x).lower() for word in ['clip', 'ad', 'banner', 'promo', 'recommended', 'social', 'share'])):
            clip.decompose()

        # Удаляем списки банков, продуктов и прочего мусора
        for ul in soup.find_all('ul'):
            text_content = ul.get_text()
            if any(keyword in text_content.lower() for keyword in ['банк', 'вклад', 'кредит', 'карта', 'ипотека', 'реклам']):
                ul.decompose()

        text = soup.get_text()
        text = html.unescape(text)

        # Удаляем информацию об авторе, источнике, дате и прочий мусор
        patterns = [
            r'^\s*[А-Я][а-я]+\s+[А-Я]\.[А-Я]\.?',
            r'^\s*[А-Я][а-я]+(?:\s+[А-Я][а-я]+)?\s*/\s*[А-Я][а-я]+',
            r'^\s*—\s*[^\n]+',
            r'(Фото|Иллюстрация|Источник|Комментарий|Автор|Читать подробнее|Редакция|Корреспондент)[:\s].*?(?=\s*[А-Я])',
            r'^\s*[А-Я]\.\s*',
            r'^\s*Цены на.*?(?=\s*[А-Я])',
            r'^\s*По.*?на прошлой неделе.*?(?=\s*[А-Я])',
            r'^\s*\d{1,2}\s+[А-Яа-я]+\s+\d{4}[^А-Яа-я]*',
            r'Ещё видео.*?(?=\s*[А-Я])',
            r'^\s*[А-Яа-я]+\s+(?:новости|пресс-релиз|стратегии)\b',
            r'\* Рейтинг составлен.*',
            r'Рейтинг составлен.*',
            r'Чтобы не.*',
            r'Инна Солдатенкова.*',
            r'Эксперт-аналитик.*',
            r'Аналитик Банки\.ру.*',
            r'Рассчитать сумму.*',
            r'https?://\S+',
            r'\d{1,2}:\d{2}',  # время
            r'\d{1,2}\s*[а-я]+\s+\d{4}',  # даты
            r'Читайте также.*',
            r'Реклама.*',
            r'Материал.*партнеров',
            r'Обсудить в телеграме.*',
            r'Подпишитесь на.*',
            r'Мы в соцсетях.*',
            r'Прислать новость.*',
            r'Комментарии.*',
            r'Телеграм-канал.*',
            r'Подписывайтесь.*',
        ]

        for pattern in patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)

        # Удаляем последовательности из 3+ восклицательных или вопросительных знаков
        text = re.sub(r'[!?]{3,}', ' ', text)

        # Заменяем множественные пробелы и переносы строк на один пробел
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
        """Парсит полный текст статьи с веб-страницы с повторными попытками"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                headers = HEADERS.copy()
                headers['User-Agent'] = random.choice(USER_AGENTS)

                async with self.session.get(url, headers=headers, timeout=15) as response:
                    if response.status != 200:
                        logger.warning(f"Не удалось загрузить статью {url}: статус {response.status}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt)
                        continue

                    html_text = await response.text()
                    soup = BeautifulSoup(html_text, "html.parser")

                    for elem in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside', 'advertisement', 'iframe', 'form']):
                        elem.decompose()

                    # Удаляем рекламные блоки
                    for ad in soup.find_all(class_=lambda x: x and any(word in str(x).lower() for word in ['ad', 'banner', 'promo', 'recommended', 'social', 'share'])):
                        ad.decompose()

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
                        except Exception:
                            continue

                    if not content:
                        all_texts = soup.find_all(text=True)
                        text_blocks = [t.parent for t in all_texts if len(t.strip()) > 50]
                        text_blocks.sort(key=lambda x: len(x.get_text()), reverse=True)
                        content = text_blocks[0] if text_blocks else soup.find('body')

                    if content:
                        return self.clean_text(content.get_text())
                    return ""

            except asyncio.TimeoutError:
                logger.warning(f"Таймаут при парсинге статьи {url}, попытка {attempt + 1}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Ошибка при парсинге статьи {url}: {e}, попытка {attempt + 1}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)

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
        """Форматирует сообщение с HTML-разметкой, проверяя длину и форматируя абзацы"""
        # Удаляем даты из заголовка
        title = re.sub(r'\s*\d{1,2}\s+[А-Яа-я]+\s+\d{4}\s*года?\b', '', title, flags=re.IGNORECASE).strip()

        full_text = self.clean_text(content)
        if full_text.startswith(title):
            full_text = full_text[len(title):].lstrip(":.- ")

        truncated_content = self.smart_truncate(full_text, MAX_CONTENT_LENGTH)

        # Разбиваем на предложения и формируем абзацы по 1-2 предложения
        sentences = re.split(r'(?<=[.!?])\s+', truncated_content)
        paragraphs = []
        current_paragraph = ""

        for sentence in sentences:
            if not current_paragraph:
                current_paragraph = sentence
            elif len(current_paragraph.split('. ')) < 2:  # Если в абзаце меньше 2 предложений
                current_paragraph += " " + sentence
            else:
                paragraphs.append(current_paragraph.strip())
                current_paragraph = sentence

        if current_paragraph:
            paragraphs.append(current_paragraph.strip())

        # Форматируем абзацы
        formatted_content = "\n\n".join(f"{p}" for p in paragraphs if len(p) > 20)

        hashtags = self.extract_hashtags(title, content, creator)
        hashtag_line = "\n\n" + " ".join(hashtags) if hashtags else ""
        emoji = self.get_relevant_emoji(title, content, creator)

        message = (
            f"<b>{emoji} {title}</b>\n\n"
            f"{formatted_content}\n\n"
            f"👉 <a href='{url}'>Читать далее</a>"
            f"{hashtag_line}"
        )

        # Проверка длины сообщения (лимит Telegram ~4096 символов)
        if len(message) > 3900:
            message = message[:3897] + "..."

        return message

    async def fetch_feed(self, url: str) -> List[Dict]:
        """Парсинг RSS-ленты с улучшенной очисткой и повторными попытками"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                headers = HEADERS.copy()
                headers['User-Agent'] = random.choice(USER_AGENTS)

                async with self.session.get(url, headers=headers, timeout=15) as response:
                    if response.status != 200:
                        logger.warning(f"Ошибка HTTP {response.status} при запросе {url}")
                        self.failed_sources.add(url)
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt)
                        continue

                    content = await response.text()
                    # Используем to_thread для синхронной библиотеки feedparser
                    feed = await asyncio.to_thread(feedparser.parse, content)
                    entries = []

                    for entry in feed.entries:
                        title = entry.get("title", "").strip()
                        if "видео" in title.lower() or "video" in title.lower():
                            continue

                        # Очистка заголовка
                        title = re.sub(r'^(.*?)(?:\s*-\s*[А-Яа-я]+)*$', r'\1', title).strip()
                        link = entry.get("link", "").strip()
                        description = entry.get("description", "") or entry.get("summary", "")

                        if "<![CDATA[" in description:
                            description = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', description, flags=re.DOTALL)

                        # Очистка описания
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

                        if title and link and self.is_finance_related(title, description):
                            entries.append({
                                "title": title,
                                "url": link,
                                "content": description,
                                "creator": creator or "",
                                "source": url
                            })

                    logger.info(f"Получено {len(entries)} новостей из {urlparse(url).netloc}")
                    return entries

            except asyncio.TimeoutError:
                logger.warning(f"Таймаут при парсинге фида {url}, попытка {attempt + 1}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Ошибка при парсинге {url}: {e}, попытка {attempt + 1}")
                self.failed_sources.add(url)
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)

        return []

    async def publish_post(self, title: str, content: str, url: str, creator: str = "", source: str = "") -> bool:
        """Публикует пост с повторными попытками"""
        if self.is_duplicate(url, title):
            logger.info(f"Пропущено (дубликат): {title[:50]}...")
            return False

        # Дополнительная проверка на финансовую тематику
        if not self.is_finance_related(title, content):
            logger.info(f"Пропущено (не финансовая тематика): {title[:50]}...")
            return False

        full_text = await self.fetch_full_article_text(url)
        use_text = full_text if full_text.strip() else content

        cleaned_text = self.clean_text(use_text)
        if len(cleaned_text) < MIN_CONTENT_LENGTH:
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
                
                # Повышаем приоритет успешного источника
                if source:
                    self.source_priority[source] = self.source_priority.get(source, 0) + 1
                    self.save_source_stats()
                
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
                    # Отслеживаем неудачные публикации
                    if source:
                        self.track_deleted_post(source)
                    return False
                await asyncio.sleep(2 ** attempt)

        return False

    def generate_post_schedule(self) -> List[datetime]:
        """Генерирует расписание публикаций строго с 9:00 до 21:00 по Мск"""
        try:
            moscow_tz = pytz.timezone('Europe/Moscow')
            now_moscow = datetime.now(moscow_tz)
            
            # Определяем временное окно (9:00-21:00 по Мск)
            start_hour, end_hour = 9, 21
            
            # Если сейчас вне рабочего времени, начинаем с 9:00 следующего дня
            if now_moscow.hour < start_hour:
                first_post_time = now_moscow.replace(
                    hour=start_hour, minute=0, second=0, microsecond=0
                )
            elif now_moscow.hour >= end_hour:
                first_post_time = now_moscow.replace(
                    hour=start_hour, minute=0, second=0, microsecond=0
                ) + timedelta(days=1)
            else:
                # Текущее время в рабочем окне
                first_post_time = now_moscow
            
            # Генерируем времена публикаций
            times = []
            available_minutes = (end_hour - start_hour) * 60
            time_slots = MAX_POSTS_PER_DAY
            
            # Равномерно распределяем публикации в течение дня
            for i in range(time_slots):
                # Вычисляем позицию в минутном интервале
                minute_position = available_minutes * (i + 1) / (time_slots + 1)
                random_variation = random.uniform(-30, 30)  # ±30 минут вариация
                
                total_minutes = minute_position + random_variation
                hours_to_add = int(total_minutes // 60)
                minutes_to_add = int(total_minutes % 60)
                
                post_time = first_post_time.replace(
                    hour=start_hour + hours_to_add,
                    minute=minutes_to_add,
                    second=0,
                    microsecond=0
                )
                
                # Обеспечиваем, чтобы время было в пределах 9:00-21:00
                if post_time.hour < start_hour:
                    post_time = post_time.replace(hour=start_hour, minute=random.randint(0, 30))
                elif post_time.hour >= end_hour:
                    post_time = post_time.replace(hour=end_hour - 1, minute=random.randint(30, 59))
                
                times.append(post_time)
            
            return sorted(times)
            
        except Exception as e:
            logger.error(f"Ошибка при генерации расписания: {e}")
            # Резервное расписание на случай ошибки
            return [datetime.now() + timedelta(minutes=30 * i) for i in range(MAX_POSTS_PER_DAY)]

    def avoid_consecutive_sources(self, posts: List[Dict]) -> List[Dict]:
        """Перемешивает посты, чтобы не было подряд из одного источника"""
        if len(posts) < 2:
            return posts
        
        # Группируем посты по источникам
        posts_by_source = {}
        for post in posts:
            source = post["source"]
            if source not in posts_by_source:
                posts_by_source[source] = []
            posts_by_source[source].append(post)
        
        # Сортируем источники по количеству постов
        sorted_sources = sorted(posts_by_source.keys(), key=lambda x: len(posts_by_source[x]), reverse=True)
        
        result = []
        while any(posts_by_source.values()):
            for source in sorted_sources:
                if posts_by_source[source]:
                    # Берем пост из текущего источника
                    post = posts_by_source[source].pop(0)
                    result.append(post)
                    
                    # Проверяем, не идут ли два поста подряд из одного источника
                    if len(result) >= 2 and result[-2]["source"] == result[-1]["source"]:
                        # Если да, ищем пост из другого источника для вставки
                        for other_source in sorted_sources:
                            if other_source != source and posts_by_source[other_source]:
                                insert_post = posts_by_source[other_source].pop(0)
                                result.insert(-1, insert_post)
                                break
        
        return result[:MAX_POSTS_PER_DAY]

    async def run(self):
        """Основной цикл бота с улучшенной ротацией источников"""
        connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=20)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            self.session = session

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
                        logger.error(f"Ошибка при обработке резервного источника{backup_source}: {e}")
                        self.failed_sources.add(backup_source)

            # Фильтрация новостей
            filtered_news = []
            for item in all_news:
                if (not self.is_duplicate(item["url"], item["title"]) and
                    self.is_finance_related(item["title"], item["content"]) and
                    len(self.clean_text(item["content"])) >= MIN_CONTENT_LENGTH):
                    filtered_news.append(item)

            # Приоритизация источников с учетом истории удалений
            prioritized_news = self.prioritize_sources(filtered_news)

            if not prioritized_news:
                logger.info("Нет подходящих новостей для публикации.")
                return

            # Избегаем последовательных публикаций из одного источника
            final_news = self.avoid_consecutive_sources(prioritized_news)

            # Генерируем расписание публикаций
            schedule = self.generate_post_schedule()
            
            logger.info(f"Сгенерировано расписание на {len(schedule)} публикаций:")
            for i, pub_time in enumerate(schedule, 1):
                logger.info(f"  {i}. {pub_time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Публикуем новости по расписанию
            for i, (news_item, pub_time) in enumerate(zip(final_news, schedule)):
                # Ждем до времени публикации
                now = datetime.now(pytz.timezone('Europe/Moscow'))
                if pub_time > now:
                    wait_seconds = (pub_time - now).total_seconds()
                    logger.info(f"Ожидание до публикации {i+1}: {int(wait_seconds)} секунд")
                    await asyncio.sleep(wait_seconds)

                # Публикуем новость
                success = await self.publish_post(
                    title=news_item["title"],
                    content=news_item["content"],
                    url=news_item["url"],
                    creator=news_item.get("creator", ""),
                    source=news_item["source"]
                )

                if not success and i < len(final_news) - 1:
                    logger.warning("Публикация не удалась, переходим к следующей новости")
                    continue

                # Пауза между публикациями (если есть еще новости)
                if i < len(final_news) - 1:
                    await asyncio.sleep(random.uniform(5, 15))

            logger.info("✅ Цикл публикаций завершен")

            # Очистка списка неудачных источников раз в день
            if len(self.failed_sources) > 0 and datetime.now().hour == 0:
                logger.info("Очистка списка неудачных источников")
                self.failed_sources.clear()


async def main():
    """Главная функция запуска бота"""
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