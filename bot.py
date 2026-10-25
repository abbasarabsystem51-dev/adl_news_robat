"""ربات خبری عدل نیوز: جمع‌آوری خبر از RSS، انتخاب و بازنویسی با Gemini، ارسال به ایتا.

ترتیب کار در هر ران:
    فیدها → حذف خبر تکراری → انتخاب متعادل از هر دسته → گرفتن متن کامل خبر
    → یک درخواست به Gemini (با fallback بین مدل‌ها) → ارسال پست‌ها (با عکس یا متن) → ذخیره سابقه
"""

import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import feedparser
import requests
from google import genai
from google.genai import errors, types
from pydantic import BaseModel, Field

try:
    import trafilatura  # استخراج متن اصلی خبر؛ اگر نصب نبود، ربات فقط با خلاصه فید کار می‌کند
except ImportError:
    trafilatura = None

# غیرفعال کردن هشدارهای SSL برای جلوگیری از خطای گواهی در سایت‌های داخلی
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ══════════════════════════════════════════════════════════════
# ۱) تنظیمات و متغیرهای محیطی
# ══════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).resolve().parent
SEEN_FILE = BASE_DIR / "seen.json"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

GEMINI_KEY = os.environ.get("GEMINI_KEY", "").strip()
EITAA_TOKEN = os.environ.get("EITAA_TOKEN", "").strip()
CHANNEL = os.environ.get("CHANNEL", "").strip()

if not all([GEMINI_KEY, EITAA_TOKEN, CHANNEL]):
    print("❌ خطای تنظیمات: متغیرهای محیطی GEMINI_KEY، EITAA_TOKEN یا CHANNEL تنظیم نشده‌اند.")
    sys.exit(1)

CHANNEL = (
    CHANNEL.replace("https://eitaa.com/", "")
    .replace("http://eitaa.com/", "")
    .replace("@", "")
    .strip()
)

# چت اختیاری برای گزارش و هشدار خطا (کانال یا گروه خصوصی که ربات در آن ادمین است)
ALERT_CHAT = (
    os.environ.get("ALERT_CHAT", "").strip()
    .replace("https://eitaa.com/", "")
    .replace("http://eitaa.com/", "")
    .replace("@", "")
    .strip()
)
ALERT_MODE = os.environ.get("ALERT_MODE", "problems").strip().lower()  # problems = فقط وقت مشکل | always = هر ران

# ── محتوا ──
POSTS_COUNT = 4            # تعداد پست در هر ران
SEEN_LIMIT = 500           # تعداد لینک‌هایی که در سابقه نگه داشته می‌شود
ENTRIES_PER_FEED = 4       # تعداد خبر اخیر از هر فید
SUMMARY_CHARS = 350        # طول خلاصه فید
FULLTEXT_CHARS = 1200      # حداکثر طول متن اصلی که به مدل داده می‌شود
DEDUPE_THRESHOLD = 0.5     # شباهت تیتر (۰ تا ۱) برای تشخیص خبر تکراری بین منابع

# سهم هر دسته از ورودی مدل (مجموعاً ۲۵ خبر) تا هیچ دسته‌ای از قلم نیفتد
GROUP_QUOTAS = {"tech": 7, "science": 5, "sport": 6, "politics": 7}

# ── عکس ──
# در این تعداد دسته فقط خبرهای عکس‌دار به مدل داده می‌شود (پس آن پست‌ها حتماً عکس دارند).
# دسته‌های باقی‌مانده هم اگر خبرشان عکس داشت، با عکس ارسال می‌شود.
IMAGE_GROUPS_COUNT = 3
IMAGE_PRIORITY = ["tech", "sport", "politics", "science"]
EITAA_PHOTO_METHOD = "sendFile"  # نام متد ارسال عکس در API ایتایار؛ اگر متفاوت بود اینجا عوض کن
MAX_IMAGE_BYTES = 5_000_000
CAPTION_LIMIT = 1000

# ── جمع‌بندی روزانه ──
DIGEST_ENABLED = os.environ.get("DIGEST", "on").strip().lower() != "off"
DIGEST_MODELS = ["gemini-3.5-flash-lite", "gemini-3.6-flash"]  # مدل ارزان‌تر اول، تا سهمیه مدل‌های اصلی نسوزد
DIGEST_START_HOUR = 21     # بازه ارسال به وقت تهران: از ساعت ۲۱ تا ۲ بامداد (ران بعدی، ران ناموفق را جبران می‌کند)
DIGEST_END_HOUR = 2
DIGEST_MIN_POSTS = 3       # با کمتر از این تعداد خبر، جمع‌بندی ساخته نمی‌شود
DIGEST_MIN_GAP_HOURS = 12  # فاصله حداقلی بین دو جمع‌بندی
LOG_KEEP_DAYS = 3
LOG_FILE = Path(os.environ.get("POSTS_LOG_FILE") or BASE_DIR / "posts_log.json")
IRAN_TZ = timezone(timedelta(hours=3, minutes=30))  # ایران ساعت تابستانی ندارد
FEED_FAIL_ALERT = 3        # اگر حداقل این تعداد فید خراب بود، هشدار بده

# ── Gemini ──
# خطاهایی که ارزش تلاش مجدد روی همان مدل دارند (خطای موقت و سریع‌برگشت مثل 503).
RETRYABLE = {408, 500, 502, 503}
# این کدها فوراً می‌روند سراغ مدل بعدی (تکرار روی همان مدل فایده ندارد و وقت می‌سوزاند):
#   429 = پر شدن سهمیه (هر مدل سهمیه جدا دارد)
#   504 = تایم‌اوت سمت گوگل؛ دوباره همان درخواست سنگین هم احتمالاً تایم‌اوت می‌خورد
SKIP_MODEL_CODES = {429, 504}

# به ترتیب اولویت؛ مدل‌هایی که در API وجود ندارند خودکار حذف می‌شوند
MODELS = [
    "gemini-3.6-flash",   # در چند ران پیاپی از اولین تلاش جواب داده (سریع و پایدار)
    "gemini-3.8-flash",
    "gemini-3.7-flash",
    "gemini-3.5-flash",
    "gemini-3.5-flash-lite",
]

# ── منابع خبر؛ هر فید یک group دارد تا انتخاب متعادل باشد ──
GNEWS = "https://news.google.com/rss/search?q=when:{w}+site:reuters.com/{sec}&ceid=US:en&hl=en-US&gl=US"

RSS_FEEDS = [
    # --- تکنولوژی و هوش مصنوعی ---
    {"name": "تک‌کرانچ (هوش مصنوعی)", "group": "tech", "category": "تکنولوژی و هوش مصنوعی",
     "url": "https://techcrunch.com/category/artificial-intelligence/feed/"},
    {"name": "ورج (هوش مصنوعی)", "group": "tech", "category": "تکنولوژی و هوش مصنوعی",
     "url": "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml"},
    {"name": "آرس تکنیکا (هوش مصنوعی)", "group": "tech", "category": "تکنولوژی و هوش مصنوعی",
     "url": "https://arstechnica.com/ai/feed/"},
    {"name": "مجله فناوری MIT", "group": "tech", "category": "تکنولوژی و هوش مصنوعی",
     "url": "https://www.technologyreview.com/topic/artificial-intelligence/feed"},
    {"name": "دیجیاتو", "group": "tech", "category": "تکنولوژی و فناوری",
     "url": "https://digiato.com/feed"},
    {"name": "رویترز (فناوری)", "group": "tech", "category": "تکنولوژی و فناوری",
     "url": GNEWS.format(w="7d", sec="technology")},
    # --- علمی ---
    {"name": "آرس تکنیکا (علم)", "group": "science", "category": "علمی",
     "url": "https://feeds.arstechnica.com/arstechnica/science"},
    {"name": "گاردین (علم)", "group": "science", "category": "علمی",
     "url": "https://www.theguardian.com/science/rss"},
    {"name": "وایرد (علم)", "group": "science", "category": "علمی",
     "url": "https://www.wired.com/feed/category/science/latest/rss"},
    # --- ورزشی ---
    {"name": "ورزش سه", "group": "sport", "category": "ورزش",
     "url": "https://www.varzesh3.com/rss/all"},
    {"name": "اسکای اسپورتس", "group": "sport", "category": "ورزش",
     "url": "https://www.skysports.com/rss/12040"},
    {"name": "گاردین (ورزش)", "group": "sport", "category": "ورزش",
     "url": "https://www.theguardian.com/uk/sport/rss"},
    {"name": "رویترز (ورزش)", "group": "sport", "category": "ورزش",
     "url": GNEWS.format(w="7d", sec="sports")},
    # --- سیاسی و جنگ ---
    {"name": "الجزیره (بین‌الملل و جنگ)", "group": "politics", "category": "بین‌الملل و سیاست",
     "url": "https://www.aljazeera.com/xml/rss/all.xml"},
    {"name": "گاردین (جهان)", "group": "politics", "category": "بین‌الملل و سیاست",
     "url": "https://www.theguardian.com/world/rss"},
    {"name": "رویترز (جهان)", "group": "politics", "category": "بین‌الملل و سیاست",
     "url": GNEWS.format(w="24h", sec="world")},
    {"name": "خبرگزاری مهر", "group": "politics", "category": "سیاست و اخبار عمومی",
     "url": "https://www.mehrnews.com/rss"},
    {"name": "پرس‌تی‌وی", "group": "politics", "category": "بین‌الملل و سیاست",
     "url": "https://www.presstv.ir/rss.xml"},
]

DIGEST_PROMPT_TEMPLATE = """نقش: سردبیر ارشد «عدل نیوز». خبرهای تکنولوژی و هوش مصنوعی که امروز در کانال منتشر شده در زیر آمده است.

وظیفه:
1. «جمع‌بندی امروز»: مهم‌ترین موارد را به تعداد خبرهای ورودی (حداکثر ۶ مورد) بنویس؛ هر مورد فقط یک جمله کوتاه و دقیق.
2. «سؤال روز»: یک سؤال کوتاه و تعاملی درباره یکی از این خبرها بنویس که مخاطب را به نظر دادن دعوت کند (سؤال باز؛ نه بله/خیر).
3. دقیقاً ۳ هشتگ فارسی مرتبط.

قوانین سخت‌گیرانه:
1. فقط از اطلاعات همین اخبار استفاده کن؛ بدون حدس و اطلاعات خارج از متن.
2. عدم استفاده از ستاره (*) یا تگ HTML در متون.
3. لحن کوتاه و حرفه‌ای ژورنالیستی.

اخبار امروز:
{posts_text}
"""

PROMPT_TEMPLATE = """نقش: سردبیر ارشد «عدل نیوز». شما باید دقیقاً ۴ خبر برتر را از بین اخبار زیر انتخاب کنید، به طوری که تفکیک موضوعی زیر به دقت رعایت شود:
1. یک خبر حتماً درباره «تکنولوژی و هوش مصنوعی (AI)» باشد.
2. یک خبر حتماً «علمی» باشد.
3. یک خبر حتماً درباره «ورزش» باشد.
4. یک خبر حتماً درباره «جنگ، مسائل بین‌الملل یا سیاست» باشد.

قوانین سخت‌گیرانه:
1. بدون حدس و اطلاعات خارج از متن.
2. عدم استفاده از ستاره (*) یا تگ HTML در متون.
3. لحن کوتاه و حرفه‌ای ژورنالیستی.
4. در صورت برابری اهمیت، خبری که «پوشش» بیشتری (منابع بیشتر) دارد اولویت دارد.
5. متن هر خبر باید کامل و دقیق باشد و جزئیات مهم (اسامی، اعداد، زمان، مکان) را داشته باشد؛ نه خلاصه کوتاه و نه تحلیل شخصی.

اخبار ورودی:
{news_text}
"""


# ══════════════════════════════════════════════════════════════
# ۲) ساختار خروجی Gemini
# ══════════════════════════════════════════════════════════════

class AnalyzedNewsItem(BaseModel):
    index: int = Field(description="شماره ردیف خبر انتخاب شده")
    title: str = Field(description="تیتر جذاب و دقیق به فارسی")
    body: str = Field(
        description="متن کامل و دقیق خبر با جزئیات مهم (اسامی، اعداد، زمان، مکان) در ۴ تا ۵ جمله، "
                    "حدود ۴۰۰ تا ۵۰۰ کاراکتر؛ بدون تحلیل و بدون حدس"
    )
    hashtags: list[str] = Field(description="دقیقا ۳ هشتگ مرتبط به فارسی")


class AnalyzedNewsBatch(BaseModel):
    selected_news: list[AnalyzedNewsItem] = Field(description="لیست ۴ خبر برتر با تفکیک موضوعی")


class DailyDigest(BaseModel):
    items: list[str] = Field(description="موارد جمع‌بندی؛ هر مورد یک جمله کوتاه")
    question: str = Field(description="سؤال تعاملی روز برای مخاطبان")
    hashtags: list[str] = Field(description="دقیقا ۳ هشتگ مرتبط به فارسی")


# آمار همین ران؛ برای گزارش و هشدار خطا
STATS = {}


def _reset_stats():
    STATS.clear()
    STATS.update(feed_failed=[], posts_planned=0, posts_sent=0, photo_failed=0,
                 model=None, models_tried=0, digest=None)


_reset_stats()


# ══════════════════════════════════════════════════════════════
# ۳) سابقه خبرهای ارسال‌شده
# ══════════════════════════════════════════════════════════════

def normalize_link(url):
    return url.split("?")[0].rstrip("/").strip()


def load_seen():
    if not SEEN_FILE.exists():
        return []
    try:
        with SEEN_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def save_seen(seen_list):
    unique_seen = list(dict.fromkeys(seen_list))
    with SEEN_FILE.open("w", encoding="utf-8") as f:
        json.dump(unique_seen[-SEEN_LIMIT:], f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════════════════════════
# ۴) دریافت فیدها
# ══════════════════════════════════════════════════════════════

def clean_html(raw_html):
    text = re.sub(r"<[^>]+>", " ", raw_html or "")
    return re.sub(r"\s+", " ", text).strip()


def extract_image_url(entry):
    """اولین عکس معتبر از فید (media:content، media:thumbnail یا enclosure)."""
    for key in ("media_content", "media_thumbnail"):
        for m in entry.get(key, []) or []:
            url = str(m.get("url", ""))
            if url.startswith("http"):
                return url
    for enc in entry.get("enclosures", []) or []:
        if str(enc.get("type", "")).startswith("image/") and str(enc.get("href", "")).startswith("http"):
            return enc["href"]
    return None


def fetch_single_feed(feed_info, seen_set):
    headers = {"User-Agent": USER_AGENT}
    name = feed_info["name"]
    feed_articles = []
    try:
        try:
            resp = requests.get(feed_info["url"], headers=headers, timeout=6)
        except requests.exceptions.SSLError:
            # مدیریت خطای گواهی SSL برای خبرگزاری‌های داخلی
            resp = requests.get(feed_info["url"], headers=headers, timeout=6, verify=False)

        if resp.status_code != 200:
            print(f"❌ {name}: HTTP {resp.status_code}")
            STATS["feed_failed"].append(name)
            return []

        entries = feedparser.parse(resp.content).entries[:ENTRIES_PER_FEED]
        for entry in entries:
            link = normalize_link(entry.get("link", "").strip())
            title = clean_html(entry.get("title", ""))
            summary = clean_html(entry.get("summary", entry.get("description", "")))

            if link and title and link not in seen_set:
                feed_articles.append({
                    "source": name,
                    "category": feed_info["category"],
                    "group": feed_info["group"],
                    "title": title,
                    "summary": summary[:SUMMARY_CHARS] if summary else title,
                    "link": link,
                    "image": extract_image_url(entry),
                })

        if not entries:
            print(f"⚠️ {name}: فید خالی یا نامعتبر")
            STATS["feed_failed"].append(name)
        else:
            print(f"✅ {name}: {len(feed_articles)} خبر جدید از {len(entries)}")
    except Exception as e:
        print(f"⚡ عبور سریع از {name} به دلیل خطا یا کندی: {e}")
        STATS["feed_failed"].append(name)
    return feed_articles


def fetch_new_articles(seen_list):
    seen_set = set(seen_list)
    articles = []
    batch_links = set()

    # دریافت موازی تمامی فیدها
    with ThreadPoolExecutor(max_workers=len(RSS_FEEDS)) as executor:
        futures = [executor.submit(fetch_single_feed, feed, seen_set) for feed in RSS_FEEDS]
        for future in as_completed(futures):
            for art in future.result():
                if art["link"] not in batch_links:
                    batch_links.add(art["link"])
                    articles.append(art)

    return dedupe_articles(articles)


# ══════════════════════════════════════════════════════════════
# ۵) پردازش محلی (بدون مصرف سهمیه Gemini)
# ══════════════════════════════════════════════════════════════

_WORD_RE = re.compile(r"[\w\u0600-\u06FF]{4,}")


def _title_tokens(title):
    return {w.lower() for w in _WORD_RE.findall(title)}


def dedupe_articles(articles, threshold=DEDUPE_THRESHOLD):
    """خبرهای شبیه هم (در یک دسته) را یکی می‌کند.

    coverage = تعداد منابعی که این خبر را پوشش داده‌اند.
    links = لینک همه نسخه‌های ادغام‌شده؛ بعد از ارسال همه در سابقه ذخیره می‌شوند
            تا نسخه منبع دیگرِ همین خبر در رانِ بعد دوباره منتشر نشود.
    """
    kept = []
    for art in articles:
        tokens = _title_tokens(art["title"])
        dup = None
        for k in kept:
            if k["group"] != art["group"]:
                continue
            kt = k["_tokens"]
            if tokens and kt and len(tokens & kt) / len(tokens | kt) >= threshold:
                dup = k
                break
        if dup:
            dup["coverage"] += 1
            dup["links"].append(art["link"])
            if not dup.get("image") and art.get("image"):
                dup["image"] = art["image"]
            continue
        art["coverage"] = 1
        art["links"] = [art["link"]]
        art["_tokens"] = tokens
        kept.append(art)

    removed = len(articles) - len(kept)
    if removed:
        print(f"🧹 {removed} خبر تکراری بین منابع ادغام شد")
    return kept


def balance_articles(articles):
    """از هر دسته به‌اندازه سهمش خبر برمی‌دارد و بین منابع آن دسته نوبتی پخش می‌کند.

    در IMAGE_GROUPS_COUNT دسته (به ترتیب IMAGE_PRIORITY) فقط خبرهای عکس‌دار انتخاب
    می‌شوند. روی هر خبر انتخاب‌شده allow_image=True گذاشته می‌شود.
    """
    by_group = {}
    for art in articles:
        by_group.setdefault(art["group"], {}).setdefault(art["source"], []).append(art)

    groups_with_img = {
        g for g, srcs in by_group.items()
        if any(a.get("image") for q in srcs.values() for a in q)
    }
    image_groups = [g for g in IMAGE_PRIORITY if g in groups_with_img][:IMAGE_GROUPS_COUNT]
    print(f"🖼 دسته‌های دارای عکس در این ران: {image_groups or 'هیچ‌کدام'}")

    result = []
    for group, quota in GROUP_QUOTAS.items():
        image_only = group in image_groups
        queues = list(by_group.get(group, {}).values())
        if image_only:
            queues = [[a for a in q if a.get("image")] for q in queues]

        picked = []
        while len(picked) < quota and any(queues):
            for q in queues:
                if q and len(picked) < quota:
                    picked.append(q.pop(0))

        for art in picked:
            art["allow_image"] = True
        print(f"📊 دسته {group}: {len(picked)} خبر برای تحلیل" + (" (فقط عکس‌دار)" if image_only else ""))
        result.extend(picked)
    return result


def fetch_article_text(art):
    url = art["link"]
    if trafilatura is None or "news.google.com" in url:
        return None
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=6)
        if resp.status_code != 200:
            return None
        text = trafilatura.extract(resp.text, include_comments=False, include_tables=False)
        return re.sub(r"\s+", " ", text)[:FULLTEXT_CHARS] if text else None
    except Exception:
        return None


def enrich_with_fulltext(articles):
    """متن اصلی خبرهای نامزد را موازی می‌گیرد تا مدل جزئیات بیشتری داشته باشد."""
    if trafilatura is None or not articles:
        return
    with ThreadPoolExecutor(max_workers=10) as executor:
        texts = list(executor.map(fetch_article_text, articles))
    improved = 0
    for art, text in zip(articles, texts):
        if text and len(text) > len(art["summary"]):
            art["summary"] = text
            improved += 1
    print(f"📰 متن کامل برای {improved} از {len(articles)} خبر گرفته شد")


# ══════════════════════════════════════════════════════════════
# ۶) Gemini: انتخاب و بازنویسی
# ══════════════════════════════════════════════════════════════

def _available_models(client):
    """فقط مدل‌هایی که واقعاً در API هستند، تا 404 وقت تلف نکند."""
    try:
        names = {m.name.removeprefix("models/") for m in client.models.list()}
        return [m for m in MODELS if m in names] or MODELS
    except Exception as e:
        print(f"⚠️ دریافت لیست مدل‌ها ناموفق بود ({e})؛ از لیست پیش‌فرض استفاده می‌شود")
        return MODELS


def _load_json(resp):
    raw = (resp.text or "").strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw).strip()
    return json.loads(raw)


def _parse_selected(resp):
    return _load_json(resp).get("selected_news", [])


def _parse_digest(resp):
    data = _load_json(resp)
    items = [str(i).strip() for i in data.get("items", []) if str(i).strip()]
    question = str(data.get("question", "")).strip()
    if len(items) >= 2 and question:
        return {"items": items, "question": question, "hashtags": data.get("hashtags", [])}
    return None


def _make_config(low_thinking, schema=AnalyzedNewsBatch):
    cfg = dict(
        response_mime_type="application/json",
        response_schema=schema,
        temperature=0.2,
    )
    if low_thinking:
        # تفکر سبک = پاسخ سریع‌تر و تایم‌اوت کمتر؛ برای انتخاب خبر کافی است
        cfg["thinking_config"] = types.ThinkingConfig(thinking_level=types.ThinkingLevel.LOW)
    return types.GenerateContentConfig(**cfg)


def _try_model(client, model, prompt, attempts=2, base=2, cap=30, schema=AnalyzedNewsBatch, parse=None):
    """روی یک مدل با exponential backoff + jitter تلاش می‌کند. شکست = None."""
    parse = parse or _parse_selected
    low_thinking = True
    i = 0
    while i < attempts:
        i += 1
        try:
            resp = client.models.generate_content(
                model=model,
                contents=prompt,
                config=_make_config(low_thinking, schema),
            )
            selected = parse(resp)
            if selected:
                return selected
            print(f"⚠️ {model}: خروجی خالی (تلاش {i}/{attempts})")
        except errors.APIError as e:
            if e.code == 400 and low_thinking:
                # بعضی مدل‌ها thinking_level را قبول نمی‌کنند؛ همان تلاش را بدون آن تکرار کن
                print(f"⚠️ {model}: خطای 400 با thinking_level؛ تلاش مجدد بدون آن")
                low_thinking = False
                i -= 1
                continue
            if e.code in SKIP_MODEL_CODES:
                print(f"⛔ {model}: خطای {e.code} (سهمیه/تایم‌اوت گوگل) → مدل بعدی")
                return None
            if e.code not in RETRYABLE:
                # 404 / 403 ...: تکرار فایده ندارد، برو مدل بعدی
                print(f"⛔ {model}: خطای {e.code} (غیرقابل تلاش مجدد) → مدل بعدی")
                return None
            print(f"⚠️ {model} تلاش {i}/{attempts}: خطای {e.code}")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"⚠️ {model}: خروجی نامعتبر ({e})")
        except Exception as e:
            # تایم‌اوت سمت کلاینت، قطعی شبکه و ...: وقت را نسوزان، برو مدل بعدی
            print(f"⛔ {model}: {type(e).__name__} (تایم‌اوت/شبکه) → مدل بعدی")
            return None

        if i < attempts:
            delay = min(cap, base * 2 ** (i - 1)) + random.uniform(0, 1)
            print(f"⏳ {delay:.1f} ثانیه صبر...")
            time.sleep(delay)
    return None


def build_prompt(articles):
    news_text = "".join(
        f"[{i}] منبع: {art['source']} | دسته‌بندی: {art['category']} | "
        f"پوشش: {art.get('coverage', 1)} منبع | "
        f"تیتر: {art['title']}\nمتن: {art['summary']}\n---\n"
        for i, art in enumerate(articles)
    )
    return PROMPT_TEMPLATE.format(news_text=news_text)


def _make_client():
    return genai.Client(
        api_key=GEMINI_KEY,
        http_options=types.HttpOptions(
            timeout=50_000,  # میلی‌ثانیه؛ بیشتر از این صبر نمی‌کنیم
            # ریتری داخلی SDK خاموش؛ ریتری را خودمان کنترل می‌کنیم تا سهمیه دوبرابر نسوزد
            retry_options=types.HttpRetryOptions(attempts=1),
        ),
    )


def select_and_analyze(articles):
    if not articles:
        return []

    client = _make_client()
    prompt = build_prompt(articles)
    models = _available_models(client)

    for round_no in (1, 2):
        for model in models:
            print(f"🧠 پردازش با {model}...")
            STATS["models_tried"] += 1
            result = _try_model(client, model, prompt)
            if result:
                STATS["model"] = model
                return result
        if round_no == 1:
            print("🔁 همه مدل‌ها ناموفق بودند؛ ۲۰ ثانیه صبر و دور دوم...")
            time.sleep(20)

    print("❌ هیچ‌کدام از مدل‌ها نتیجه نداد")
    return []


# ══════════════════════════════════════════════════════════════
# ۷) ایتا: قالب‌بندی و ارسال
# ══════════════════════════════════════════════════════════════

def _format_tags(raw_tags, default):
    clean_tags = [f"#{t.strip().lstrip('#').replace(' ', '_')}" for t in raw_tags[:3] if t.strip()]
    return " ".join(clean_tags) if clean_tags else default


def format_post(item, source_name):
    title = item.get("title", "").replace("*", "").strip()
    body = item.get("body", "").replace("*", "").strip()
    hashtags_str = _format_tags(item.get("hashtags", []), "#خبر #فناوری")

    # بدون ستاره و فرمت مارک‌داون، چون در ایتا خراب نمایش داده می‌شود
    return f"📌 {title}\n\n{body}\n\n🏷 {hashtags_str}\n📡 منبع: {source_name}\n🆔 @{CHANNEL}"


def _eitaa_request(method, data, files=None, timeout=10):
    """یک فراخوانی به API ایتایار؛ True فقط اگر پاسخ ok باشد."""
    url = f"https://eitaayar.ir/api/{EITAA_TOKEN}/{method}"
    try:
        resp = requests.post(url, data=data, files=files, timeout=timeout)
        if resp.status_code != 200:
            print(f"⚠️ خطای HTTP از ایتاپیر ({method}): {resp.status_code}")
            return False
        res_json = resp.json()
        if res_json.get("ok") is True:
            return True
        print(f"⚠️ خطای ایتاپیر در پاسخ ({method}): {res_json}")
    except Exception as e:
        print(f"⚠️ خطا در ارتباط با ایتا ({method}): {e}")
    return False


def send_to_eitaa(text):
    return _eitaa_request("sendMessage", {"chat_id": CHANNEL, "text": text})


def send_photo_to_eitaa(image_bytes, mime, caption):
    files = {"file": ("news.jpg", image_bytes, mime)}
    return _eitaa_request(EITAA_PHOTO_METHOD, {"chat_id": CHANNEL, "caption": caption}, files=files, timeout=20)


def download_image(url):
    """دانلود عکس؛ اگر چیزی نبود، عکس نبود یا خیلی بزرگ بود None برمی‌گرداند."""
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=8)
        mime = resp.headers.get("Content-Type", "").split(";")[0].strip()
        if resp.status_code != 200 or not mime.startswith("image/"):
            return None
        if not resp.content or len(resp.content) > MAX_IMAGE_BYTES:
            return None
        return resp.content, mime
    except Exception as e:
        print(f"⚠️ دانلود عکس ناموفق ({e})")
        return None


def publish_post(post_text, image_url):
    """اول عکس با کپشن؛ اگر نشد یا کپشن جا نشد، متن. برمی‌گرداند آیا پست منتشر شد."""
    if image_url:
        img = download_image(image_url)
        if img:
            if len(post_text) <= CAPTION_LIMIT:
                if send_photo_to_eitaa(img[0], img[1], post_text):
                    return True
            elif send_photo_to_eitaa(img[0], img[1], ""):
                return send_to_eitaa(post_text)
        STATS["photo_failed"] += 1  # عکس نرسید؛ پست به‌صورت متن می‌رود
    return send_to_eitaa(post_text)


# ══════════════════════════════════════════════════════════════
# ۸) سابقه پست‌ها و جمع‌بندی روزانه
# ══════════════════════════════════════════════════════════════

def now_utc():
    return datetime.now(timezone.utc)


def _parse_ts(value):
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def load_log():
    try:
        data = json.loads(LOG_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("posts"), list):
            return data
    except Exception:
        pass
    return {"posts": [], "last_digest": None}


def save_log(log):
    cutoff = now_utc() - timedelta(days=LOG_KEEP_DAYS)
    log["posts"] = [p for p in log["posts"] if (_parse_ts(p.get("ts")) or cutoff) >= cutoff]
    LOG_FILE.write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")


def record_published(entries):
    """خبرهای تکنولوژی و هوش مصنوعیِ منتشرشده را برای جمع‌بندی روزانه نگه می‌دارد."""
    if not entries:
        return
    log = load_log()
    stamp = now_utc().isoformat()
    for art, item in entries:
        if art.get("group") == "tech":
            log["posts"].append({
                "ts": stamp,
                "title": item.get("title", ""),
                "body": item.get("body", ""),
                "source": art.get("source", ""),
                "link": art.get("link", ""),
                "digested": False,
            })
    save_log(log)


def build_digest_prompt(posts):
    posts_text = "".join(
        f"[{i}] {p.get('title', '')}\n{p.get('body', '')[:300]}\n---\n"
        for i, p in enumerate(posts[-12:], start=1)
    )
    return DIGEST_PROMPT_TEMPLATE.format(posts_text=posts_text)


def generate_digest(posts):
    client = _make_client()
    prompt = build_digest_prompt(posts)
    for model in DIGEST_MODELS:
        print(f"🧠 جمع‌بندی با {model}...")
        result = _try_model(client, model, prompt, schema=DailyDigest, parse=_parse_digest)
        if result:
            return result
    return None


def format_digest(digest):
    clean = lambda text: str(text).replace("*", "").strip()
    items = [clean(i) for i in digest["items"] if clean(i)][:6]
    tags = _format_tags(digest.get("hashtags", []), "#هوش_مصنوعی #تکنولوژی")
    lines = ["📋 جمع‌بندی امروز تکنولوژی و هوش مصنوعی", ""]
    lines += [f"▫️ {i}" for i in items]
    lines += ["", "💬 سؤال امروز:", clean(digest["question"]), "", f"🏷 {tags}", f"🆔 @{CHANNEL}"]
    return "\n".join(lines)


def maybe_send_digest():
    """روزی یک‌بار، شب‌ها: جمع‌بندی خبرهای تکنولوژی که هنوز در جمع‌بندی نیامده‌اند.
    فقط یک درخواست به Gemini می‌زند و هیچ ران جدیدی در گیت‌هاب نمی‌سازد."""
    if not DIGEST_ENABLED:
        return
    now = now_utc()
    hour = now.astimezone(IRAN_TZ).hour
    if not (hour >= DIGEST_START_HOUR or hour < DIGEST_END_HOUR):
        return

    log = load_log()
    last = _parse_ts(log.get("last_digest"))
    if last and now - last < timedelta(hours=DIGEST_MIN_GAP_HOURS):
        return
    pending = [p for p in log["posts"] if not p.get("digested")]
    if len(pending) < DIGEST_MIN_POSTS:
        print(f"📋 جمع‌بندی: فقط {len(pending)} خبر جدید دارم (حداقل {DIGEST_MIN_POSTS})؛ امشب رد شد.")
        return

    digest = generate_digest(pending)
    if digest and send_to_eitaa(format_digest(digest)):
        for p in pending:
            p["digested"] = True
        log["last_digest"] = now.isoformat()
        save_log(log)
        STATS["digest"] = "sent"
        print("📋 جمع‌بندی روزانه ارسال شد.")
    else:
        STATS["digest"] = "failed"
        print("⚠️ جمع‌بندی ساخته یا ارسال نشد؛ ران بعدی دوباره تلاش می‌کند.")


# ══════════════════════════════════════════════════════════════
# ۹) گزارش و هشدار خطا
# ══════════════════════════════════════════════════════════════

def send_alert(text):
    if not ALERT_CHAT:
        return False
    return _eitaa_request("sendMessage", {"chat_id": ALERT_CHAT, "text": text})


def send_report(exit_code):
    """اگر مشکلی بود (یا ALERT_MODE=always) یک گزارش کوتاه به ALERT_CHAT می‌فرستد."""
    if not ALERT_CHAT:
        return
    problems = []
    if exit_code != 0:
        problems.append("ران ناموفق بود: هوش مصنوعی نتیجه نداد")
    if STATS["posts_planned"] and STATS["posts_sent"] < STATS["posts_planned"]:
        problems.append(f"فقط {STATS['posts_sent']} از {STATS['posts_planned']} پست ارسال شد")
    if len(STATS["feed_failed"]) >= FEED_FAIL_ALERT:
        problems.append("چند فید خراب بود")
    if STATS["photo_failed"]:
        problems.append(f"{STATS['photo_failed']} عکس ارسال نشد و پست به‌صورت متن رفت")
    if STATS["digest"] == "failed":
        problems.append("جمع‌بندی روزانه ساخته یا ارسال نشد")

    if not problems and ALERT_MODE != "always":
        return

    lines = ["⚠️ گزارش ربات عدل نیوز" if problems else "✅ گزارش ربات عدل نیوز"]
    lines += [f"• {p}" for p in problems] or ["• همه‌چیز عادی بود"]
    lines.append(f"📨 پست‌ها: {STATS['posts_sent']} از {STATS['posts_planned']}")
    if STATS["model"]:
        lines.append(f"🧠 مدل: {STATS['model']} (تعداد مدل امتحان‌شده: {STATS['models_tried']})")
    if STATS["feed_failed"]:
        lines.append("📡 فیدهای خراب: " + "، ".join(sorted(set(STATS["feed_failed"]))))
    if STATS["digest"]:
        lines.append("📋 جمع‌بندی: " + ("ارسال شد" if STATS["digest"] == "sent" else "ناموفق"))
    send_alert("\n".join(lines))


# ══════════════════════════════════════════════════════════════
# ۱۰) اجرای اصلی
# ══════════════════════════════════════════════════════════════

def run_news():
    seen_list = load_seen()
    all_new_articles = fetch_new_articles(seen_list)

    if not all_new_articles:
        print("📭 خبر جدیدی یافت نشد.")
        return 0

    batch_articles = balance_articles(all_new_articles)
    enrich_with_fulltext(batch_articles)
    selected_items = select_and_analyze(batch_articles)

    if not selected_items:
        print("⚠️ خبری توسط هوش مصنوعی انتخاب نشد.")
        # کد خروج 1 => ران گیت‌هاب اکشنز قرمز می‌شود و می‌توانی دوباره اجرایش کنی
        return 1

    sent_links = []
    published = []
    used_indexes = set()

    for item in selected_items:
        if len(used_indexes) >= POSTS_COUNT:
            break
        idx = item.get("index")
        # شماره نامعتبر یا تکراری (اگر مدل یک خبر را دو بار برگرداند) رد می‌شود و سهمیه پست را نمی‌خورد
        if idx is None or not 0 <= idx < len(batch_articles) or idx in used_indexes:
            continue
        used_indexes.add(idx)

        art = batch_articles[idx]
        image_url = art.get("image") if art.get("allow_image") else None
        print(f"🖼 خبر {idx} ({art['source']}): {'با عکس' if image_url else 'بدون عکس'}")

        if publish_post(format_post(item, art["source"]), image_url):
            STATS["posts_sent"] += 1
            sent_links.extend(art.get("links") or [art["link"]])
            published.append((art, item))
            print(f"✅ خبر شماره {idx} با موفقیت به ایتا ارسال شد.")
            time.sleep(2)
        else:
            print(f"❌ ارسال خبر شماره {idx} ناموفق بود.")
    STATS["posts_planned"] = len(used_indexes)

    # سابقه فقط برای خبرهای ارسال‌شده ذخیره می‌شود (با لینک همه نسخه‌های ادغام‌شده)
    if sent_links:
        save_seen(seen_list + sent_links)
        record_published(published)
        print(f"✅ مجموعاً {STATS['posts_sent']} پست با موفقیت منتشر و سابقه ذخیره شد.")
    else:
        print("⚠️ هیچ پستی با موفقیت ارسال نشد، سابقه آپدیت نشد.")

    return 0


def main():
    print("🚀 شروع اجرای ربات...")
    _reset_stats()
    try:
        code = run_news()
        try:
            maybe_send_digest()
        except Exception as e:  # خطای جمع‌بندی نباید ران خبر را خراب کند
            STATS["digest"] = "failed"
            print(f"⚠️ خطا در جمع‌بندی: {type(e).__name__}: {e}")
    except Exception as e:
        send_alert(f"❌ ربات عدل نیوز با خطا متوقف شد:\n{type(e).__name__}: {e}")
        raise
    send_report(code)
    return code


if __name__ == "__main__":
    sys.exit(main())