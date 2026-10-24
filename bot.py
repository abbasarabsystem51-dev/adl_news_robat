import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import feedparser
import requests
from google import genai
from google.genai import errors, types
from pydantic import BaseModel, Field

# غیرفعال کردن هشدارهای SSL برای جلوگیری از خطای گواهی در سایت‌های داخلی
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = Path(__file__).resolve().parent
SEEN_FILE = BASE_DIR / "seen.json"

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

# لیست منابع فید RSS؛ هر فید یک "group" دارد تا انتخاب خبرها متعادل باشد
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

# سهم هر دسته از ورودی مدل (مجموعاً ۲۵ خبر) تا هیچ دسته‌ای از قلم نیفتد
GROUP_QUOTAS = {"tech": 7, "science": 5, "sport": 6, "politics": 7}
POSTS_COUNT = 4

# دقیقاً این تعداد از پست‌ها عکس‌دار می‌شوند. دسته‌هایی که عکس دارند به ترتیب اولویت انتخاب می‌شوند.
IMAGE_GROUPS_COUNT = 2
IMAGE_PRIORITY = ["tech", "sport", "politics", "science"]


class AnalyzedNewsItem(BaseModel):
    index: int = Field(description="شماره ردیف خبر انتخاب شده")
    title: str = Field(description="تیتر جذاب و دقیق به فارسی")
    summary: str = Field(description="خلاصه در ۲ تا ۳ جمله کوتاه")
    why_important: str = Field(description="تحلیل کوتاه در ۱ یا ۲ جمله")
    hashtags: list[str] = Field(description="دقیقا ۳ هشتگ مرتبط به فارسی")


class AnalyzedNewsBatch(BaseModel):
    selected_news: list[AnalyzedNewsItem] = Field(description="لیست ۴ خبر برتر با تفکیک موضوعی")


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
    # نگهداری ۵۰۰ خبر آخر جهت جلوگیری از حجیم شدن فایل
    with SEEN_FILE.open("w", encoding="utf-8") as f:
        json.dump(unique_seen[-500:], f, ensure_ascii=False, indent=2)


def clean_html(raw_html):
    text = re.sub(r"<[^>]+>", " ", raw_html or "")
    return re.sub(r"\s+", " ", text).strip()


def fetch_single_feed(feed_info, seen_set):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    feed_articles = []
    name = feed_info["name"]
    try:
        try:
            resp = requests.get(feed_info["url"], headers=headers, timeout=6)
        except requests.exceptions.SSLError:
            # مدیریت خطای گواهی SSL برای خبرگزاری‌های داخلی
            resp = requests.get(feed_info["url"], headers=headers, timeout=6, verify=False)

        if resp.status_code != 200:
            print(f"❌ {name}: HTTP {resp.status_code}")
            return []

        feed = feedparser.parse(resp.content)
        entries = feed.entries[:4]
        for entry in entries:
            raw_link = entry.get("link", "").strip()
            norm_link = normalize_link(raw_link)
            title = clean_html(entry.get("title", ""))
            summary = clean_html(entry.get("summary", entry.get("description", "")))

            if norm_link and title and norm_link not in seen_set:
                feed_articles.append(
                    {
                        "source": name,
                        "category": feed_info["category"],
                        "group": feed_info["group"],
                        "title": title,
                        "summary": summary[:350] if summary else title,
                        "link": norm_link,
                        "image": extract_image_url(entry),
                    }
                )
        if not entries:
            print(f"⚠️ {name}: فید خالی یا نامعتبر")
        else:
            print(f"✅ {name}: {len(feed_articles)} خبر جدید از {len(entries)}")
    except Exception as e:
        print(f"⚡ عبور سریع از {name} به دلیل خطا یا کندی: {e}")
    return feed_articles


def fetch_new_articles(seen_list):
    seen_set = set(seen_list)
    articles = []
    batch_links = set()

    # دریافت موازی تمامی فیدها جهت سرعت فوق‌العاده
    with ThreadPoolExecutor(max_workers=len(RSS_FEEDS)) as executor:
        futures = {executor.submit(fetch_single_feed, feed, seen_set): feed for feed in RSS_FEEDS}
        for future in as_completed(futures):
            res = future.result()
            for art in res:
                if art["link"] not in batch_links:
                    batch_links.add(art["link"])
                    articles.append(art)

    return articles


def balance_articles(articles):
    """از هر دسته به‌اندازه سهمش خبر برمی‌دارد. در دقیقاً IMAGE_GROUPS_COUNT دسته فقط خبرهای
    دارای عکس به مدل داده می‌شوند و اجازه ارسال عکس (allow_image) فقط برای همان‌هاست."""
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
        is_img = group in image_groups
        queues = list(by_group.get(group, {}).values())
        if is_img:
            queues = [[a for a in q if a.get("image")] for q in queues]
        picked = []
        while len(picked) < quota and any(queues):
            for q in queues:
                if q and len(picked) < quota:
                    picked.append(q.pop(0))
        for art in picked:
            art["allow_image"] = is_img
        print(f"📊 دسته {group}: {len(picked)} خبر برای تحلیل" + (" (فقط عکس‌دار)" if is_img else ""))
        result.extend(picked)
    return result


# کدهایی که ارزش تلاش مجدد روی همان مدل دارند (خطای موقت و سریع‌برگشت مثل 503).
# این کدها فوراً می‌روند سراغ مدل بعدی (تکرار روی همان مدل فایده ندارد و وقت می‌سوزاند):
#   429 = پر شدن سهمیه (هر مدل سهمیه جدا دارد)
#   504 = تایم‌اوت سمت گوگل؛ دوباره همان درخواست سنگین هم احتمالاً تایم‌اوت می‌خورد
RETRYABLE = {408, 500, 502, 503}
SKIP_MODEL_CODES = {429, 504}

# به ترتیب اولویت؛ مدل‌هایی که در API وجود ندارند خودکار حذف می‌شوند
MODELS = [
    "gemini-3.6-flash",   # در چند ران پیاپی از اولین تلاش جواب داده (سریع و پایدار)
    "gemini-3.8-flash",
    "gemini-3.7-flash",
    "gemini-3.5-flash",
    "gemini-3.5-flash-lite",
]


def _available_models(client):
    """فقط مدل‌هایی که واقعاً در API هستند، تا 404 وقت تلف نکند."""
    try:
        names = {m.name.removeprefix("models/") for m in client.models.list()}
        ok = [m for m in MODELS if m in names]
        return ok or MODELS
    except Exception as e:
        print(f"⚠️ دریافت لیست مدل‌ها ناموفق بود ({e})؛ از لیست پیش‌فرض استفاده می‌شود")
        return MODELS


def _parse_selected(resp):
    raw = (resp.text or "").strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw).strip()
    return json.loads(raw).get("selected_news", [])


def _make_config(low_thinking):
    cfg = dict(
        response_mime_type="application/json",
        response_schema=AnalyzedNewsBatch,
        temperature=0.2,
    )
    if low_thinking:
        # تفکر سبک = پاسخ سریع‌تر و تایم‌اوت کمتر؛ برای انتخاب خبر کافی است
        cfg["thinking_config"] = types.ThinkingConfig(thinking_level=types.ThinkingLevel.LOW)
    return types.GenerateContentConfig(**cfg)


def _try_model(client, model, prompt, attempts=2, base=2, cap=30):
    """روی یک مدل با exponential backoff + jitter تلاش می‌کند. شکست = None."""
    low_thinking = True
    i = 0
    while i < attempts:
        i += 1
        try:
            resp = client.models.generate_content(
                model=model,
                contents=prompt,
                config=_make_config(low_thinking),
            )
            selected = _parse_selected(resp)
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


def select_and_analyze(articles):
    if not articles:
        return []

    client = genai.Client(
        api_key=GEMINI_KEY,
        http_options=types.HttpOptions(
            timeout=50_000,  # میلی‌ثانیه؛ بیشتر از این صبر نمی‌کنیم
            # ریتری داخلی SDK خاموش؛ ریتری را خودمان کنترل می‌کنیم تا سهمیه دوبرابر نسوزد
            retry_options=types.HttpRetryOptions(attempts=1),
        ),
    )

    news_text = ""
    for i, art in enumerate(articles):
        news_text += (
            f"[{i}] منبع: {art['source']} | دسته‌بندی: {art['category']} | "
            f"تیتر: {art['title']}\nمتن: {art['summary']}\n---\n"
        )

    prompt = f"""نقش: سردبیر ارشد «عدل نیوز». شما باید دقیقاً ۴ خبر برتر را از بین اخبار زیر انتخاب کنید، به طوری که تفکیک موضوعی زیر به دقت رعایت شود:
    1. یک خبر حتماً درباره «تکنولوژی و هوش مصنوعی (AI)» باشد.
    2. یک خبر حتماً «علمی» باشد.
    3. یک خبر حتماً درباره «ورزش» باشد.
    4. یک خبر حتماً درباره «جنگ، مسائل بین‌الملل یا سیاست» باشد.

    قوانین سخت‌گیرانه:
    1. بدون حدس و اطلاعات خارج از متن.
    2. عدم استفاده از ستاره (*) یا تگ HTML در متون.
    3. لحن کوتاه و حرفه‌ای ژورنالیستی.

    اخبار ورودی:
    {news_text}
    """

    models = _available_models(client)
    for round_no in (1, 2):
        for model in models:
            print(f"🧠 پردازش با {model}...")
            result = _try_model(client, model, prompt)
            if result:
                return result
        if round_no == 1:
            print("🔁 همه مدل‌ها ناموفق بودند؛ ۲۰ ثانیه صبر و دور دوم...")
            time.sleep(20)

    print("❌ هیچ‌کدام از مدل‌ها نتیجه نداد")
    return []


EITAA_PHOTO_METHOD = "sendFile"  # نام متد ارسال فایل/عکس در API ایتایار؛ اگر متفاوت بود اینجا عوض کن
MAX_IMAGE_BYTES = 5_000_000
CAPTION_LIMIT = 1000


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


def download_image(url):
    """دانلود عکس؛ اگر چیزی نبود یا خیلی بزرگ بود None برمی‌گرداند."""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        resp = requests.get(url, headers=headers, timeout=8)
        mime = resp.headers.get("Content-Type", "").split(";")[0].strip()
        if resp.status_code != 200 or not mime.startswith("image/"):
            return None
        if not resp.content or len(resp.content) > MAX_IMAGE_BYTES:
            return None
        return resp.content, mime
    except Exception as e:
        print(f"⚠️ دانلود عکس ناموفق ({e})")
        return None


def send_photo_to_eitaa(image_bytes, mime, caption):
    url = f"https://eitaayar.ir/api/{EITAA_TOKEN}/{EITAA_PHOTO_METHOD}"
    files = {"file": ("news.jpg", image_bytes, mime)}
    data = {"chat_id": CHANNEL, "caption": caption}
    try:
        resp = requests.post(url, data=data, files=files, timeout=20)
        if resp.status_code == 200 and resp.json().get("ok") is True:
            return True
        print(f"⚠️ ارسال عکس ناموفق: HTTP {resp.status_code}")
    except Exception as e:
        print(f"⚠️ خطا در ارسال عکس: {e}")
    return False


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
    return send_to_eitaa(post_text)


def format_post(item, source_name):
    title = item.get("title", "").replace("*", "").strip()
    summary = item.get("summary", "").replace("*", "").strip()
    why_imp = item.get("why_important", "").replace("*", "").strip()
    raw_tags = item.get("hashtags", [])

    clean_tags = [f"#{t.strip().lstrip('#').replace(' ', '_')}" for t in raw_tags[:3] if t.strip()]
    hashtags_str = " ".join(clean_tags) if clean_tags else "#خبر #فناوری"

    # ساختار متن بدون هیچ‌گونه ستاره یا فرمت مارک‌داون مخرب برای ایتا
    return f"📌 {title}\n\n📝 خلاصه:\n{summary}\n\n💡 چرا مهم است؟\n{why_imp}\n\n🏷 {hashtags_str}\n📡 منبع: {source_name}\n🆔 @{CHANNEL}"


def send_to_eitaa(text):
    url = f"https://eitaayar.ir/api/{EITAA_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, data={"chat_id": CHANNEL, "text": text}, timeout=10)
        if resp.status_code == 200:
            res_json = resp.json()
            if res_json.get("ok") is True:
                return True
            else:
                print(f"⚠️ خطای ایتاپیر در پاسخ: {res_json}")
        else:
            print(f"⚠️ خطای HTTP از ایتاپیر: {resp.status_code}")
        return False
    except Exception as e:
        print(f"⚠️ خطا در ارتباط با ایتا: {e}")
        return False


def main():
    print("🚀 شروع اجرای فوق‌سریع و هوشمند ربات...")
    seen_list = load_seen()
    all_new_articles = fetch_new_articles(seen_list)

    if not all_new_articles:
        print("📭 خبر جدیدی یافت نشد.")
        return 0

    batch_articles = balance_articles(all_new_articles)
    selected_items = select_and_analyze(batch_articles)

    if not selected_items:
        print("⚠️ خبری توسط هوش مصنوعی انتخاب نشد.")
        # کد خروج 1 => ران گیت‌هاب اکشنز قرمز می‌شود و می‌توانی دوباره اجرایش کنی
        return 1

    sent_count = 0
    successfully_sent_links = []

    for item in selected_items[:POSTS_COUNT]:
        idx = item.get("index")
        if idx is not None and 0 <= idx < len(batch_articles):
            art = batch_articles[idx]
            post_text = format_post(item, art["source"])
            print(f"🖼 خبر {idx} ({art['source']}): {'با عکس' if art.get('allow_image') and art.get('image') else 'بدون عکس'}")
            if publish_post(post_text, art.get("image") if art.get("allow_image") else None):
                sent_count += 1
                successfully_sent_links.append(art["link"])
                print(f"✅ خبر شماره {idx} با موفقیت به ایتا ارسال شد.")
                time.sleep(2)
            else:
                print(f"❌ ارسال خبر شماره {idx} ناموفق بود.")

    # ذخیره سابقه فقط برای اخبار ارسال‌شده (جهت جلوگیری از گم شدن اخبار در صورت خطای موقت ایتا)
    if successfully_sent_links:
        seen_list.extend(successfully_sent_links)
        save_seen(seen_list)
        print(f"✅ مجموعاً {sent_count} پست با موفقیت منتشر و سابقه ذخیره شد.")
    else:
        print("⚠️ هیچ پستی با موفقیت ارسال نشد، سابقه آپدیت نشد.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
