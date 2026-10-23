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

# لیست منابع فید RSS استاندارد و پرسرعت
RSS_FEEDS = [
    {
        "name": "تک‌کرانچ (هوش مصنوعی)",
        "url": "https://techcrunch.com/category/artificial-intelligence/feed/",
        "category": "تکنولوژی و هوش مصنوعی",
    },
    {
        "name": "دیجیاتو",
        "url": "https://digiato.com/feed",
        "category": "تکنولوژی و فناوری",
    },
    {
        "name": "ورزش سه",
        "url": "https://www.varzesh3.com/rss",
        "category": "ورزش",
    },
    {
        "name": "الجزیره (بین‌الملل و جنگ)",
        "url": "https://www.aljazeera.com/xml/rss/all.xml",
        "category": "بین‌الملل و سیاست",
    },
    {
        "name": "خبرگزاری فارس",
        "url": "https://www.farsnews.ir/rss",
        "category": "سیاست و اخبار عمومی",
    },
]


class AnalyzedNewsItem(BaseModel):
    index: int = Field(description="شماره ردیف خبر انتخاب شده")
    title: str = Field(description="تیتر جذاب و دقیق به فارسی")
    summary: str = Field(description="خلاصه در ۲ تا ۳ جمله کوتاه")
    why_important: str = Field(description="تحلیل کوتاه در ۱ یا ۲ جمله")
    hashtags: list[str] = Field(description="دقیقا ۳ هشتگ مرتبط به فارسی")


class AnalyzedNewsBatch(BaseModel):
    selected_news: list[AnalyzedNewsItem] = Field(description="لیست ۳ خبر برتر با تفکیک موضوعی")


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
    try:
        print(f"⏳ دریافت هم‌زمان: {feed_info['name']}...")
        try:
            resp = requests.get(feed_info["url"], headers=headers, timeout=6)
        except requests.exceptions.SSLError:
            # مدیریت خطای گواهی SSL برای خبرگزاری‌های داخلی
            resp = requests.get(feed_info["url"], headers=headers, timeout=6, verify=False)

        if resp.status_code != 200:
            return []
        
        feed = feedparser.parse(resp.content)
        for entry in feed.entries[:4]:
            raw_link = entry.get("link", "").strip()
            norm_link = normalize_link(raw_link)
            title = clean_html(entry.get("title", ""))
            summary = clean_html(entry.get("summary", entry.get("description", "")))

            if norm_link and title and norm_link not in seen_set:
                feed_articles.append(
                    {
                        "source": feed_info["name"],
                        "category": feed_info["category"],
                        "title": title,
                        "summary": summary[:350] if summary else title,
                        "link": norm_link,
                    }
                )
    except Exception as e:
        print(f"⚡ عبور سریع از {feed_info['name']} به دلیل خطا یا کندی: {e}")
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


# کدهایی که ارزش تلاش مجدد دارند (overload / rate limit / خطای موقت سرور)
RETRYABLE = {408, 429, 500, 502, 503, 504}

# به ترتیب اولویت؛ مدل‌هایی که در API وجود ندارند خودکار حذف می‌شوند
MODELS = [
    "gemini-3.8-flash",
    "gemini-3.7-flash",
    "gemini-3.6-flash",
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


def _try_model(client, model, prompt, attempts=3, base=2, cap=30):
    """روی یک مدل با exponential backoff + jitter تلاش می‌کند. شکست = None."""
    for i in range(1, attempts + 1):
        try:
            resp = client.models.generate_content(
                model=model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=AnalyzedNewsBatch,
                    temperature=0.2,
                ),
            )
            selected = _parse_selected(resp)
            if selected:
                return selected
            print(f"⚠️ {model}: خروجی خالی (تلاش {i}/{attempts})")
        except errors.APIError as e:
            if e.code not in RETRYABLE:
                # 404 / 400 / 403 ...: تکرار فایده ندارد، برو مدل بعدی
                print(f"⛔ {model}: خطای {e.code} (غیرقابل تلاش مجدد) → مدل بعدی")
                return None
            print(f"⚠️ {model} تلاش {i}/{attempts}: خطای {e.code}")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"⚠️ {model}: خروجی نامعتبر ({e})")

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
            timeout=60_000,  # میلی‌ثانیه
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

    prompt = f"""نقش: سردبیر ارشد «عدل نیوز». شما باید دقیقاً ۳ خبر برتر را از بین اخبار زیر انتخاب کنید، به طوری که تفکیک موضوعی زیر به دقت رعایت شود:
    1. یک خبر حتماً درباره «تکنولوژی و هوش مصنوعی (AI)» باشد.
    2. یک خبر حتماً درباره «ورزش» باشد.
    3. یک خبر حتماً درباره «جنگ، مسائل بین‌الملل یا سیاست» باشد.

    قوانین سخت‌گیرانه:
    1. بدون حدس و اطلاعات خارج از متن.
    2. عدم استفاده از ستاره (*) یا تگ HTML در متون.
    3. لحن کوتاه و حرفه‌ای ژورنالیستی.

    اخبار ورودی:
    {news_text}
    """

    for model in _available_models(client):
        print(f"🧠 پردازش با {model}...")
        result = _try_model(client, model, prompt)
        if result:
            return result

    print("❌ هیچ‌کدام از مدل‌ها نتیجه نداد")
    return []


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

    batch_articles = all_new_articles[:25]
    selected_items = select_and_analyze(batch_articles)

    if not selected_items:
        print("⚠️ خبری توسط هوش مصنوعی انتخاب نشد.")
        # کد خروج 1 => ران گیت‌هاب اکشنز قرمز می‌شود و می‌توانی دوباره اجرایش کنی
        return 1

    sent_count = 0
    successfully_sent_links = []

    for item in selected_items[:3]:
        idx = item.get("index")
        if idx is not None and 0 <= idx < len(batch_articles):
            art = batch_articles[idx]
            post_text = format_post(item, art["source"])
            if send_to_eitaa(post_text):
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
