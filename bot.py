import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import feedparser
import requests
from google import genai
from google.genai import types
from pydantic import BaseModel, Field

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

# لیست منابع فید RSS (زومیت حذف و فارس جایگزین شد)
RSS_FEEDS = [
    {
        "name": "تک‌کرانچ (هوش مصنوعی)",
        "url": "https://techcrunch.com/category/artificial-intelligence/feed/",
        "category": "تکنولوژی و هوش مصنوعی جهانی",
    },
    {
        "name": "خبرگزاری فارس",
        "url": "https://www.farsnews.ir/rss",
        "category": "اخبار و فناوری",
    },
    {
        "name": "دیجیاتو",
        "url": "https://digiato.com/feed",
        "category": "تکنولوژی و فناوری",
    },
    {
        "name": "الجزیره (بین‌الملل)",
        "url": "https://www.aljazeera.com/xml/rss/all.xml",
        "category": "اخبار جهانی و تحولات سیاسی/جنگ",
    },
    {
        "name": "خبرگزاری مهر (فناوری)",
        "url": "https://www.mehrnews.com/rss/tp/5",
        "category": "علم و فناوری",
    },
]


class AnalyzedNewsItem(BaseModel):
    index: int = Field(description="شماره ردیف خبر انتخاب شده")
    title: str = Field(description="تیتر جذاب و دقیق به فارسی")
    summary: str = Field(description="خلاصه در ۲ تا ۳ جمله کوتاه")
    why_important: str = Field(description="تحلیل کوتاه در ۱ یا ۲ جمله")
    hashtags: list[str] = Field(description="دقیقا ۳ هشتگ مرتبط به فارسی")


class AnalyzedNewsBatch(BaseModel):
    selected_news: list[AnalyzedNewsItem] = Field(description="لیست ۱ تا ۳ خبر برتر")


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
        json.dump(unique_seen[-500:], f, ensure_ascii=False, indent=2)


def clean_html(raw_html):
    text = re.sub(r"<[^>]+>", " ", raw_html or "")
    return re.sub(r"\s+", " ", text).strip()


def fetch_single_feed(feed_info, seen_set):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    feed_articles = []
    try:
        print(f"⏳ دریافت هم‌زمان: {feed_info['name']}...")
        resp = requests.get(feed_info["url"], headers=headers, timeout=5)
        if resp.status_code != 200:
            return []
        
        feed = feedparser.parse(resp.content)
        for entry in feed.entries[:3]:
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

    with ThreadPoolExecutor(max_workers=len(RSS_FEEDS)) as executor:
        futures = {executor.submit(fetch_single_feed, feed, seen_set): feed for feed in RSS_FEEDS}
        for future in as_completed(futures):
            res = future.result()
            for art in res:
                if art["link"] not in batch_links:
                    batch_links.add(art["link"])
                    articles.append(art)

    return articles


def select_and_analyze(articles):
    if not articles:
        return []

    client = genai.Client(api_key=GEMINI_KEY)
    news_text = ""

    for i, art in enumerate(articles):
        news_text += f"[{i}] منبع: {art['source']} | تیتر: {art['title']}\nمتن: {art['summary']}\n---\n"

    prompt = f"""نقش: سردبیر «عدل نیوز». انتخاب ۱ تا ۳ خبر برتر و خلاصه روان فارسی.

    قوانین:
    1. بدون حدس و اطلاعات خارج از متن.
    2. عدم استفاده از ستاره (*) یا تگ HTML در متون.
    3. لحن کوتاه ژورنالیستی.

    اخبار:
    {news_text}
    """

    for model_name in ["gemini-3.8-flash", "gemini-1.5-flash"]:
        try:
            print(f"🧠 پردازش با {model_name}...")
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=AnalyzedNewsBatch,
                    temperature=0.2,
                ),
            )
            parsed_data = json.loads(response.text)
            return parsed_data.get("selected_news", [])
        except Exception as e:
            print(f"⚠️ خطای مدل {model_name}: {e}")

    return []


def format_post(item, source_name):
    title = item.get("title", "").replace("*", "").strip()
    summary = item.get("summary", "").replace("*", "").strip()
    why_imp = item.get("why_important", "").replace("*", "").strip()
    raw_tags = item.get("hashtags", [])

    clean_tags = [f"#{t.strip().lstrip('#').replace(' ', '_')}" for t in raw_tags[:3] if t.strip()]
    hashtags_str = " ".join(clean_tags) if clean_tags else "#خبر #فناوری"

    # متن کاملاً ساده و بدون علامت بولد برای جلوگیری از به هم‌ریختگی در ایتا
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
                print(f"⚠️ خطای ایتاپیر در ارسال: {res_json}")
        else:
            print(f"⚠️ خطای HTTP از ایتاپیر: {resp.status_code}")
        return False
    except Exception as e:
        print(f"⚠️ خطا در ارتباط با ایتا: {e}")
        return False


def main():
    print("🚀 شروع اجرای فوق‌سریع ربات...")
    seen_list = load_seen()
    all_new_articles = fetch_new_articles(seen_list)

    if not all_new_articles:
        print("📭 خبر جدیدی یافت نشد.")
        return 0

    batch_articles = all_new_articles[:15]
    selected_items = select_and_analyze(batch_articles)

    if not selected_items:
        print("⚠️ خبری توسط هوش مصنوعی انتخاب نشد.")
        return 0

    sent_count = 0
    for item in selected_items[:3]:
        idx = item.get("index")
        if idx is not None and 0 <= idx < len(batch_articles):
            post_text = format_post(item, batch_articles[idx]["source"])
            if send_to_eitaa(post_text):
                sent_count += 1
                print(f"✅ خبر شماره {idx} با موفقیت به ایتا ارسال شد.")
                time.sleep(2)
            else:
                print(f"❌ ارسال خبر شماره {idx} ناموفق بود.")

    if sent_count > 0:
        for art in batch_articles:
            seen_list.append(art["link"])
        save_seen(seen_list)
        print(f"✅ مجموعاً {sent_count} پست با موفقیت منتشر و سابقه ذخیره شد.")
    else:
        print("⚠️ هیچ پستی با موفقیت ارسال نشد.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
