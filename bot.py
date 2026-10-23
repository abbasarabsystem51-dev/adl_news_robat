import json
import os
import re
import signal
import sys
import time
from pathlib import Path

import feedparser
import requests
from google import genai
from google.genai import types
from pydantic import BaseModel, Field

# تنظیم تایم‌اوت کل برنامه (حداکثر ۶۰ ثانیه)
def timeout_handler(signum, frame):
    print("⏰ زمان اجرای اسکریپت به پایان رسید (Timeout 60s). خروج جهت جلوگیری از گیر کردن.")
    sys.exit(0)

if hasattr(signal, "SIGALRM"):
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(60)

BASE_DIR = Path(__file__).resolve().parent
SEEN_FILE = BASE_DIR / "seen.json"

GEMINI_KEY = os.environ.get("GEMINI_KEY", "").strip()
EITAA_TOKEN = os.environ.get("EITAA_TOKEN", "").strip()
CHANNEL = os.environ.get("CHANNEL", "").strip()

if not all([GEMINI_KEY, EITAA_TOKEN, CHANNEL]):
    print("❌ خطای تنظیمات: متغیرها به درستی وارد نشده‌اند.")
    sys.exit(1)

CHANNEL = (
    CHANNEL.replace("https://eitaa.com/", "")
    .replace("http://eitaa.com/", "")
    .replace("@", "")
    .strip()
)

RSS_FEEDS = [
    {
        "name": "تک‌کرانچ (هوش مصنوعی)",
        "url": "https://techcrunch.com/category/artificial-intelligence/feed/",
        "category": "تکنولوژی و هوش مصنوعی جهانی",
    },
    {
        "name": "زومیت",
        "url": "https://www.zoomit.ir/feed/",
        "category": "تکنولوژی و هوش مصنوعی",
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


def fetch_new_articles(seen_list):
    seen_set = set(seen_list)
    articles = []
    batch_links = set()

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    for feed_info in RSS_FEEDS:
        try:
            print(f"⏳ دریافت فید: {feed_info['name']}...")
            resp = requests.get(feed_info["url"], headers=headers, timeout=4)
            if resp.status_code != 200:
                continue
            feed = feedparser.parse(resp.content)

            for entry in feed.entries[:3]:
                raw_link = entry.get("link", "").strip()
                norm_link = normalize_link(raw_link)
                title = clean_html(entry.get("title", ""))
                summary = clean_html(entry.get("summary", entry.get("description", "")))

                if norm_link and title and norm_link not in seen_set and norm_link not in batch_links:
                    batch_links.add(norm_link)
                    articles.append(
                        {
                            "source": feed_info["name"],
                            "category": feed_info["category"],
                            "title": title,
                            "summary": summary[:350] if summary else title,
                            "link": norm_link,
                        }
                    )
        except Exception:
            print(f"⚡ عبور سریع از {feed_info['name']}")

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
    2. عدم استفاده از ستاره (**) یا تگ HTML در متون.
    3. لحن کوتاه ژورنالیستی.

    اخبار:
    {news_text}
    """

    for model_name in ["gemini-2.5-flash", "gemini-2.0-flash"]:
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

    return f"**📌 {title}**\n\n**📝 خلاصه:**\n{summary}\n\n**💡 چرا مهم است؟**\n{why_imp}\n\n🏷 {hashtags_str}\n📡 منبع: {source_name}\n🆔 @{CHANNEL}"


def send_to_eitaa(text):
    url = f"https://eitaayar.ir/api/{EITAA_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, data={"chat_id": CHANNEL, "text": text}, timeout=8)
        return resp.status_code == 200 and resp.json().get("ok") is True
    except Exception:
        return False


def main():
    print("🚀 شروع اجرای ربات...")
    seen_list = load_seen()
    all_new_articles = fetch_new_articles(seen_list)

    if not all_new_articles:
        print("📭 خبر جدیدی یافت نشد.")
        return

    batch_articles = all_new_articles[:15]
    selected_items = select_and_analyze(batch_articles)

    if not selected_items:
        print("⚠️ خبری انتخاب نشد.")
        return

    sent_count = 0
    for item in selected_items[:3]:
        idx = item.get("index")
        if idx is not None and 0 <= idx < len(batch_articles):
            if send_to_eitaa(format_post(item, batch_articles[idx]["source"])):
                sent_count += 1
                time.sleep(2)

    if sent_count > 0:
        for art in batch_articles:
            seen_list.append(art["link"])
        save_seen(seen_list)
        print(f"✅ {sent_count} پست منتشر و ذخیره شد.")


if __name__ == "__main__":
    main()
