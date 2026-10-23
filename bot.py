import json
import os
import re
import sys
import time
from pathlib import Path

import feedparser
import requests
from google import genai
from google.genai import types
from pydantic import BaseModel, Field
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DIR = Path(__file__).resolve().parent
SEEN_FILE = BASE_DIR / "seen.json"

GEMINI_KEY = os.environ.get("GEMINI_KEY", "").strip()
EITAA_TOKEN = os.environ.get("EITAA_TOKEN", "").strip()
CHANNEL = os.environ.get("CHANNEL", "").strip()

if not all([GEMINI_KEY, EITAA_TOKEN, CHANNEL]):
    print("❌ خطای تنظیمات: متغیرهای GEMINI_KEY، EITAA_TOKEN یا CHANNEL ست نشده‌اند.")
    raise SystemExit(1)

CHANNEL = (
    CHANNEL.replace("https://eitaa.com/", "")
    .replace("http://eitaa.com/", "")
    .replace("@", "")
    .strip()
)

# لیست منابع خبری معتبر و فعال
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
    index: int = Field(description="شماره ردیف خبر انتخاب شده از لیست ورودی")
    title: str = Field(description="تیتر جذاب، دقیق و روان به زبان فارسی")
    summary: str = Field(
        description="خلاصه دقیق در ۲ تا ۳ جمله کوتاه به زبان فارسی روان"
    )
    why_important: str = Field(
        description="تحلیل کوتاه در ۱ یا ۲ جمله که چرا این خبر مهم است"
    )
    hashtags: list[str] = Field(
        description="دقیقا ۳ هشتگ مرتبط به زبان فارسی (مثال: هوش_مصنوعی)"
    )


class AnalyzedNewsBatch(BaseModel):
    selected_news: list[AnalyzedNewsItem] = Field(
        description="لیست ۱ تا ۳ خبر برتر انتخاب شده"
    )


def create_http_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=1,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "Accept": "application/xhtml+xml,application/xml,text/xml,application/json,*/*;q=0.9",
        }
    )
    return session


HTTP_SESSION = create_http_session()


def normalize_link(url):
    return url.split("?")[0].rstrip("/").strip()


def load_seen():
    if not SEEN_FILE.exists():
        return []

    try:
        with SEEN_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception as e:
        print(f"⚠️ هشدار در خواندن فایل دیده شده‌ها: {e}")
        return []


def save_seen(seen_list):
    unique_seen = list(dict.fromkeys(seen_list))
    with SEEN_FILE.open("w", encoding="utf-8") as f:
        json.dump(unique_seen[-500:], f, ensure_ascii=False, indent=2)


def clean_html(raw_html):
    text = re.sub(r"<[^>]+>", " ", raw_html or "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def fetch_new_articles(seen_list):
    seen_set = set(seen_list)
    articles = []
    batch_links = set()

    for feed_info in RSS_FEEDS:
        try:
            print(f"⏳ در حال دریافت فید: {feed_info['name']}...")
            resp = HTTP_SESSION.get(feed_info["url"], timeout=8)
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)

            for entry in feed.entries[:5]:
                raw_link = entry.get("link", "").strip()
                norm_link = normalize_link(raw_link)
                title = clean_html(entry.get("title", ""))
                summary = clean_html(entry.get("summary", entry.get("description", "")))

                if (
                    norm_link
                    and title
                    and norm_link not in seen_set
                    and norm_link not in batch_links
                ):
                    batch_links.add(norm_link)
                    articles.append(
                        {
                            "source": feed_info["name"],
                            "category": feed_info["category"],
                            "title": title,
                            "summary": summary[:500] if summary else title,
                            "link": norm_link,
                        }
                    )
        except Exception as e:
            print(f"⚠️ عبور سریع از فید «{feed_info['name']}» به دلیل خطا/تایم‌اوت: {e}")

    return articles


def select_and_analyze(articles):
    if not articles:
        return []

    client = genai.Client(api_key=GEMINI_KEY)
    news_text = ""

    for i, art in enumerate(articles):
        news_text += (
            f"[{i}] منبع: {art['source']} | دسته: {art['category']}\n"
            f"تیتر: {art['title']}\n"
            f"متن: {art['summary']}\n"
            f"---\n"
        )

    prompt = f"""نقش: سردبیر ارشد «عدل نیوز». وظیفه: انتخاب ۱ تا ۳ خبر برتر از لیست زیر، ترجمه روان (برای منابع انگلیسی) و خلاصه استاندارد به فارسی.

    قوانین سخت‌گیرانه:
    1. استناد صددرصدی به متن؛ بدون حدس، آمار یا اطلاعات خارج از متن.
    2. ممنوعیت کامل استفاده از ستاره (**) یا تگ‌های HTML در محتوای درون فیلدها.
    3. لحن کاملاً ژورنالیستی و محتوای کوتاه.
    4. ارجاع دقیق به ایندکس عددی خبر `[index]`.

    لیست اخبار ورودی:
    {news_text}
    """

    for model_name in ["gemini-2.5-flash", "gemini-2.0-flash"]:
        try:
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
            print(f"⚠️ خطا با مدل {model_name}: {e}")

    print("❌ خطای ارتباط با جمینای.")
    return []


def format_post(item, source_name):
    title = item.get("title", "").replace("*", "").strip()
    summary = item.get("summary", "").replace("*", "").strip()
    why_imp = item.get("why_important", "").replace("*", "").strip()
    raw_tags = item.get("hashtags", [])

    clean_tags = []
    for t in raw_tags[:3]:
        t_clean = t.strip().lstrip("#").replace(" ", "_")
        if t_clean:
            clean_tags.append(f"#{t_clean}")

    hashtags_str = " ".join(clean_tags) if clean_tags else "#فناوری #بین_الملل #اخبار_روز"
    
    # فرمت کاملاً سازگار با Markdown ایتا
    header = f"**📌 {title}**\n\n"
    body = f"**📝 خلاصه:**\n{summary}\n\n**💡 چرا مهم است؟**\n{why_imp}"
    footer = f"\n\n🏷 {hashtags_str}\n📡 منبع: {source_name}\n🆔 @{CHANNEL}"

    return f"{header}{body}{footer}"


def send_to_eitaa(text):
    url = f"https://eitaayar.ir/api/{EITAA_TOKEN}/sendMessage"
    payload = {"chat_id": CHANNEL, "text": text}

    try:
        response = HTTP_SESSION.post(url, data=payload, timeout=15)
        if response.status_code == 200 and response.json().get("ok") is True:
            print("✅ پست با موفقیت در کانال ایتا منتشر شد!")
            return True
        print(f"❌ خطا از ایتا: {response.text[:300]}")
        return False
    except Exception as e:
        print(f"❌ خطای ارسال به ایتا: {e}")
        return False


def main():
    print("🚀 شروع اجرای ربات هوشمند عدل نیوز...")
    seen_list = load_seen()
    all_new_articles = fetch_new_articles(seen_list)

    if not all_new_articles:
        print("📭 هیچ خبر جدیدی یافت نشد.")
        return 0

    batch_articles = all_new_articles[:25]
    print(f"🔍 تعداد {len(batch_articles)} خبر جدید آماده تحلیل است.")

    selected_items = select_and_analyze(batch_articles)
    if not selected_items:
        print("⚠️ خبری توسط هوش مصنوعی انتخاب نشد.")
        return 0

    sent_count = 0
    for item in selected_items[:3]:
        idx = item.get("index")
        if idx is not None and 0 <= idx < len(batch_articles):
            source_name = batch_articles[idx]["source"]
            post_text = format_post(item, source_name)

            if send_to_eitaa(post_text):
                sent_count += 1
                time.sleep(3)
            else:
                break

    if sent_count > 0:
        for art in batch_articles:
            seen_list.append(art["link"])
        save_seen(seen_list)
        print(f"💾 تعداد {sent_count} پست منتشر و ذخیره شد.")
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
