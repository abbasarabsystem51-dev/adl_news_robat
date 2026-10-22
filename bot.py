import json
import os
import re
import sys
import time
import feedparser
from google import genai
from google.genai import types
from pydantic import BaseModel, Field
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ۱. دریافت و اعتبارسنجی متغیرهای محیطی (Secrets)
GEMINI_KEY = os.environ.get("GEMINI_KEY", "").strip()
EITAA_TOKEN = os.environ.get("EITAA_TOKEN", "").strip()
CHANNEL = os.environ.get("CHANNEL", "").strip()

if not all([GEMINI_KEY, EITAA_TOKEN, CHANNEL]):
    print(
        "❌ خطای تنظیمات: یکی از متغیرهای GEMINI_KEY یا EITAA_TOKEN یا CHANNEL در بخش Secrets گیت‌هاب وارد نشده است!"
    )
    sys.exit(1)

# استانداردسازی آیدی کانال
CHANNEL = (
    CHANNEL.replace("https://eitaa.com/", "")
    .replace("http://eitaa.com/", "")
    .replace("@", "")
    .strip()
)

# ۲. منابع خبری (ترکیب متوازن تکنولوژی/AI و اخبار مهم سیاسی-عمومی)
RSS_FEEDS = [
    {
        "name": "زومیت",
        "url": "https://www.zoomit.ir/feed/",
        "category": "تکنولوژی و هوش مصنوعی",
    },
    {
        "name": "دیجیاتو",
        "url": "https://digiato.com/feed",
        "category": "تکنولوژی و هوش مصنوعی",
    },
    {
        "name": "تسنیم (فناوری)",
        "url": "https://www.tasnimnews.com/fa/rss/feed/0/7/82/%D9%81%D9%86%D8%A7%D9%88%D8%B1%DB%8C-%D8%A7%D8%B7%D9%84%D8%A7%D8%B9%D8%A7%D8%AA",
        "category": "تکنولوژی",
    },
    {
        "name": "خبرگزاری مهر (فناوری)",
        "url": "https://www.mehrnews.com/rss/tp/5",
        "category": "علم و فناوری",
    },
    {
        "name": "تسنیم (سیاسی)",
        "url": "https://www.tasnimnews.com/fa/rss/feed/0/7/1/%D8%B3%DB%8C%D8%A7%D8%B3%DB%8C",
        "category": "سیاسی",
    },
    {
        "name": "خبرگزاری مهر (سیاسی)",
        "url": "https://www.mehrnews.com/rss/tp/7",
        "category": "سیاسی",
    },
]

SEEN_FILE = "seen.json"


# ۳. تعریف ساختار دقیق خروجی هوش مصنوعی (جلوگیری ۱۰۰٪ از بهم‌ریختگی قالب)
class AnalyzedNewsItem(BaseModel):
    index: int = Field(description="شماره ردیف خبر انتخاب شده از لیست ورودی")
    title: str = Field(description="تیتر جذاب، دقیق و کاملا وفادار به متن خبر")
    summary: str = Field(
        description="خلاصه دقیق و روان در ۲ تا ۳ جمله کوتاه فقط بر اساس متن خبر"
    )
    why_important: str = Field(
        description="تحلیل کوتاه در ۱ یا ۲ جمله که چرا این خبر مهم است، بدون حدس یا اطلاعات خارج از متن"
    )
    hashtags: list[str] = Field(
        description="دقیقا ۳ هشتگ مرتبط به زبان فارسی (کلمات با خط زیر جدا شوند، مثال: هوش_مصنوعی)"
    )


class AnalyzedNewsBatch(BaseModel):
    selected_news: list[AnalyzedNewsItem] = Field(
        description="لیست ۱ تا ۳ خبر برتر انتخاب شده"
    )


# ۴. ساخت نشست شبکه (Session) با قابلیت تلاش مجدد و هدر مرورگر واقعی
def create_http_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "Accept": "application/xhtml+xml,application/xml,text/xml,application/json,*/*;q=0.9",
            "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7",
        }
    )
    return session


HTTP_SESSION = create_http_session()


def normalize_link(url):
    """حذف پارامترهای اضافی لینک برای جلوگیری از ثبت خبر تکراری"""
    return url.split("?")[0].rstrip("/").strip()


def load_seen():
    if os.path.exists(SEEN_FILE):
        try:
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except Exception as e:
            print(f"⚠️ هشدار در خواندن {SEEN_FILE}: {e} - شروع با لیست خالی.")
            return []
    return []


def save_seen(seen_list):
    # حذف موارد تکراری با حفظ ترتیب و نگهداری ۵۰۰ مورد آخر
    unique_seen = list(dict.fromkeys(seen_list))
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
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
            resp = HTTP_SESSION.get(feed_info["url"], timeout=15)
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)

            for entry in feed.entries[:5]:
                raw_link = entry.get("link", "").strip()
                norm_link = normalize_link(raw_link)
                title = clean_html(entry.get("title", ""))
                summary = clean_html(
                    entry.get("summary", entry.get("description", ""))
                )

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
            print(f"⚠️ خطا در دریافت فید «{feed_info['name']}»: {e}")

    return articles


def select_and_analyze(articles):
    client = genai.Client(api_key=GEMINI_KEY)

    news_text = ""
    for i, art in enumerate(articles):
        news_text += (
            f"[{i}] منبع: {art['source']} | دسته: {art['category']}\n"
            f"تیتر: {art['title']}\n"
            f"متن کوتاه: {art['summary']}\n"
            f"---\n"
        )

    prompt = f"""
تو سردبیر ارشد کانال خبری-تحلیلی «عدل نیوز» هستی.
از لیست اخبار زیر، بین ۱ تا ۳ خبر که بیشترین اهمیت و جذابیت را دارند انتخاب کن.

اولویت‌بندی انتخاب:
- اولویت اصلی با اخبار «هوش مصنوعی (AI) و تکنولوژی‌های نوین» است (۱ تا ۲ خبر).
- در کنار آن، مهم‌ترین و اثرگذارترین خبر «سیاسی یا علمی روز» را هم انتخاب کن (۱ خبر).

قوانین سخت‌گیرانه (خط قرمز):
1. به هیچ وجه اطلاعات، آمار، نام افراد یا تحلیلی که در متن خبر نیست از خودت اضافه نکن (صفر درصد توهم).
2. اگر متن کوتاه یک خبر اطلاعات کافی ندارد، فقط بر اساس همان اطلاعات موجود بنویس و چیزی حدس نزن.
3. از نوشتن ستاره (**) یا تگ‌های HTML در داخل متن‌ها خودداری کن.
4. جملات باید روان، رسمی، ژورنالیستی و کوتاه باشند تا کل پست نهایی زیر ۷۵۰ کاراکتر جا شود.

لیست اخبار ورودی:
{news_text}
"""

    models_to_try = ["gemini-2.5-flash", "gemini-2.0-flash"]
    for model_name in models_to_try:
        for attempt in range(1, 3):
            try:
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=AnalyzedNewsBatch,
                        temperature=0.2,  # دمای پایین برای جلوگیری از خیال‌پردازی و توهم
                    ),
                )
                parsed_data = json.loads(response.text)
                return parsed_data.get("selected_news", [])
            except Exception as e:
                print(
                    f"⚠️ تلاش {attempt} با مدل {model_name} ناموفق بود: {e}"
                )
                time.sleep(4)

    print("❌ خطای بحرانی: ارتباط با سرورهای Gemini پس از چندین تلاش برقرار نشد.")
    sys.exit(1)


def format_post(item, source_name):
    """مونتاژ امن پست در پایتون به شکلی که هرگز زیر ۸۰۰ کاراکتر قطع نشود"""
    title = item.get("title", "").replace("*", "").strip()
    summary = item.get("summary", "").replace("*", "").strip()
    why_imp = item.get("why_important", "").replace("*", "").strip()
    raw_tags = item.get("hashtags", [])

    clean_tags = []
    for t in raw_tags[:3]:
        t_clean = t.strip().lstrip("#").replace(" ", "_")
        if t_clean:
            clean_tags.append(f"#{t_clean}")
    hashtags_str = " ".join(clean_tags) if clean_tags else "#فناوری #اخبار_روز"

    footer = f"\n\n🏷 {hashtags_str}\n📡 منبع: {source_name}\n🆔 @{CHANNEL}"
    header = f"📌 {title}\n\n"

    # محاسبه فضای باقی‌مانده برای بدنه تا کل پست همیشه زیر ۷۸۰ کاراکتر بماند
    max_body_len = 780 - len(header) - len(footer)

    body = f"📝 خلاصه:\n{summary}\n\n💡 چرا مهم است؟\n{why_imp}"
    if len(body) > max_body_len:
        # کوتاه کردن هوشمند بدنه بدون آسیب زدن به فوتر (منبع و آیدی کانال)
        body = body[: max_body_len - 3].rsplit(" ", 1)[0] + "..."

    return f"{header}{body}{footer}"


def send_to_eitaa(text):
    url = f"https://eitaayar.ir/api/{EITAA_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHANNEL,
        "text": text,
    }

    try:
        response = HTTP_SESSION.post(url, data=payload, timeout=20)

        if response.status_code != 200:
            print(f"❌ خطا از سمت سرور ایتایار (کد وضعیت HTTP: {response.status_code})")
            if response.status_code == 403:
                print(
                    "💡 علت خطا: فایروال ایتایار دسترسی سرور خارج از کشور (GitHub) را مسدود کرده است."
                )
            print(f"پاسخ خام سرور: {response.text[:300]}")
            return False

        data = response.json()
        if data.get("ok") is True:
            print("✅ پست با موفقیت در کانال ایتا منتشر شد!")
            return True
        else:
            print(f"❌ API ایتایار پیام را رد کرد: {data}")
            print(
                "💡 بررسی کنید: ۱) توکن ایتایار صحیح باشد ۲) بات ایتایار در کانال ادمین باشد ۳) آیدی کانال درست باشد."
            )
            return False

    except requests.exceptions.ConnectTimeout:
        print("❌ خطای Timeout: اتصال به سرور eitaayar.ir بیش از حد طول کشید.")
        print(
            "💡 علت احتمالی: مسدود بودن موقت IPهای خارج از کشور روی سرورهای ایتایار."
        )
        return False
    except requests.exceptions.ConnectionError as e:
        print(f"❌ خطای شبکه در اتصال به ایتایار: {e}")
        print(
            "💡 علت احتمالی: فایروال شبکه داخلی ایران ارتباط از سرور GitHub را قطع کرده است."
        )
        return False
    except Exception as e:
        print(f"❌ خطای پیش‌بینی‌نشده در ارسال به ایتا: {e}")
        return False


def main():
    print("🚀 شروع اجرای ربات هوشمند عدل نیوز...")
    seen_list = load_seen()
    all_new_articles = fetch_new_articles(seen_list)

    if not all_new_articles:
        print("📭 هیچ خبر جدیدی از آخرین اجرا تا الان منتشر نشده است. پایان کار.")
        return

    # انتخاب حداکثر ۲۴ خبر اول برای ارسال به هوش مصنوعی
    batch_articles = all_new_articles[:24]
    print(
        f"🔍 تعداد {len(batch_articles)} خبر جدید دریافت شد. در حال تحلیل توسط Gemini..."
    )

    selected_items = select_and_analyze(batch_articles)

    if not selected_items:
        print("⚠️ هیچ خبری توسط هوش مصنوعی انتخاب نشد.")
        return

    sent_count = 0
    for item in selected_items[:3]:
        idx = item.get("index")
        if idx is not None and 0 <= idx < len(batch_articles):
            source_name = batch_articles[idx]["source"]
            post_text = format_post(item, source_name)

            print(
                f"📤 در حال ارسال خبر «{item.get('title')}» ({len(post_text)} کاراکتر) به @{CHANNEL}..."
            )
            if send_to_eitaa(post_text):
                sent_count += 1
                time.sleep(5)  # وقفه ایمن بین ارسال پست‌ها
            else:
                print("⚠️ ارسال متوقف شد تا در نوبت بعدی مجدداً تلاش شود.")
                break

    # اگر ارسال به ایتا موفقیت‌آمیز بود، تمام اخبار بررسی‌شده این نوبت را در seen.json ثبت کن
    # تا در نوبت ۴ ساعت بعد، اخبار ردشده‌ی این نوبت دوباره بررسی نشوند.
    if sent_count > 0:
        for art in batch_articles:
            seen_list.append(art["link"])
        save_seen(seen_list)
        print(
            f"💾 تعداد {sent_count} پست منتشر شد و {len(batch_articles)} خبر بررسی‌شده در seen.json بایگانی شد."
        )
    else:
        print("❌ هیچ پستی ارسال نشد؛ فایل seen.json تغییر نکرد تا در نوبت بعد دوباره تلاش شود.")
        sys.exit(1)


if __name__ == "__main__":
    main()
