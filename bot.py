
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
