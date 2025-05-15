def matched_theme(text):
    text_low = text.lower()
    for theme, kwds in THEMES.items():
        if any(k in text_low for k in kwds):
            return theme
    return None

with OUTFILE.open("w", encoding="utf-8") as f_out:
    for sub_name in TARGET_SUBS:
        sub = reddit.subreddit(sub_name)
        search_q = " OR ".join(set(sum(THEMES.values(), [])))
        for submission in tqdm(
            sub.search(search_q, limit=RESULTS_PER_SUB, sort="new", time_filter="year"),
            desc=f"r/{sub_name}"
        ):
            theme = matched_theme(submission.title + " " + submission.selftext)
            if not theme:
                continue 

            row = {
                "type":      "submission",
                "theme":     theme,
                "subreddit": sub_name,
                "title":     submission.title,
                "body":      submission.selftext,
            }
            f_out.write(json.dumps(row) + "\n")

            # fetch comments lazily
            submission.comments.replace_more(limit=0)
            for comment in submission.comments.list():
                if comment.depth > COMMENT_DEPTH:
                    continue
                theme_c = matched_theme(comment.body)
                if not theme_c:
                    continue
                row_c = {
                    "type":      "comment",
                    "theme":     theme_c,
                    "parent_sub": sub_name,
                    "body":      comment.body,
                }
                f_out.write(json.dumps(row_c) + "\n")
            time.sleep(SLEEP_BETWEEN)

print(f"✅  Finished. Data written to {OUTFILE.resolve()}")