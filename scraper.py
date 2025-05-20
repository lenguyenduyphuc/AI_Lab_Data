from __future__ import annotations
import os, re, asyncio, pandas as pd
from typing import List, Tuple
from pathlib import Path

import asyncpraw
from dotenv import load_dotenv
from tqdm.asyncio import tqdm, tqdm_asyncio

# ── API setup ───────────────────────────────────────────────────────────
load_dotenv()

TARGET_SUBS = [
    "teenagers", "privacy", "AdviceForTeens", "mentalhealth",
    "therapists", "relationship_advice", "AskReddit", "DecidingToBeBetter",
    "dating_advice", "TooAfraidToAsk", "internetparents", "depression",
    "Parenting", "Productivitycafe", "regretfulparents", "parentingteenagers",
]

THEMES = {
    "mental_health": [
        "anxiety", "depression", "self-harm", "panic", "adhd", "bipolar",
        "eating disorder", "anorexia", "bulimia", "trauma", "ptsd",
        "schizophrenia", "psychosis", "mood swings", "mental illness",
        "suicidal ideation", "suicide", "overdose", "hopelessness",
        "worthlessness", "self hatred", "self mutilation", "cutting",
        "panic attack", "insomnia", "hallucinations", "delusion", "paranoia",
        "dissociation", "intrusive thoughts", "derealization",
        "depersonalization", "loneliness", "isolation", "numb", "void",
        "mental breakdown", "meltdown", "shutdown", "sensory overload",
        "fear", "no will to live",
    ],
    "behavioral_health": [
        "anger", "rage", "addiction", "substance abuse", "alcohol abuse",
        "drug abuse", "binge drinking", "blackout", "impulse control",
        "stress", "burnout", "gambling addiction", "codependency",
        "people-pleasing", "manipulation", "aggression", "violent outburst",
        "fight", "punching", "reckless behavior", "risk-taking", "truancy",
        "runaway", "shoplifting", "stealing", "lying", "vandalism",
        "self sabotage", "executive dysfunction", "hoarding",
        "compulsive behavior", "social anxiety", "avoidance", "phobias",
        "procrastination",
    ],
    "online_safety": [
        "cyberbullying", "bullying", "harassment", "online harassment",
        "doxxing", "grooming", "groomer", "blackmail", "clickbait", "predator",
        "online predators", "child exploitation", "sex trafficking", "nudes leak",
        "snapchat leak", "revenge porn", "catfish", "deepfake", "identity theft",
        "phishing", "malware", "hacked", "data breach", "scams", "swatting",
        "impersonation", "stalking", "online stalking", "hate speech",
        "death threat", "trolling", "flaming", "fake news", "disinformation",
        "misinformation", "sadfishing", "stranger danger",
    ],
    "dating": [
        "heartbreak", "toxic", "abuse", "emotional abuse", "physical abuse",
        "domestic violence", "sexual assault", "rape", "coercion", "cheating",
        "cheater", "gaslighting", "love bombing", "red flags", "jealousy",
        "insecurity", "obsession", "control", "manipulation", "breadcrumbing",
        "ghosting", "situationship", "mixed signals", "unrequited love",
        "abandonment", "attachment issues", "boundaries", "violated boundaries",
        "consent", "lack of consent", "sexting pressure", "nudes pressure",
        "stalking ex", "toxic ex", "hate relationship", "fight",
    ],
}

PATTERNS = {
    theme: [(kw, re.compile(rf"\b{re.escape(kw)}\b", re.I)) for kw in kws]
    for theme, kws in THEMES.items()
}

YOUTH_TOKENS = [
    "teen", "teens", "teenager", "teenagers",
    "preteen", "preadolescent", "youth", "youngster",
    "high school", "high-schooler", "highschooler",
    "middle school", "middleschooler",
    "grade 6", "grade 7", "grade 8", "grade 9",
    "grade 10", "grade 11", "grade 12"
]
YOUTH_RGX = re.compile(r"\b(?:%s)\b" % "|".join(map(re.escape, YOUTH_TOKENS)), re.I)

AGE_NUM_RGX = re.compile(
    r"""
    (
       \b(?:i[' ]?m|im)\s+
       (1[0-9])
    |  \b(1[0-9])[fm]\b
    |  \((1[0-9])[fm]\)
    )
    """,
    re.I | re.X
)

def is_age(text: str) -> bool:
    return bool(YOUTH_RGX.search(text) or AGE_NUM_RGX.search(text))

SEARCH_KEYWORDS = sorted({w for v in THEMES.values() for w in v})

def build_chunks(max_len: int = 450) -> List[str]:
    out, cur, ln = [], [], 0
    for w in SEARCH_KEYWORDS:
        add = len(w) + 4
        if ln + add > max_len and cur:
            out.append(" OR ".join(cur)); cur, ln = [w], len(w)
        else:
            cur.append(w); ln += add
    if cur:
        out.append(" OR ".join(cur))
    return out

QUERY_CHUNKS = build_chunks()
REQ_GATE = asyncio.Semaphore(60)

def theme_match(text: str) -> Tuple[str | None, str | None]:
    for theme, regs in PATTERNS.items():
        for kw, rgx in regs:
            if rgx.search(text):
                return theme, kw
    return None, None

def summary(text: str, theme: str) -> str:
    sents = re.split(r"(?<=[.!?])\s+", text)
    hits = [s.strip() for s in sents if any(rgx.search(s) for _, rgx in PATTERNS[theme])]
    if len(hits) >= 2:
        return f"{hits[0]} {hits[1]}"
    if len(hits) == 1:
        nxt = next((s for s in sents if s not in hits), "")
        return f"{hits[0]} {nxt}"
    return f"This post discusses {theme}."

async def full_text(sub):
    async with REQ_GATE: await sub.load()
    async with REQ_GATE: await sub.comments.replace_more(limit=50)
    comments = " ".join(c.body for c in sub.comments.list() if c.body)
    return f"{sub.title} {sub.selftext} {comments}"

async def scrape_sub(reddit, name) -> List[dict]:
    sr = await reddit.subreddit(name)
    pool = {}
    for chunk in QUERY_CHUNKS:
        async with REQ_GATE:
            async for s in sr.search(f"({chunk})", sort="new", time_filter="all"):
                if s.id not in pool or s.score > pool[s.id].score:
                    pool[s.id] = s
    if not pool:
        async with REQ_GATE:
            async for s in sr.new(limit=100):
                pool[s.id] = s
    subs = list(pool.values())
    if not subs:
        return []

    texts = await tqdm_asyncio.gather(*(full_text(s) for s in subs), desc=f"r/{name}", unit="post")
    rows = []
    for s, txt in zip(subs, texts):
        if not is_age(txt):
            continue
        th, kw = theme_match(txt)
        if th is None:
            continue
        rows.append({
            "subreddit": name,
            "theme": th,
            "keyword": kw,
            "title": s.title,
            "body": summary(txt, th)
        })
    return rows

async def run():
    async with asyncpraw.Reddit(
        client_id=os.environ["REDDIT_ID"],
        client_secret=os.environ["REDDIT_SECRET"],
        user_agent=os.environ.get("REDDIT_UA", "teen_scraper/0.4-async"),
        read_only=True,
    ) as reddit:
        rows: List[dict] = []
        for name in tqdm(TARGET_SUBS, desc="Subreddits"):
            rows.extend(await scrape_sub(reddit, name))

        if rows:
            pd.DataFrame(rows).to_csv(Path("Reddit.csv"), index=False, encoding="utf-8", quoting=1)

if __name__ == "__main__":
    asyncio.run(run())
