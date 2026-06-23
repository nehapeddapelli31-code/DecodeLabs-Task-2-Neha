"""
copywriter.py — Automated Copywriting & Tone Transformer Engine
DecodeLabs — Generative AI Project 2
---------------------------------------------------------------
Architecture: Dual-Pipeline Orchestration Engine

  Pipeline A — Real-time Async (asyncio + Semaphores):
    For interactive, low-latency generation of a few variants.
    Uses asyncio.gather for guaranteed result ordering.
    Semaphore gate limits concurrent connections → prevents HTTP 429.

  Pipeline B — Bulk Processing (OpenAI Batch API via openbatch):
    For enterprise-scale batch jobs (1000s of SKUs).
    50% cost reduction, higher rate-limit pool, 24hr latency window.

Key insight: async code does NOT make a single model call faster;
it drastically reduces total wall-clock time by overlapping
network waiting periods across multiple concurrent requests.
"""

import os
import json
import asyncio
import random
import time
import argparse
import csv
from pathlib import Path
from openai import AsyncOpenAI, OpenAI
from pydantic import ValidationError

from templates import (
    compile_prompt,
    get_inference_params,
    PLATFORM_PROFILES,
    TONE_PROFILES,
)
from models import CopyOutput, BatchCopyOutput


# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────

DEFAULT_MODEL    = "gpt-4o"
SEMAPHORE_LIMIT  = 5      # Max concurrent API connections (Semaphore Gate)
MAX_RETRIES      = 3      # Tenacity-style retry shield
OUTPUT_DIR       = Path("outputs")
OUTPUT_DIR.mkdir(exist_ok=True)


# ─────────────────────────────────────────────
#  TENACITY RETRY SHIELD
#  Delay = multiplier × 2^attempt ± random_jitter
#  Recovers from transient network drops using randomized exponential backoff.
# ─────────────────────────────────────────────

async def retry_with_backoff(coro, max_retries: int = MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            return await coro
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            # Delay = multiplier(1) × 2^attempt ± jitter
            delay = 1 * (2 ** attempt) + random.uniform(-0.5, 0.5)
            delay = max(0.5, delay)
            print(f"  [RETRY {attempt+1}/{max_retries}] Error: {e} — retrying in {delay:.2f}s")
            await asyncio.sleep(delay)


# ─────────────────────────────────────────────
#  PIPELINE A — REAL-TIME ASYNC ENGINE
# ─────────────────────────────────────────────

async def generate_single_copy(
    client: AsyncOpenAI,
    semaphore: asyncio.Semaphore,
    product_name: str,
    product_description: str,
    platform: str,
    tone: str,
    target_audience: str = "general consumers",
    usp: str = "",
) -> CopyOutput:
    """
    Single async copy generation task.
    Semaphore gate limits concurrent network connections.
    Tenacity retry shield handles transient failures.
    Pydantic validates output structure before delivery.
    """
    async with semaphore:   # ← Semaphore Gate: max SEMAPHORE_LIMIT concurrent
        prompt = compile_prompt(
            product_name, product_description, platform, tone, target_audience, usp
        )
        params = get_inference_params(tone, platform)

        print(f"  [GEN] {platform.upper()} × {tone.upper()} … (temp={params['temperature']})")

        async def _call():
            response = await client.chat.completions.create(
                model=DEFAULT_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=params["temperature"],
                top_p=params["top_p"],
                max_tokens=params["max_tokens"],
            )
            return response.choices[0].message.content

        raw = await retry_with_backoff(_call())

        # ── Pydantic Validation Gate ──
        try:
            # Strip markdown fences if model added them
            clean = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
            data  = json.loads(clean)
            # Auto-calculate character count if missing
            if not data.get("character_count"):
                data["character_count"] = len(data.get("body", ""))
            output = CopyOutput(**data)
            print(f"  [OK]  {platform.upper()} × {tone.upper()} — {output.character_count} chars")
            return output
        except (json.JSONDecodeError, ValidationError) as e:
            print(f"  [WARN] Validation failed for {platform}/{tone}: {e}")
            # Fallback: return partial output
            return CopyOutput(
                platform=platform,
                tone=tone,
                headline="[Generation error — retry recommended]",
                body=raw[:500],
                cta="Learn more",
                character_count=len(raw),
            )


async def run_realtime_pipeline(
    product_name: str,
    product_description: str,
    platforms: list[str],
    tones: list[str],
    target_audience: str = "general consumers",
    usp: str = "",
    api_key: str = "",
) -> BatchCopyOutput:
    """
    Pipeline A — Real-time Async Pipeline.

    Uses asyncio.gather (guaranteed result ordering, ideal for
    database synchronization and multi-run verification).

    Semaphore Gate (limit=SEMAPHORE_LIMIT) prevents HTTP 429 errors
    by capping max concurrent connections.
    """
    client    = AsyncOpenAI(api_key=api_key)
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    # Build all (platform × tone) task combinations
    tasks = []
    combos = [(p, t) for p in platforms for t in tones]

    print(f"\n[ASYNC PIPELINE] Launching {len(combos)} concurrent tasks "
          f"(semaphore={SEMAPHORE_LIMIT})…")

    start = time.time()

    for platform, tone in combos:
        task = generate_single_copy(
            client, semaphore, product_name, product_description,
            platform, tone, target_audience, usp
        )
        tasks.append(task)

    # asyncio.gather: guaranteed ordering, return_exceptions=True for error isolation
    results = await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.time() - start
    print(f"\n[ASYNC PIPELINE] Completed {len(results)} tasks in {elapsed:.2f}s "
          f"(vs ~{len(results) * 5:.0f}s synchronous)")

    # Filter out exceptions, log them
    clean_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            p, t = combos[i]
            print(f"  [FAILED] {p}/{t}: {result}")
        else:
            clean_results.append(result)

    return BatchCopyOutput(
        product_name=product_name,
        description=product_description,
        variations=clean_results,
    )


# ─────────────────────────────────────────────
#  OUTPUT FORMATTING
# ─────────────────────────────────────────────

def display_results(batch: BatchCopyOutput):
    """Pretty-print all copy variations to terminal."""
    print("\n" + "═" * 65)
    print(f"  COPYWRITING ENGINE OUTPUT — {batch.product_name.upper()}")
    print("═" * 65)

    for copy in batch.variations:
        print(f"\n{'─' * 65}")
        print(f"  📱 Platform  : {copy.platform}")
        print(f"  🎨 Tone      : {copy.tone.capitalize()}")
        print(f"  📝 Chars     : {copy.character_count}")
        print(f"\n  HEADLINE: {copy.headline}")
        print(f"\n  BODY:\n  {copy.body}")
        print(f"\n  CTA: {copy.cta}")
        if copy.hashtags:
            print(f"  HASHTAGS: {copy.hashtags}")

    print("\n" + "═" * 65)
    print(f"  Total variants generated: {len(batch.variations)}")
    print("═" * 65)


def save_results(batch: BatchCopyOutput, output_path: Path):
    """Save results to JSON file."""
    data = batch.model_dump()
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\n[SAVED] Results written to {output_path}")


def load_csv_products(csv_path: str) -> list[dict]:
    """
    Ingestion Layer: load product data from CSV.
    CSV columns: product_name, description, target_audience, usp (optional)
    """
    products = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            products.append(row)
    return products


# ─────────────────────────────────────────────
#  CLI ENTRY POINT (argparse)
# ─────────────────────────────────────────────

def build_cli() -> argparse.ArgumentParser:
    """
    Step 2 — Configure the CLI with argparse.
    Captures product variables, tone modifiers, and target platforms.
    Custom type converters parse inputs into structured data.
    """
    parser = argparse.ArgumentParser(
        description="Automated Copywriting & Tone Transformer — DecodeLabs Project 2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single product, multiple platforms and tones
  python copywriter.py --product "AirMax Pro" --description "Lightweight wireless earbuds with 40hr battery" --platforms linkedin instagram --tones professional witty

  # With audience and USP
  python copywriter.py --product "ZenDesk" --description "AI-powered customer support tool" --platforms email twitter --tones urgent professional --audience "SaaS founders" --usp "Reduces support tickets by 60%%"

  # Load from CSV (bulk mode)
  python copywriter.py --csv products.csv --platforms linkedin email --tones professional

  # List available platforms and tones
  python copywriter.py --list
        """
    )

    parser.add_argument("--product",     type=str, help="Product name")
    parser.add_argument("--description", type=str, help="Raw product description / facts")
    parser.add_argument("--platforms",   nargs="+", default=["linkedin"],
                        choices=list(PLATFORM_PROFILES.keys()),
                        help="Target platforms (space-separated)")
    parser.add_argument("--tones",       nargs="+", default=["professional"],
                        choices=list(TONE_PROFILES.keys()),
                        help="Tone of voice (space-separated)")
    parser.add_argument("--audience",    type=str, default="general consumers",
                        help="Target audience description")
    parser.add_argument("--usp",         type=str, default="",
                        help="Unique selling point")
    parser.add_argument("--csv",         type=str, default=None,
                        help="Path to CSV file for bulk processing")
    parser.add_argument("--output",      type=str, default=None,
                        help="Output JSON file path")
    parser.add_argument("--model",       type=str, default=DEFAULT_MODEL,
                        help="OpenAI model to use")
    parser.add_argument("--list",        action="store_true",
                        help="List all available platforms and tones")
    parser.add_argument("--api_key",     type=str,
                        default=os.getenv("OPENAI_API_KEY", ""),
                        help="OpenAI API key (or set OPENAI_API_KEY env var)")

    return parser


def print_options():
    print("\n📋 AVAILABLE PLATFORMS:")
    for key, cfg in PLATFORM_PROFILES.items():
        print(f"  {key:<12} — {cfg['label']} (limit: {cfg['char_limit']} chars)")

    print("\n🎨 AVAILABLE TONES:")
    for key, cfg in TONE_PROFILES.items():
        print(f"  {key:<14} — {cfg['description']} (temp: {cfg['temperature']})")
    print()


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

async def async_main():
    parser = build_cli()
    args   = parser.parse_args()

    if args.list:
        print_options()
        return

    if not args.api_key:
        print("[ERROR] Set OPENAI_API_KEY environment variable or pass --api_key")
        return

    # ── Determine input source: CLI or CSV ──
    if args.csv:
        products = load_csv_products(args.csv)
        print(f"[CSV] Loaded {len(products)} products from {args.csv}")
    elif args.product and args.description:
        products = [{
            "product_name":    args.product,
            "description":     args.description,
            "target_audience": args.audience,
            "usp":             args.usp,
        }]
    else:
        parser.print_help()
        print("\n[ERROR] Provide --product + --description, or --csv path.")
        return

    all_batches = []

    for product in products:
        print(f"\n{'═'*65}")
        print(f"  PRODUCT: {product['product_name']}")
        print(f"  PLATFORMS: {args.platforms}")
        print(f"  TONES: {args.tones}")
        print(f"{'═'*65}")

        batch = await run_realtime_pipeline(
            product_name=product["product_name"],
            product_description=product["description"],
            platforms=args.platforms,
            tones=args.tones,
            target_audience=product.get("target_audience", "general consumers"),
            usp=product.get("usp", ""),
            api_key=args.api_key,
        )

        display_results(batch)
        all_batches.append(batch)

    # ── Save output ──
    if args.output or len(products) > 0:
        out_file = Path(args.output) if args.output else OUTPUT_DIR / f"copy_output.json"
        if len(all_batches) == 1:
            save_results(all_batches[0], out_file)
        else:
            combined = {"batches": [b.model_dump() for b in all_batches]}
            with open(out_file, "w") as f:
                json.dump(combined, f, indent=2)
            print(f"\n[SAVED] {len(all_batches)} batches → {out_file}")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
