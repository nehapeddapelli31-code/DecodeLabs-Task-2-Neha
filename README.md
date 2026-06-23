# ✍️ Automated Copywriting & Tone Transformer
### DecodeLabs — Generative AI · Project 2 · Batch 2026

An enterprise-grade content generation engine that transforms raw product descriptions into platform-optimized marketing copy across multiple tones using dynamic prompt template compilation and async orchestration.

---

## 🏗️ Architecture — Dual-Pipeline Orchestration Engine

```
CLI Input ──▶ Prompt Compiler ──▶ Router ──┬──▶ Real-time Async Pipeline (asyncio + Semaphores)
(argparse)    (f-strings merge              │
               variables)                  └──▶ Bulk Processing Pipeline (Batch API)
                                                         │
                                               Tenacity Retry Shield
                                                         │
                                               Pydantic Validation
                                                         │
                                               Final Structured Output
```

---

## 🎨 Supported Platforms & Tones

### Platforms
| Key | Platform | Char Limit | Style |
|---|---|---|---|
| `linkedin` | LinkedIn | 3,000 | Thought-leadership, professional |
| `instagram` | Instagram | 2,200 | Visual storytelling, emojis |
| `twitter` | Twitter/X | **280** | Punchy, direct (hard limit enforced) |
| `email` | Email | 5,000 | Subject line + body + CTA |
| `facebook` | Facebook | 63,206 | Community-oriented, question-driven |

### Tones + Temperature Mapping
| Tone | Temperature | Best For |
|---|---|---|
| `professional` | 0.2 | LinkedIn, corporate emails |
| `witty` | 0.8 | Instagram, Twitter |
| `urgent` | 0.5 | Flash sales, limited offers |
| `inspirational` | 0.7 | Brand storytelling |
| `friendly` | 0.6 | Facebook, community posts |
| `minimalist` | 0.3 | Premium brands, clean copy |
| `luxury` | 0.4 | High-end product launches |

---

## ⚙️ Setup

```bash
git clone https://github.com/YOUR_USERNAME/copywriting-tone-transformer.git
cd copywriting-tone-transformer
pip install -r requirements.txt
export OPENAI_API_KEY="your_key_here"
```

---

## 🚀 Usage

### Single product, multiple platforms + tones
```bash
python copywriter.py \
  --product "AirMax Pro" \
  --description "Lightweight wireless earbuds with 40hr battery and ANC" \
  --platforms linkedin instagram twitter \
  --tones professional witty \
  --audience "tech-savvy millennials" \
  --usp "2x battery life of competition"
```

### Bulk processing from CSV
```bash
python copywriter.py \
  --csv sample_products.csv \
  --platforms linkedin email \
  --tones professional urgent \
  --output outputs/batch_results.json
```

### List all available options
```bash
python copywriter.py --list
```

---

## 🔑 Key Engineering Concepts

### Master Instruction Template (f-string compiler)
Variables like `Product_Name`, `Platform`, `Tone` are injected via Python f-strings into a hidden template. The user provides raw facts; the application enforces brand safety and structural constraints.

```python
compiled = f"""You are an expert marketing copywriter...
  Product Name: {product_name}
  Platform Constraints: {platform_constraint}  # e.g., "Max 280 chars" for Twitter
  Tone: {tone.upper()} — {tone_description}
"""
```

### Parameter Tuning Spectrum
```
Temperature 0.2 ──────────────────────────── 0.8
  └─ Consistent, structured, factual    Diverse, witty, unexpected hooks
     (Professional emails, LinkedIn)    (Instagram, Twitter social copy)
```

### Async + Semaphore Gate
```python
semaphore = asyncio.Semaphore(5)    # Max 5 concurrent connections
async with semaphore:
    response = await client.chat.completions.create(...)

# asyncio.gather fires all tasks concurrently — overlapping wait times
results = await asyncio.gather(*tasks, return_exceptions=True)
# 10 tasks × 5s each = 5s total (vs 50s synchronous)
```

### Tenacity Retry Shield
```python
delay = 1 * (2 ** attempt) + random.uniform(-0.5, 0.5)
# attempt 1 → ~1.5s  |  attempt 2 → ~3.5s  |  attempt 3 → ~7.5s
```

### Pydantic Output Validation
Every API response is parsed through a strict Pydantic schema before delivery — no malformed JSON reaches downstream systems.

---

## 📂 Project Structure

```
copywriting-tone-transformer/
├── copywriter.py        # Main engine — CLI, async pipeline, orchestration
├── templates.py         # Master Instruction Template compiler + platform profiles
├── models.py            # Pydantic output schemas (CopyOutput, BatchCopyOutput)
├── sample_products.csv  # Demo CSV for bulk processing
├── outputs/             # Generated JSON results
└── requirements.txt
```

---

## 📊 Sync vs Async Performance

| Approach | 10 tasks | 50 tasks |
|---|---|---|
| Synchronous | ~50s | ~250s |
| **Async + Semaphore** | **~5–10s** | **~25–50s** |

Key insight: async doesn't make one call faster — it **overlaps** network waiting periods across concurrent requests.

---

## 📜 License

MIT License — Built for DecodeLabs Industrial Training Kit · Batch 2026
