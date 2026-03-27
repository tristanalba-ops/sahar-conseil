"""
SAHAR Conseil — Content Factory
Module 06 : Génération audio podcast via ElevenLabs

Pipeline :
  1. Génère un script podcast dialogue (2 voix : Alex + Marie)
     depuis le HTML de l'article — via Claude API
  2. Synthèse vocale ElevenLabs (2 voix différentes)
  3. Merge audio MP3 final
  4. Upload vers un dossier /output/audio/

Usage :
  python 06_audio.py --slug mon-article
  python 06_audio.py --all

Prérequis :
  pip install requests pydub
  export ELEVENLABS_API_KEY=...
  export ANTHROPIC_API_KEY=...
"""

import os
import re
import json
import time
import argparse
import requests
from pathlib import Path
from datetime import datetime

HERE         = Path(__file__).parent.parent
OUT_ARTICLES = HERE / "output" / "articles"
OUT_AUDIO    = HERE / "output" / "audio"
OUT_AUDIO.mkdir(parents=True, exist_ok=True)

ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
ELEVENLABS_KEY = os.getenv("ELEVENLABS_API_KEY", "")
MODEL          = "claude-sonnet-4-20250514"

# Voix ElevenLabs (IDs stables, voix françaises naturelles)
# https://api.elevenlabs.io/v1/voices
VOICE_HOST  = os.getenv("EL_VOICE_HOST",  "pNInz6obpgDQGcFmaJgB")  # Adam — voix masculine
VOICE_GUEST = os.getenv("EL_VOICE_GUEST", "EXAVITQu4vr4xnSDxMaL")  # Bella — voix féminine

PODCAST_DURATION_TARGET = 8   # minutes cible
WORDS_PER_MINUTE        = 140  # débit naturel français


# ─────────────────────────────────────────────────────────────────────────────
# GÉNÉRATION SCRIPT
# ─────────────────────────────────────────────────────────────────────────────

def generate_script(data: dict) -> str:
    """
    Génère un script podcast dialogue 2 voix depuis l'article.
    Format : ALEX: texte\nMARIE: texte\n...
    """
    if not ANTHROPIC_KEY:
        raise ValueError("ANTHROPIC_API_KEY non définie")

    keyword   = data.get("keyword", "")
    html      = data.get("html", "")
    secteur   = data.get("secteur", "")
    cible     = data.get("cible", "")
    duration  = PODCAST_DURATION_TARGET
    words_target = duration * WORDS_PER_MINUTE

    # Extraire le texte brut
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text).strip()

    prompt = f"""Tu vas écrire un script de podcast audio de {duration} minutes ({words_target} mots environ)
sur le sujet : "{keyword}"

CONTEXTE :
  - Podcast de SAHAR Conseil — outils de prospection avec données publiques françaises
  - Secteur : {secteur}
  - Audience : {cible}
  - Ton : conversationnel, concret, terrain — pas académique

STRUCTURE (respecter cet ordre) :
  1. Intro 30s : ALEX présente le sujet et pourquoi c'est important maintenant
  2. Contexte 90s : MARIE apporte les chiffres clés et le contexte réglementaire/marché
  3. Problème 60s : ALEX décrit le problème vécu terrain (cas concret)
  4. Solution 3min : dialogue ALEX/MARIE — comment SAHAR résout ce problème avec les données
  5. Cas pratique 90s : MARIE donne un exemple concret étape par étape
  6. Outro 30s : ALEX — résumé + mention outil SAHAR + où aller

RÈGLES FORMAT STRICT :
  - Chaque réplique commence par "ALEX:" ou "MARIE:" sur sa propre ligne
  - Répliques courtes (2-4 phrases max)
  - Naturel à l'oral : contractions, questions rhétoriques, "vous savez", "imaginez"
  - Chiffres et faits extraits de l'article ci-dessous
  - Mentionner SAHAR Conseil naturellement (pas de pub agressive)
  - FIN du script : une seule ligne "FIN"

ARTICLE SOURCE :
{text[:3000]}

Génère UNIQUEMENT le script. Pas d'intro, pas d'explication."""

    headers = {
        "Content-Type":      "application/json",
        "x-api-key":         ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
    }
    body = {
        "model":      MODEL,
        "max_tokens": 3000,
        "messages":   [{"role": "user", "content": prompt}],
    }

    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers=headers, json=body, timeout=90,
    )
    r.raise_for_status()
    return r.json()["content"][0]["text"]


# ─────────────────────────────────────────────────────────────────────────────
# SYNTHÈSE VOCALE
# ─────────────────────────────────────────────────────────────────────────────

def parse_script(script: str) -> list:
    """
    Parse le script en liste de tuples (speaker, text).
    Retourne [(speaker, text), ...]
    """
    lines = []
    for line in script.split('\n'):
        line = line.strip()
        if line.startswith("ALEX:"):
            lines.append(("ALEX", line[5:].strip()))
        elif line.startswith("MARIE:"):
            lines.append(("MARIE", line[6:].strip()))
        elif line == "FIN":
            break
    return lines


def tts_elevenlabs(text: str, voice_id: str) -> bytes | None:
    """Synthétise une réplique via ElevenLabs. Retourne les bytes MP3."""
    if not ELEVENLABS_KEY:
        raise ValueError("ELEVENLABS_API_KEY non définie")
    if not text.strip():
        return None

    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
    headers = {
        "xi-api-key":   ELEVENLABS_KEY,
        "Content-Type": "application/json",
    }
    body = {
        "text": text,
        "model_id": "eleven_multilingual_v2",
        "voice_settings": {
            "stability":        0.55,
            "similarity_boost": 0.75,
            "style":            0.10,
            "use_speaker_boost": True,
        },
    }

    r = requests.post(url, headers=headers, json=body, timeout=60)
    if r.status_code == 200:
        return r.content
    else:
        print(f"  ⚠️  ElevenLabs {r.status_code}: {r.text[:200]}")
        return None


def merge_audio_segments(segments: list[bytes], output_path: Path) -> bool:
    """
    Merge les segments MP3 en un seul fichier.
    Utilise pydub si disponible, sinon concaténation directe (moins propre).
    """
    try:
        from pydub import AudioSegment
        import io
        combined = AudioSegment.empty()
        silence  = AudioSegment.silent(duration=400)  # 400ms pause entre répliques
        for seg_bytes in segments:
            seg = AudioSegment.from_mp3(io.BytesIO(seg_bytes))
            combined += seg + silence
        combined.export(output_path, format="mp3", bitrate="128k")
        return True
    except ImportError:
        # Fallback : concat brute (peut craquer entre segments)
        with open(output_path, "wb") as f:
            for seg in segments:
                f.write(seg)
        return True
    except Exception as e:
        print(f"  ⚠️  Merge audio erreur : {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATION
# ─────────────────────────────────────────────────────────────────────────────

def generate_podcast(slug: str) -> dict | None:
    """Pipeline complet pour un article : script → TTS → merge → sauvegarde."""

    art_path = OUT_ARTICLES / f"{slug}.json"
    if not art_path.exists():
        print(f"❌ Article introuvable : {art_path}")
        return None

    data    = json.loads(art_path.read_text())
    keyword = data.get("keyword", slug)
    print(f"\n🎙️  Podcast : {keyword}")

    # 1. Générer le script
    print("   → Génération script...")
    try:
        script = generate_script(data)
    except Exception as e:
        print(f"   ❌ Script erreur : {e}")
        return None

    lines  = parse_script(script)
    print(f"   → Script : {len(lines)} répliques")

    # Sauvegarder le script
    script_path = OUT_AUDIO / f"{slug}_script.txt"
    script_path.write_text(script)

    # 2. TTS segment par segment
    if not ELEVENLABS_KEY:
        print("   ⚠️  ELEVENLABS_API_KEY non définie — script sauvegardé uniquement")
        data["podcast"] = {
            "script_path": str(script_path),
            "audio_path":  None,
            "status":      "script_only",
            "timestamp":   datetime.now().isoformat(),
        }
        art_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        return data

    print("   → Synthèse vocale ElevenLabs...")
    segments = []
    for i, (speaker, text) in enumerate(lines):
        if not text:
            continue
        voice = VOICE_HOST if speaker == "ALEX" else VOICE_GUEST
        audio = tts_elevenlabs(text, voice)
        if audio:
            segments.append(audio)
        # Rate limit ElevenLabs : max 2 req/s
        time.sleep(0.6)
        if (i + 1) % 10 == 0:
            print(f"   → {i+1}/{len(lines)} répliques générées...")

    if not segments:
        print("   ❌ Aucun segment audio généré")
        return None

    # 3. Merge
    audio_path = OUT_AUDIO / f"{slug}.mp3"
    print(f"   → Merge {len(segments)} segments...")
    success = merge_audio_segments(segments, audio_path)

    if success:
        size_mb = audio_path.stat().st_size / 1024 / 1024
        print(f"   ✅ Audio : {audio_path.name} ({size_mb:.1f} Mo)")
    else:
        print("   ❌ Merge échoué")
        return None

    # 4. Mettre à jour l'article
    data["podcast"] = {
        "script_path": str(script_path),
        "audio_path":  str(audio_path),
        "status":      "ready",
        "duration_est": f"{len(lines) * 5 // 60} min",  # estimation
        "timestamp":   datetime.now().isoformat(),
    }
    art_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))

    return data


def generate_all_podcasts(max_items: int = 3) -> list:
    """Génère les podcasts pour tous les articles checkés sans podcast."""
    files = sorted(OUT_ARTICLES.glob("*.json"))
    done  = []
    for f in files:
        if len(done) >= max_items:
            break
        data = json.loads(f.read_text())
        if "ai_check" in data and "podcast" not in data:
            result = generate_podcast(f.stem)
            if result:
                done.append(result)
    print(f"\n✅ {len(done)} podcasts générés → {OUT_AUDIO}")
    return done


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--slug", type=str)
    parser.add_argument("--all",  action="store_true")
    parser.add_argument("--max",  type=int, default=3)
    args = parser.parse_args()

    if args.slug:
        generate_podcast(args.slug)
    elif args.all:
        generate_all_podcasts(args.max)
    else:
        print("Usage: python 06_audio.py --slug <slug> | --all [--max 3]")
