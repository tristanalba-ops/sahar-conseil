"""
Module 06 — Audio Podcast
Génère un script podcast dialogue (2 voix) depuis l'article,
puis synthèse vocale via ElevenLabs API.
Sauvegarde MP3 dans output/audio/.
"""

import os, re, json, logging, requests
from pathlib import Path

logger = logging.getLogger(__name__)

OUTPUT_AUDIO = Path(__file__).resolve().parents[1] / "output" / "audio"
OUTPUT_AUDIO.mkdir(parents=True, exist_ok=True)

# ElevenLabs — voix par défaut (IDs stables)
VOICE_HOST = os.getenv("EL_VOICE_HOST", "pNInz6obpgDQGcFmaJgB")   # Adam
VOICE_GUEST = os.getenv("EL_VOICE_GUEST", "EXAVITQu4vr4xnSDxMaL")  # Bella
EL_MODEL = "eleven_multilingual_v2"


def _generate_script_with_claude(article: dict) -> list:
    """
    Génère un script podcast dialogue (HOST / GUEST) depuis l'article.
    Retourne liste de dict {speaker, text}.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY manquant")
        return _fallback_script(article)

    h1 = article.get("h1", "")
    intro = article.get("intro", "")
    sections_text = " ".join(
        s.get("h2", "") + " " + re.sub(r"<[^>]+>", " ", s.get("content", ""))
        for s in article.get("sections", [])
    )

    prompt = f"""Transforme cet article en script de podcast conversationnel de 3 à 4 minutes.

ARTICLE : {h1}
INTRO : {intro[:400]}
CONTENU : {sections_text[:1500]}

FORMAT STRICT — réponds UNIQUEMENT en JSON :
[
  {{"speaker": "HOST", "text": "..."}},
  {{"speaker": "GUEST", "text": "..."}},
  ...
]

RÈGLES :
- HOST = journaliste qui pose les questions, guide la conversation
- GUEST = expert SAHAR qui répond avec des données concrètes
- 8 à 14 échanges
- Ton naturel, oral, dynamique — pas de lecture d'article
- Inclure 2 ou 3 chiffres/stats tirés de l'article
- Terminer par un CTA oral naturel vers sahar-conseil.fr
- Phrases courtes, rythme audio
- Pas de "comme je le disais", "en effet", "il est important"
- Durée cible : 3-4 minutes (environ 400-550 mots total)"""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 2000,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        r.raise_for_status()
        raw = r.json()["content"][0]["text"]

        raw = re.sub(r"```json\s*", "", raw)
        raw = re.sub(r"```\s*", "", raw).strip()

        script = json.loads(raw)
        logger.info(f"  → Script généré : {len(script)} répliques")
        return script

    except Exception as e:
        logger.warning(f"Script generation failed: {e}")
        return _fallback_script(article)


def _fallback_script(article: dict) -> list:
    """Script minimal si Claude indisponible."""
    h1 = article.get("h1", "Bonjour")
    return [
        {"speaker": "HOST", "text": f"Bonjour et bienvenue sur le podcast SAHAR Conseil. Aujourd'hui on parle de : {h1}."},
        {"speaker": "GUEST", "text": "Bonjour. SAHAR Conseil transforme les données publiques françaises en outils de prospection pour les professionnels."},
        {"speaker": "HOST", "text": "Merci pour cet éclairage. Pour en savoir plus, rendez-vous sur sahar-conseil.fr."},
    ]


def _tts_elevenlabs(text: str, voice_id: str, api_key: str) -> bytes:
    """Synthèse vocale via ElevenLabs."""
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
    headers = {"xi-api-key": api_key, "Content-Type": "application/json"}
    payload = {
        "text": text,
        "model_id": EL_MODEL,
        "voice_settings": {"stability": 0.5, "similarity_boost": 0.75},
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    return r.content


def _merge_audio_segments(segments: list, output_path: Path) -> bool:
    """Fusionne les segments MP3 avec pydub si disponible."""
    try:
        from pydub import AudioSegment
        import io

        combined = AudioSegment.empty()
        pause = AudioSegment.silent(duration=400)  # 400ms entre répliques

        for seg_bytes in segments:
            audio = AudioSegment.from_mp3(io.BytesIO(seg_bytes))
            combined += audio + pause

        combined.export(output_path, format="mp3", bitrate="128k")
        logger.info(f"  → Audio fusionné : {output_path.name} ({len(combined)/1000:.1f}s)")
        return True

    except ImportError:
        # Sans pydub : concaténer les bytes directement (fonctionne pour la plupart des players)
        with open(output_path, "wb") as f:
            for seg_bytes in segments:
                f.write(seg_bytes)
        logger.info(f"  → Audio concaténé (sans pydub) : {output_path.name}")
        return True

    except Exception as e:
        logger.error(f"Audio merge failed: {e}")
        return False


def generate_audio(article: dict, slug: str) -> dict:
    """
    Point d'entrée principal.
    Génère le script, TTS, sauvegarde MP3.
    Retourne dict avec chemin audio + script.
    """
    el_key = os.getenv("ELEVENLABS_API_KEY", "")

    # 1. Générer le script
    script = _generate_script_with_claude(article)

    # Sauvegarder le script JSON
    script_path = OUTPUT_AUDIO / f"{slug}_script.json"
    script_path.write_text(json.dumps(script, ensure_ascii=False, indent=2))

    # 2. TTS si ElevenLabs disponible
    audio_path = OUTPUT_AUDIO / f"{slug}.mp3"

    if not el_key:
        logger.warning("ELEVENLABS_API_KEY manquant — audio non généré, script sauvegardé")
        return {
            "slug": slug,
            "script": script,
            "script_path": str(script_path),
            "audio_path": None,
            "audio_generated": False,
            "reason": "no_elevenlabs_key",
        }

    try:
        logger.info(f"TTS: {len(script)} répliques → ElevenLabs...")
        segments = []

        for turn in script:
            speaker = turn.get("speaker", "HOST")
            text = turn.get("text", "")
            voice_id = VOICE_HOST if speaker == "HOST" else VOICE_GUEST

            audio_bytes = _tts_elevenlabs(text, voice_id, el_key)
            segments.append(audio_bytes)

        success = _merge_audio_segments(segments, audio_path)

        return {
            "slug": slug,
            "script": script,
            "script_path": str(script_path),
            "audio_path": str(audio_path) if success else None,
            "audio_generated": success,
            "nb_turns": len(script),
        }

    except Exception as e:
        logger.error(f"TTS failed: {e}")
        return {
            "slug": slug,
            "script": script,
            "script_path": str(script_path),
            "audio_path": None,
            "audio_generated": False,
            "reason": str(e),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    print("Module audio OK — nécessite ANTHROPIC_API_KEY + ELEVENLABS_API_KEY")
