#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Build an Apache Hop video-series episode from a manifest, stills, and clips.

Scene length follows the voice-over. When several languages are rendered
together, every scene lasts as long as the longest language, plus the pause
in the manifest. A crossfade sits inside that pause, so it does not cover
the start of the next sentence.

The first scene is speech, then the pause. Every later scene starts with a
silent lead-in of the same length as the crossfade. The fade overlaps that
lead-in with the previous pause, and the lead-in drops out of the timeline.
Speech starts line up with the chapter marks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests
import yaml

XAI_TTS_URL = "https://api.x.ai/v1/tts"
PUBLISH_LOCALES = ("en", "zh", "es-MX", "pt-BR", "fr", "de-DE", "it-IT", "nl-NL")
# The speech service documents de and it, not the regional tags. Dutch is not
# on the published list; nl is sent anyway and may be less reliable.
TTS_LANGUAGES = {
    "de-DE": "de",
    "it-IT": "it",
    "nl-NL": "nl",
    "zh-CN": "zh",
    "fr-FR": "fr",
    "es-ES": "es-ES",
    "pt-PT": "pt-PT",
}
MIN_WORDS = 45
MAX_WORDS = 110
DURATION_RATIO_WARN = 1.4
CAPTION_LINE_CHARS = 42
CAPTION_MAX_CHARS = CAPTION_LINE_CHARS * 2
MAX_RETRIES = 5
NON_RETRYABLE_STATUS = {400, 401, 403, 404, 405, 415, 422}
TRANSITIONS = {"fade", "fadeblack", "dissolve"}
FONT = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")
FONT_BOLD = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")

REPO_ROOT = Path(__file__).resolve().parents[2]
SERIES_ROOT = REPO_ROOT / "docs" / "video-series"


@dataclass
class Scene:
    scene_id: str
    title: str
    visual: str
    visual_type: str
    shot: str
    narration: dict[str, str]
    card_title: str | None = None
    card_subtitle: str | None = None
    trim_sec: float | None = None


@dataclass
class Video:
    path: Path
    video_id: str
    title: str
    issue: int | None
    width: int
    height: int
    fps: int
    transition: str
    transition_sec: float
    padding_sec: float
    voice_id: str
    enforce_length: bool
    links: list[dict[str, str]]
    locales: list[str] = field(default_factory=list)
    scenes: list[Scene] = field(default_factory=list)

    @property
    def output_dir(self) -> Path:
        return self.path / "output"


@dataclass
class Speech:
    locale: str
    audio_sec: float
    audio_file: Path


def die(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def run(command: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        rendered = " ".join(command)
        die(f"Command failed ({result.returncode}): {rendered}\n{result.stderr.strip()}")
    return result


def require_tool(name: str) -> str:
    found = shutil.which(name)
    if not found:
        die(f"Required tool not found on PATH: {name}")
    return found


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    if not isinstance(loaded, dict):
        die(f"{path} did not contain a YAML mapping")
    return loaded


def normalize(text: str) -> str:
    return " ".join(text.split())


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def word_count(text: str) -> int:
    return len(normalize(text).split())


def load_glossary_data(series_root: Path) -> dict[str, Any]:
    path = series_root / "glossary.yaml"
    if not path.exists():
        return {}
    return load_yaml(path)


def load_glossary(series_root: Path) -> list[tuple[str, str]]:
    data = load_glossary_data(series_root)
    rules: list[tuple[str, str]] = []
    for entry in data.get("speak_as", []) or []:
        pattern = str(entry.get("pattern", ""))
        say = str(entry.get("say", ""))
        if pattern and say:
            rules.append((pattern, say))
    rules.sort(key=lambda item: len(item[0]), reverse=True)
    return rules


def load_pronunciations(series_root: Path) -> dict[str, str]:
    """Phrases the TTS service should say differently. Values may be IPA."""
    data = load_glossary_data(series_root)
    raw = data.get("pronounce") or {}
    if not isinstance(raw, dict):
        die("glossary.yaml pronounce must be a mapping of written word to spoken form")
    pronunciations: dict[str, str] = {}
    for key, value in raw.items():
        written = str(key).strip()
        spoken = str(value).strip()
        if written and spoken:
            pronunciations[written] = spoken
    return pronunciations


def pronunciations_in(text: str, pronounce: dict[str, str]) -> dict[str, str]:
    folded = text.casefold()
    return {key: value for key, value in pronounce.items() if key.casefold() in folded}


def keep_english_terms(series_root: Path) -> list[str]:
    data = load_glossary_data(series_root)
    return [str(term) for term in data.get("keep_english", []) or []]


def warn_translated_terms(video: Video, locales: list[str], terms: list[str]) -> None:
    for scene in video.scenes:
        english = scene.narration.get("en", "")
        for locale in locales:
            if locale == "en":
                continue
            translated = scene.narration.get(locale, "")
            if not translated:
                continue
            for term in terms:
                present = re.search(rf"\b{re.escape(term)}\b", english, re.IGNORECASE)
                if present and term not in translated:
                    print(
                        f"Warning: {scene.scene_id} ({locale}) drops the English product word '{term}'.",
                        file=sys.stderr,
                    )


def apply_speak_as(text: str, rules: list[tuple[str, str]]) -> str:
    spoken = text
    for pattern, say in rules:
        spoken = spoken.replace(pattern, say)
    return spoken


def warn_unspoken_tokens(scene_id: str, locale: str, spoken: str) -> None:
    if "${" in spoken or "#{" in spoken:
        print(
            f"Warning: {scene_id} ({locale}) still has a variable token after speak-as. "
            "Add a glossary.yaml rule so the voice does not read the punctuation.",
            file=sys.stderr,
        )


def parse_video(path: Path) -> Video:
    manifest_path = path / "manifest.yaml"
    if not manifest_path.exists():
        die(f"Manifest not found: {manifest_path}")
    raw = load_yaml(manifest_path)
    block = raw.get("video")
    if not isinstance(block, dict):
        die(f"{manifest_path} needs a video: mapping")
    transition_ms = int(block.get("transition_ms", 400))
    padding = float(block.get("padding_after_sec", 0.4))
    transition_sec = transition_ms / 1000.0
    if transition_sec > padding:
        die(
            f"{manifest_path}: transition_ms ({transition_ms}) is longer than "
            f"padding_after_sec ({padding}). The fade has to sit inside the pause."
        )
    transition = str(block.get("transition", "fade"))
    if transition not in TRANSITIONS:
        die(f"Unsupported transition '{transition}'. Use one of: {', '.join(sorted(TRANSITIONS))}")

    scenes: list[Scene] = []
    seen: set[str] = set()
    for entry in raw.get("scenes", []) or []:
        scene_id = str(entry.get("id", "")).strip()
        if not scene_id:
            die(f"{manifest_path} has a scene without an id")
        if scene_id in seen:
            die(f"Duplicate scene id: {scene_id}")
        seen.add(scene_id)
        narration = entry.get("narration") or {}
        if not isinstance(narration, dict):
            die(f"Scene '{scene_id}' narration must be a mapping of locale to text")
        cleaned = {str(locale): normalize(str(text)) for locale, text in narration.items()}
        trim = entry.get("trim_sec")
        scenes.append(
            Scene(
                scene_id=scene_id,
                title=str(entry.get("title", scene_id)).strip(),
                visual=str(entry.get("visual", "")).strip(),
                visual_type=str(entry.get("visual_type", "")).strip(),
                shot=normalize(str(entry.get("shot", ""))),
                narration=cleaned,
                card_title=entry.get("card_title"),
                card_subtitle=entry.get("card_subtitle"),
                trim_sec=float(trim) if trim is not None else None,
            )
        )
    if not scenes:
        die(f"{manifest_path} has no scenes")

    links = block.get("links") or []
    return Video(
        path=path,
        video_id=str(block.get("id", path.name)),
        title=str(block.get("title", path.name)),
        issue=int(block["issue"]) if block.get("issue") is not None else None,
        width=int(block.get("width", 1920)),
        height=int(block.get("height", 1080)),
        fps=int(block.get("fps", 30)),
        transition=transition,
        transition_sec=transition_sec,
        padding_sec=padding,
        voice_id=str(block.get("voice_id", "luna")),
        enforce_length=bool(block.get("enforce_length", True)),
        links=list(links),
        locales=[str(locale) for locale in (block.get("locales") or [])],
        scenes=scenes,
    )


def resolve_video(argument: str) -> Path:
    candidate = Path(argument)
    if candidate.is_dir() and (candidate / "manifest.yaml").exists():
        return candidate.resolve()
    named = SERIES_ROOT / argument
    if named.is_dir() and (named / "manifest.yaml").exists():
        return named.resolve()
    available = []
    if SERIES_ROOT.is_dir():
        for child in sorted(SERIES_ROOT.iterdir()):
            if (child / "manifest.yaml").exists():
                available.append(child.name)
    listed = ", ".join(available) if available else "(none)"
    die(f"No video manifest for '{argument}'. Videos: {listed}")
    return named


def probe_duration(path: Path) -> float:
    result = run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ]
    )
    return float(result.stdout.strip())


def probe_size(path: Path) -> tuple[int, int]:
    result = run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height",
            "-of",
            "csv=p=0:s=x",
            str(path),
        ]
    )
    width, height = result.stdout.strip().split("x")
    return int(width), int(height)


def validate_scene_text(video: Video, locales: list[str]) -> list[str]:
    errors: list[str] = []
    for scene in video.scenes:
        if not scene.shot:
            errors.append(f"{scene.scene_id}: missing shot description")
        if scene.visual_type not in {"still", "clip"}:
            errors.append(f"{scene.scene_id}: visual_type must be still or clip")
        if not scene.visual:
            errors.append(f"{scene.scene_id}: missing visual path")
        for locale in locales:
            text = scene.narration.get(locale, "")
            if not text:
                errors.append(f"{scene.scene_id}: no narration for {locale}")
                continue
            if locale == "en" and video.enforce_length:
                count = word_count(text)
                if count < MIN_WORDS or count > MAX_WORDS:
                    errors.append(
                        f"{scene.scene_id}: English narration is {count} words "
                        f"(expected {MIN_WORDS}-{MAX_WORDS})"
                    )
    return errors


def warn_capture_size(scene_id: str, width: int, height: int, frame_width: int, frame_height: int) -> None:
    """Captures do not have to match the frame. Warn only when the fit will look poor."""
    if min(width, height) < 720:
        print(
            f"Warning: {scene_id} is {width}x{height}. "
            f"Fitting it into {frame_width}x{frame_height} will soften the text.",
            file=sys.stderr,
        )
    source_aspect = width / height
    frame_aspect = frame_width / frame_height
    if source_aspect > frame_aspect:
        bar = 1 - (frame_width / source_aspect) / frame_height
    else:
        bar = 1 - (frame_height * source_aspect) / frame_width
    if bar > 0.12:
        print(
            f"Warning: {scene_id} is {width}x{height}, so the {frame_width}x{frame_height} "
            "frame will have wide bars around it.",
            file=sys.stderr,
        )


def validate_files(video: Video) -> list[str]:
    errors: list[str] = []
    for scene in video.scenes:
        visual = video.path / scene.visual
        if not visual.exists():
            errors.append(f"{scene.scene_id}: missing file {scene.visual}")
            continue
        if scene.visual_type == "still" and visual.suffix.lower() != ".png":
            errors.append(f"{scene.scene_id}: stills are PNG files")
        if scene.visual_type == "clip" and visual.suffix.lower() not in {".mp4", ".mov", ".mkv"}:
            errors.append(f"{scene.scene_id}: clips are mp4, mov, or mkv files")
        width, height = probe_size(visual)
        warn_capture_size(scene.scene_id, width, height, video.width, video.height)
    return errors


def fit_filter(width: int, height: int) -> str:
    """Scale a capture up or down so it fits inside the frame, then pad."""
    return (
        f"scale=w='if(gte(a\\,{width}/{height})\\,{width}\\,-1)':"
        f"h='if(gte(a\\,{width}/{height})\\,-1\\,{height})':flags=lanczos,"
        f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:black,setsar=1"
    )


def publish_locales(video: Video) -> list[str]:
    if video.locales:
        return list(video.locales)
    return list(PUBLISH_LOCALES)


def cmd_validate(video: Video, *, publish: bool) -> None:
    require_tool("ffprobe")
    locales = publish_locales(video) if publish else ["en"]
    errors = validate_scene_text(video, locales) + validate_files(video)
    if errors:
        die("Manifest is not ready:\n- " + "\n- ".join(errors))
    if video.path.is_relative_to(SERIES_ROOT):
        warn_translated_terms(video, locales, keep_english_terms(SERIES_ROOT))
    mode = "publish" if publish else "preview"
    print(f"Valid ({mode}): {video.video_id} — {len(video.scenes)} scenes, locales {', '.join(locales)}")


def cmd_shotlist(video: Video) -> None:
    lines = [
        f"# Shot list: {video.title}",
        "",
        f"Capture the Hop window as large as you can. It does not have to be exactly "
        f"{video.width}x{video.height}. The renderer fits each picture into that frame.",
        "No secrets, no personal paths, no bookmarks, no notification popups.",
        "Clips are muted. Record only the action, at 30 fps.",
        "",
    ]
    missing = 0
    for index, scene in enumerate(video.scenes, start=1):
        visual = video.path / scene.visual
        state = "ready" if visual.exists() else "missing"
        if state == "missing":
            missing += 1
        lines.append(f"## {index}. {scene.title}")
        lines.append("")
        lines.append(f"- Id: `{scene.scene_id}`")
        lines.append(f"- Type: {scene.visual_type}")
        lines.append(f"- File: `{scene.visual}` ({state})")
        if scene.card_title:
            lines.append(f"- Card: {scene.card_title}")
        lines.append(f"- Show: {scene.shot}")
        english = scene.narration.get("en", "")
        if english:
            lines.append(f"- Say: {english}")
        lines.append("")
    video.output_dir.mkdir(parents=True, exist_ok=True)
    destination = video.output_dir / "shotlist.md"
    destination.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {destination}")
    if missing:
        print(f"{missing} file(s) still to capture.")
    else:
        print("Every visual is on disk.")


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def generate_card(video: Video, scene: Scene, destination: Path) -> None:
    if not FONT.is_file() or not FONT_BOLD.is_file():
        die(f"DejaVu fonts not found at {FONT}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        title_file = folder / "title.txt"
        subtitle_file = folder / "subtitle.txt"
        kicker_file = folder / "kicker.txt"
        write_text(title_file, scene.card_title or scene.title)
        write_text(subtitle_file, scene.card_subtitle or "")
        write_text(kicker_file, "Apache Hop")
        run(
            [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                f"color=c=0x0E3A5A:s={video.width}x{video.height}:r=1",
                "-vf",
                (
                    f"drawtext=fontfile={FONT}:textfile={kicker_file}:fontsize=32:"
                    "fontcolor=white@0.75:x=(w-text_w)/2:y=h*0.38,"
                    f"drawtext=fontfile={FONT_BOLD}:textfile={title_file}:fontsize=72:"
                    "fontcolor=white:x=(w-text_w)/2:y=(h-text_h)/2,"
                    f"drawtext=fontfile={FONT}:textfile={subtitle_file}:fontsize=36:"
                    "fontcolor=white@0.9:x=(w-text_w)/2:y=h*0.60"
                ),
                "-frames:v",
                "1",
                "-update",
                "1",
                str(destination),
            ]
        )


def cmd_cards(video: Video, *, force: bool) -> None:
    require_tool("ffmpeg")
    written = 0
    for scene in video.scenes:
        if not scene.card_title:
            continue
        destination = video.path / scene.visual
        if destination.exists() and not force:
            print(f"Keep existing card {scene.visual}")
            continue
        generate_card(video, scene, destination)
        written += 1
        print(f"Wrote card {scene.visual}")
    if written == 0:
        print("No cards to write.")


def require_api_key() -> str:
    api_key = os.environ.get("XAI_API_KEY", "")
    api_key = api_key.strip().strip('"').strip("'")
    if not api_key:
        die(
            "XAI_API_KEY is not set. Export a key from https://console.x.ai "
            "before generating voice-overs. Use --silence to exercise the renderer without speech."
        )
    return api_key


def tts_language(locale: str) -> str:
    return TTS_LANGUAGES.get(locale, locale)


def tts_payload(
    text: str,
    *,
    voice: str,
    language: str,
    pronounce: dict[str, str] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "text": text,
        "voice_id": voice,
        "language": tts_language(language),
    }
    used = pronunciations_in(text, pronounce or {})
    if used:
        payload["replace"] = used
    return payload


def synthesize_tts(
    text: str,
    *,
    voice: str,
    language: str,
    destination: Path,
    api_key: str,
    pronounce: dict[str, str] | None = None,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = tts_payload(text, voice=voice, language=language, pronounce=pronounce)
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "*/*",
    }
    last_error = "no response"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(XAI_TTS_URL, headers=headers, json=payload, timeout=120)
        except requests.RequestException as exc:
            last_error = str(exc)
            response = None
        if response is not None:
            if response.status_code in NON_RETRYABLE_STATUS:
                die(f"xAI TTS failed: HTTP {response.status_code}: {response.text[:500]}")
            if response.ok and response.content:
                destination.write_bytes(response.content)
                return
            last_error = f"HTTP {response.status_code}: {response.text[:300]}"
        if attempt == MAX_RETRIES:
            break
        print(f"TTS retry {attempt} for {destination.name}: {last_error}", file=sys.stderr)
    die(f"xAI TTS failed after {MAX_RETRIES} attempts: {last_error}")


def synthesize_silence(duration: float, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    run(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "anullsrc=r=48000:cl=stereo",
            "-t",
            f"{duration:.3f}",
            "-c:a",
            "pcm_s16le",
            str(destination),
        ]
    )


def audio_cache_path(
    video: Video,
    locale: str,
    scene: Scene,
    spoken: str,
    *,
    silence: bool,
    pronounce: dict[str, str] | None = None,
) -> Path:
    suffix = "wav" if silence else "mp3"
    used = pronunciations_in(spoken, pronounce or {})
    hashed = spoken
    if used:
        hashed = spoken + "\n" + json.dumps(used, ensure_ascii=False, sort_keys=True)
    return video.output_dir / "audio" / locale / f"{scene.scene_id}-{text_hash(hashed)}.{suffix}"


def cmd_speak(
    video: Video,
    locales: list[str],
    *,
    limit: int | None,
    force: bool,
    silence: bool,
    rules: list[tuple[str, str]],
    pronounce: dict[str, str] | None = None,
) -> dict[str, dict[str, Speech]]:
    """Return locale -> scene id -> speech."""
    require_tool("ffmpeg")
    require_tool("ffprobe")
    api_key = None if silence else require_api_key()
    spoken_by_scene: dict[str, dict[str, Speech]] = {locale: {} for locale in locales}
    scenes = video.scenes if limit is None else video.scenes[:limit]
    for scene in scenes:
        for locale in locales:
            text = scene.narration.get(locale, "")
            if not text:
                die(f"Scene '{scene.scene_id}' has no narration for {locale}")
            spoken = apply_speak_as(text, rules)
            warn_unspoken_tokens(scene.scene_id, locale, spoken)
            destination = audio_cache_path(
                video, locale, scene, spoken, silence=silence, pronounce=pronounce
            )
            if force or not destination.exists():
                if silence:
                    seconds = max(2.0, word_count(text) / 2.5)
                    print(f"Silence {locale} {scene.scene_id} ({seconds:.1f}s)")
                    synthesize_silence(seconds, destination)
                else:
                    print(f"TTS {locale} {scene.scene_id} voice={video.voice_id}")
                    assert api_key is not None
                    synthesize_tts(
                        spoken,
                        voice=video.voice_id,
                        language=locale,
                        destination=destination,
                        api_key=api_key,
                        pronounce=pronounce,
                    )
            else:
                print(f"Cached {locale} {scene.scene_id}")
            spoken_by_scene[locale][scene.scene_id] = Speech(
                locale=locale,
                audio_sec=probe_duration(destination),
                audio_file=destination,
            )
    return spoken_by_scene


def visible_duration(speech_secs: list[float], padding: float) -> float:
    return max(speech_secs) + padding


def clip_playback_sec(scene: Scene, visual: Path) -> float | None:
    """Seconds of the recording to keep. A set trim_sec is a cap. Otherwise the whole file."""
    if scene.visual_type != "clip" or not visual.is_file():
        return None
    if scene.trim_sec is not None:
        return scene.trim_sec
    return probe_duration(visual)


def scene_length(speech_secs: list[float], padding: float, clip_sec: float | None) -> float:
    """Voice-over length, or the full clip when the recording runs longer. The pause stays at the end."""
    spoken = visible_duration(speech_secs, padding)
    if clip_sec is None:
        return spoken
    return max(spoken, clip_sec + padding)


def speech_starts(visible: list[float]) -> list[float]:
    starts: list[float] = []
    cursor = 0.0
    for duration in visible:
        starts.append(cursor)
        cursor += duration
    return starts


def cmd_align(
    video: Video,
    spoken: dict[str, dict[str, Speech]],
    locales: list[str],
    *,
    limit: int | None,
) -> list[dict[str, Any]]:
    scenes = video.scenes if limit is None else video.scenes[:limit]
    rows: list[dict[str, Any]] = []
    for scene in scenes:
        per_locale: dict[str, Any] = {}
        lengths: list[float] = []
        for locale in locales:
            speech = spoken[locale][scene.scene_id]
            lengths.append(speech.audio_sec)
            per_locale[locale] = {
                "audio_sec": round(speech.audio_sec, 3),
                "audio_file": str(speech.audio_file.relative_to(video.output_dir)),
            }
        longest = max(lengths)
        shortest = min(lengths)
        if shortest > 0 and longest / shortest > DURATION_RATIO_WARN:
            print(
                f"Warning: {scene.scene_id} languages differ by {longest / shortest:.2f}x "
                f"({shortest:.1f}s to {longest:.1f}s). Shorten the long translation.",
                file=sys.stderr,
            )
        spoken_sec = visible_duration(lengths, video.padding_sec)
        clip_sec = clip_playback_sec(scene, video.path / scene.visual)
        length = scene_length(lengths, video.padding_sec, clip_sec)
        if clip_sec is not None and length > spoken_sec + 0.05:
            print(
                f"{scene.scene_id}: clip is {clip_sec - longest:.1f}s longer than the voice-over. "
                "The picture continues in silence."
            )
        rows.append(
            {
                "id": scene.scene_id,
                "title": scene.title,
                "scene_sec": round(length, 3),
                "speech": per_locale,
            }
        )
    payload = {
        "video_id": video.video_id,
        "locales": locales,
        "padding_after_sec": video.padding_sec,
        "transition_sec": video.transition_sec,
        "scenes": rows,
    }
    video.output_dir.mkdir(parents=True, exist_ok=True)
    destination = video.output_dir / "timings.yaml"
    with destination.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)
    print(f"Wrote {destination}")
    return rows


def format_timestamp(seconds: float) -> str:
    whole = int(seconds)
    millis = int(round((seconds - whole) * 1000))
    if millis == 1000:
        whole += 1
        millis = 0
    hours, remainder = divmod(whole, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}.{millis:03d}"


def format_chapter(seconds: float) -> str:
    whole = int(seconds)
    hours, remainder = divmod(whole, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def pack_pieces(pieces: list[str], limit: int, joiner: str) -> list[str]:
    chunks: list[str] = []
    current = ""
    for piece in pieces:
        piece = piece.strip()
        if not piece:
            continue
        candidate = piece if not current else f"{current}{joiner}{piece}"
        if current and len(candidate) <= limit:
            current = candidate
            continue
        if current:
            chunks.append(current)
        if len(piece) <= limit:
            current = piece
            continue
        current = ""
        if " " in piece:
            chunks.extend(pack_pieces(piece.split(), limit, " "))
        else:
            chunks.extend(piece[index : index + limit] for index in range(0, len(piece), limit))
    if current:
        chunks.append(current)
    return chunks


def absorb_short_chunks(chunks: list[str], limit: int, min_chars: int = 18) -> list[str]:
    """Keep a leftover fragment with the sentence it came from."""
    if not chunks:
        return []
    result = [chunks[0]]
    for chunk in chunks[1:]:
        if len(chunk) >= min_chars:
            result.append(chunk)
            continue
        combined = f"{result[-1]} {chunk}".strip()
        if len(combined) <= limit:
            result[-1] = combined
            continue
        words = combined.split()
        split_at = None
        best_score = -1
        for index in range(1, len(words)):
            left = " ".join(words[:index])
            right = " ".join(words[index:])
            if len(left) > limit or len(right) > limit or len(right) < min_chars:
                continue
            score = min(len(left), len(right))
            if score > best_score:
                best_score = score
                split_at = (left, right)
        if split_at is None:
            result.append(chunk)
            continue
        result[-1] = split_at[0]
        result.append(split_at[1])
    return result


def split_caption(text: str, limit: int = CAPTION_MAX_CHARS) -> list[str]:
    """Break a scene into cues of at most two short lines."""
    text = normalize(text)
    if not text:
        return []
    # A period only ends a sentence when whitespace follows, so hop.apache.org stays whole.
    # Chinese full stops often have no space after them.
    sentences = [
        part.strip()
        for part in re.split(r"(?<=[。！？])|(?<=[.!?])(?:\s+|(?=\Z))", text)
        if part.strip()
    ]
    chunks: list[str] = []
    for sentence in sentences:
        if len(sentence) <= limit:
            chunks.append(sentence)
            continue
        clauses = [part for part in re.split(r"(?<=[,;:，；、])\s*", sentence) if part]
        if len(clauses) > 1:
            chunks.extend(pack_pieces(clauses, limit, " "))
            continue
        words = sentence.split()
        if len(words) > 1:
            chunks.extend(pack_pieces(words, limit, " "))
            continue
        chunks.extend(sentence[index : index + limit] for index in range(0, len(sentence), limit))
    return absorb_short_chunks(chunks, limit)


def format_cue_text(text: str, width: int = CAPTION_LINE_CHARS) -> str:
    if " " not in text:
        return "\n".join(text[index : index + width] for index in range(0, len(text), width))
    return textwrap.fill(text, width=width)


def caption_cues(text: str, *, start: float, duration: float) -> list[tuple[float, float, str]]:
    chunks = split_caption(text)
    if not chunks:
        return []
    weights = [max(len(chunk), 1) for chunk in chunks]
    total = sum(weights)
    cursor = start
    cues: list[tuple[float, float, str]] = []
    for index, chunk in enumerate(chunks):
        if index == len(chunks) - 1:
            end = start + duration
        else:
            end = cursor + duration * weights[index] / total
        cues.append((cursor, end, format_cue_text(chunk)))
        cursor = end
    return cues


def write_vtt(video: Video, rows: list[dict[str, Any]], locale: str) -> None:
    starts = speech_starts([float(row["scene_sec"]) for row in rows])
    lines = ["WEBVTT", ""]
    cue_count = 0
    for row, start in zip(rows, starts):
        speech = row["speech"][locale]
        scene = next(scene for scene in video.scenes if scene.scene_id == row["id"])
        for cue_start, cue_end, cue_text in caption_cues(
            scene.narration[locale],
            start=start,
            duration=float(speech["audio_sec"]),
        ):
            lines.append(f"{format_timestamp(cue_start)} --> {format_timestamp(cue_end)}")
            lines.append(cue_text)
            lines.append("")
            cue_count += 1
    destination = video.output_dir / f"{video.video_id}-{locale}.vtt"
    destination.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {destination} ({cue_count} cues)")


def cmd_captions(video: Video) -> None:
    """Rewrite subtitle files from the last timings, without speaking or encoding."""
    timings_path = video.output_dir / "timings.yaml"
    if not timings_path.exists():
        die(f"No timings to caption from: {timings_path}")
    payload = load_yaml(timings_path)
    rows = payload.get("scenes") or []
    locales = list(payload.get("locales") or [])
    if not rows or not locales:
        die(f"{timings_path} has no scenes or locales")
    for locale in locales:
        if any(locale not in scene.narration for scene in video.scenes):
            die(f"Manifest is missing narration for {locale}")
        write_vtt(video, rows, locale)
    write_description(video, rows)


def load_descriptions(video: Video) -> dict[str, Any]:
    path = video.path / "descriptions.yaml"
    if not path.exists():
        return {}
    return load_yaml(path)


def localized(block: Any, locale: str, fallback: str = "en") -> str | None:
    if not isinstance(block, dict):
        return None
    if locale in block and block[locale]:
        return str(block[locale]).strip()
    short = locale.split("-")[0]
    if short in block and block[short]:
        return str(block[short]).strip()
    if fallback in block and block[fallback]:
        return str(block[fallback]).strip()
    return None


def description_locales(video: Video, rows: list[dict[str, Any]] | None) -> list[str]:
    if video.locales:
        return list(video.locales)
    if rows and isinstance(rows[0].get("speech"), dict):
        return list(rows[0]["speech"].keys())
    return ["en"]


def write_description(video: Video, rows: list[dict[str, Any]] | None) -> None:
    catalog = load_descriptions(video)
    video.output_dir.mkdir(parents=True, exist_ok=True)
    starts = speech_starts([float(row["scene_sec"]) for row in rows]) if rows else []
    for locale in description_locales(video, rows):
        title = localized(catalog.get("title"), locale) or video.title
        summary = localized(catalog.get("summary"), locale) or "Part of the Apache Hop getting started series."
        series = localized(catalog.get("series"), locale) or "Part of the Apache Hop getting started series."
        chapters_label = localized(catalog.get("chapters_label"), locale) or "Chapters"
        paragraphs = [title, "", summary, "", series, "https://hop.apache.org/", ""]
        link_labels = catalog.get("link_labels") or {}
        for link in video.links:
            label = localized(link_labels.get(link.get("label")), locale) or link.get("label") or link.get("url", "")
            paragraphs.append(f"{label}: {link.get('url', '')}")
        if video.issue is not None:
            issue_label = localized(catalog.get("issue_label"), locale) or "Issue"
            paragraphs.append(f"{issue_label}: https://github.com/apache/hop/issues/{video.issue}")
        paragraphs.append("")
        if rows:
            paragraphs.append(chapters_label)
            chapters = catalog.get("chapters") or {}
            for row, start in zip(rows, starts):
                chapter = localized(chapters.get(row["id"]), locale) or row["title"]
                paragraphs.append(f"{format_chapter(start)} {chapter}")
            paragraphs.append("")
        destination = video.output_dir / f"youtube-description-{locale}.txt"
        destination.write_text("\n".join(paragraphs) + "\n", encoding="utf-8")
        print(f"Wrote {destination}")
        if locale == "en":
            english = video.output_dir / "youtube-description.txt"
            english.write_text("\n".join(paragraphs) + "\n", encoding="utf-8")
            print(f"Wrote {english}")


def build_watermark(width: int) -> Path:
    if not FONT.is_file():
        die(f"DejaVu font not found at {FONT}")
    folder = SERIES_ROOT / "output"
    folder.mkdir(parents=True, exist_ok=True)
    destination = folder / "watermark.png"
    if destination.exists():
        return destination
    with tempfile.TemporaryDirectory() as tmp:
        text_file = Path(tmp) / "mark.txt"
        write_text(text_file, "Apache Hop · ASF")
        run(
            [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                f"color=c=black@0.0:s={width}x64:r=1,format=rgba",
                "-vf",
                (
                    f"drawtext=fontfile={FONT}:textfile={text_file}:fontsize=28:"
                    "fontcolor=white:shadowcolor=black@0.65:shadowx=1:shadowy=1:"
                    "x=8:y=(h-text_h)/2"
                ),
                "-frames:v",
                "1",
                "-update",
                "1",
                str(destination),
            ]
        )
    return destination


def fit_audio(source: Path, destination: Path, *, duration: float, lead_sec: float) -> None:
    lead_ms = int(round(lead_sec * 1000))
    run(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(source),
            "-af",
            (
                "aformat=sample_rates=48000:channel_layouts=stereo,"
                f"adelay={lead_ms}|{lead_ms},"
                f"apad=whole_dur={duration:.3f},atrim=duration={duration:.3f}"
            ),
            "-c:a",
            "pcm_s16le",
            str(destination),
        ]
    )


def encode_scene(
    video: Video,
    scene: Scene,
    audio: Path,
    destination: Path,
    *,
    duration: float,
    watermark: Path,
) -> None:
    visual = video.path / scene.visual
    if not visual.exists():
        die(f"Missing visual for {scene.scene_id}: {visual}")
    hold = "tpad=stop_mode=clone:stop_duration=600,"
    trim = f"trim=duration={duration:.3f},setpts=PTS-STARTPTS"
    if scene.visual_type == "still":
        video_input = ["-loop", "1", "-framerate", str(video.fps), "-t", f"{duration:.3f}", "-i", str(visual)]
        base = f"[0:v]{fit_filter(video.width, video.height)},fps={video.fps},{trim}[base]"
    else:
        source_trim = ""
        if scene.trim_sec is not None:
            source_trim = f"trim=duration={scene.trim_sec:.3f},setpts=PTS-STARTPTS,"
        video_input = ["-i", str(visual)]
        base = (
            f"[0:v]{source_trim}{fit_filter(video.width, video.height)},"
            f"fps={video.fps},{hold}{trim}[base]"
        )
    run(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            *video_input,
            "-i",
            str(audio),
            "-i",
            str(watermark),
            "-filter_complex",
            (
                f"{base};"
                "[2:v]scale=-1:36,format=rgba,colorchannelmixer=aa=0.75[wm];"
                "[base][wm]overlay=W-w-28:H-h-22:format=auto,format=yuv420p[v]"
            ),
            "-map",
            "[v]",
            "-map",
            "1:a",
            "-t",
            f"{duration:.3f}",
            "-r",
            str(video.fps),
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "18",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-ar",
            "48000",
            "-ac",
            "2",
            str(destination),
        ]
    )


def probe_stream_duration(path: Path, selector: str) -> float:
    result = run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            selector,
            "-show_entries",
            "stream=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ]
    )
    text = result.stdout.strip().splitlines()
    if not text or text[0] in {"", "N/A"}:
        return probe_duration(path)
    return float(text[0])


def usable_scene_duration(path: Path) -> float:
    """The stretch both the picture and the sound actually contain."""
    picture = probe_stream_duration(path, "v:0")
    sound = probe_stream_duration(path, "a:0")
    if picture <= 0 or sound <= 0:
        return probe_duration(path)
    return min(picture, sound)


def xfade_offset(durations: list[float], transition: float, index: int) -> float:
    """Offset at which scene `index` (1-based) begins its fade in."""
    return sum(durations[:index]) - index * transition


def concat_scenes(video: Video, scene_files: list[Path], destination: Path) -> None:
    """Join scenes with a short crossfade.

    A chained xfade on the whole episode buffers every frame until the fade
    offset, which is the length of the film. That stalls the encode. Each fade
    here is only the overlap, a fraction of a second, and the rest is concatenated.
    """
    if len(scene_files) == 1:
        shutil.copyfile(scene_files[0], destination)
        return
    durations = [usable_scene_duration(path) for path in scene_files]
    transition = video.transition_sec
    if any(duration <= transition * 2 for duration in durations):
        die("A scene is shorter than the crossfade. Lengthen the narration or lower transition_ms.")
    parts: list[str] = []
    video_labels: list[str] = []
    audio_labels: list[str] = []

    def trim_stream(index: int, kind: str, start: float, end: float, label: str) -> None:
        if kind == "v":
            parts.append(
                f"[{index}:v]trim=start={start:.3f}:end={end:.3f},setpts=PTS-STARTPTS[{label}]"
            )
        else:
            parts.append(
                f"[{index}:a]atrim=start={start:.3f}:end={end:.3f},asetpts=PTS-STARTPTS[{label}]"
            )

    trim_stream(0, "v", 0, durations[0] - transition, "vhead")
    trim_stream(0, "a", 0, durations[0] - transition, "ahead")
    video_labels.append("vhead")
    audio_labels.append("ahead")
    for index in range(len(scene_files) - 1):
        trim_stream(index, "v", durations[index] - transition, durations[index], f"vt{index}")
        trim_stream(index, "a", durations[index] - transition, durations[index], f"at{index}")
        trim_stream(index + 1, "v", 0, transition, f"vh{index}")
        trim_stream(index + 1, "a", 0, transition, f"ah{index}")
        parts.append(
            f"[vt{index}][vh{index}]xfade=transition={video.transition}:"
            f"duration={transition:.3f}:offset=0[vx{index}]"
        )
        parts.append(
            f"[at{index}][ah{index}]acrossfade=d={transition:.3f}:c1=tri:c2=tri[ax{index}]"
        )
        video_labels.append(f"vx{index}")
        audio_labels.append(f"ax{index}")
        body_start = transition
        body_end = durations[index + 1] if index + 1 == len(scene_files) - 1 else durations[index + 1] - transition
        trim_stream(index + 1, "v", body_start, body_end, f"vb{index}")
        trim_stream(index + 1, "a", body_start, body_end, f"ab{index}")
        video_labels.append(f"vb{index}")
        audio_labels.append(f"ab{index}")
    pairs = "".join(f"[{video_label}][{audio_label}]" for video_label, audio_label in zip(video_labels, audio_labels))
    parts.append(f"{pairs}concat=n={len(video_labels)}:v=1:a=1[v][a]")
    inputs: list[str] = []
    for path in scene_files:
        inputs.extend(["-i", str(path)])
    run(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            *inputs,
            "-filter_complex",
            ";".join(parts),
            "-map",
            "[v]",
            "-map",
            "[a]",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "18",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-ar",
            "48000",
            "-ac",
            "2",
            "-movflags",
            "+faststart",
            str(destination),
        ]
    )


def render_locale(
    video: Video,
    rows: list[dict[str, Any]],
    locale: str,
    *,
    watermark: Path,
) -> Path:
    scenes = {scene.scene_id: scene for scene in video.scenes}
    with tempfile.TemporaryDirectory(prefix="hop-video-") as tmp:
        folder = Path(tmp)
        scene_files: list[Path] = []
        for index, row in enumerate(rows):
            scene = scenes[row["id"]]
            speech = row["speech"][locale]
            lead = 0.0 if index == 0 else video.transition_sec
            duration = float(row["scene_sec"]) + lead
            audio = folder / f"{index:02d}.wav"
            picture = folder / f"{index:02d}.mp4"
            source = video.output_dir / speech["audio_file"]
            fit_audio(source, audio, duration=duration, lead_sec=lead)
            encode_scene(video, scene, audio, picture, duration=duration, watermark=watermark)
            scene_files.append(picture)
        destination = video.output_dir / f"{video.video_id}-{locale}.mp4"
        print(f"Joining {destination.name}")
        concat_scenes(video, scene_files, destination)
    return destination


def cmd_render(
    video: Video,
    locales: list[str],
    *,
    limit: int | None,
    audio_only: bool,
    video_only: bool,
    force: bool,
    silence: bool,
) -> None:
    require_tool("ffmpeg")
    require_tool("ffprobe")
    glossary_root = SERIES_ROOT if video.path.is_relative_to(SERIES_ROOT) else video.path.parent
    rules = load_glossary(glossary_root)
    pronounce = load_pronunciations(glossary_root)
    text_errors = validate_scene_text(video, locales)
    if text_errors:
        die("Manifest text is not ready:\n- " + "\n- ".join(text_errors))
    if not audio_only:
        file_errors = validate_files(video if limit is None else limited_view(video, limit))
        if file_errors:
            die("Visuals are not ready:\n- " + "\n- ".join(file_errors))

    if video_only:
        timings_path = video.output_dir / "timings.yaml"
        if not timings_path.exists():
            die(f"No timings to render from: {timings_path}")
        payload = load_yaml(timings_path)
        rows = payload["scenes"]
        if limit is not None:
            rows = rows[:limit]
    else:
        spoken = cmd_speak(
            video,
            locales,
            limit=limit,
            force=force,
            silence=silence,
            rules=rules,
            pronounce=pronounce,
        )
        rows = cmd_align(video, spoken, locales, limit=limit)

    write_description(video, rows)
    for locale in locales:
        write_vtt(video, rows, locale)
    if audio_only:
        print("Audio only. No MP4 written.")
        return

    watermark = build_watermark(420)
    for locale in locales:
        destination = render_locale(video, rows, locale, watermark=watermark)
        print(f"Wrote {destination} ({probe_duration(destination):.2f}s)")


def limited_view(video: Video, limit: int) -> Video:
    clone = Video(
        path=video.path,
        video_id=video.video_id,
        title=video.title,
        issue=video.issue,
        width=video.width,
        height=video.height,
        fps=video.fps,
        transition=video.transition,
        transition_sec=video.transition_sec,
        padding_sec=video.padding_sec,
        voice_id=video.voice_id,
        locales=list(video.locales),
        enforce_length=video.enforce_length,
        links=video.links,
        scenes=video.scenes[:limit],
    )
    return clone


def cmd_voicesample(text: str) -> None:
    require_tool("ffprobe")
    api_key = require_api_key()
    pronounce = load_pronunciations(SERIES_ROOT)
    destination_dir = SERIES_ROOT / "output" / "voice-samples"
    destination_dir.mkdir(parents=True, exist_ok=True)
    for voice in ("luna", "orion"):
        destination = destination_dir / f"{voice}.mp3"
        print(f"Synthesizing {voice}")
        synthesize_tts(
            text,
            voice=voice,
            language="en",
            destination=destination,
            api_key=api_key,
            pronounce=pronounce,
        )
        print(f"Wrote {destination} ({probe_duration(destination):.1f}s)")
    print("Listen to luna and orion, then set video.voice_id in the manifest.")


def self_test() -> None:
    require_tool("ffmpeg")
    require_tool("ffprobe")
    assert speech_starts([10.0, 8.0, 12.0]) == [0.0, 10.0, 18.0]
    assert scene_length([10.0], 0.4, None) == 10.4
    assert scene_length([10.0], 0.4, 35.0) == 35.4
    assert scene_length([40.0], 0.4, 10.0) == 40.4
    assert abs(xfade_offset([10.0, 10.4], 0.4, 1) - 9.6) < 0.001
    spoken = apply_speak_as("Run hop-run with ${PROJECT_HOME}.", [("hop-run", "hop run"), ("${PROJECT_HOME}", "the PROJECT_HOME variable")])
    assert "${" not in spoken
    assert "hop run" in spoken
    gooey = "/ˈɡuːi/"
    payload = tts_payload(
        "Open Hop GUI.",
        voice="orion",
        language="en",
        pronounce={"GUI": gooey},
    )
    assert payload["replace"] == {"GUI": gooey}
    assert "replace" not in tts_payload("A pipeline.", voice="orion", language="en", pronounce={"GUI": gooey})
    sample = (
        "Everything you build in Hop is metadata. A pipeline that reads and writes "
        "data is metadata. A workflow that runs that pipeline, and decides what to "
        "do when it fails, is metadata."
    )
    cues = caption_cues(sample, start=10.0, duration=20.0)
    assert len(cues) >= 3
    assert cues[0][0] == 10.0
    assert abs(cues[-1][1] - 30.0) < 0.001
    for earlier, later in zip(cues, cues[1:]):
        assert abs(earlier[1] - later[0]) < 0.001
        assert earlier[2].count("\n") <= 1
        for line in earlier[2].split("\n"):
            assert len(line) <= CAPTION_LINE_CHARS
    assert all(cue[2].strip() != "is metadata." for cue in cues)
    url_cues = split_caption("The manual for this series lives at hop.apache.org.")
    assert any("hop.apache.org" in cue for cue in url_cues)
    plain = text_hash("Open Hop GUI.")
    marked = text_hash("Open Hop GUI.\n" + json.dumps({"GUI": gooey}, ensure_ascii=False, sort_keys=True))
    assert plain != marked

    with tempfile.TemporaryDirectory(prefix="hop-video-self-test-") as tmp:
        root = Path(tmp)
        video_dir = root / "fixture"
        (video_dir / "images").mkdir(parents=True)
        (video_dir / "clips").mkdir(parents=True)
        glossary = root / "glossary.yaml"
        glossary.write_text(
            "speak_as:\n  - pattern: hop-run\n    say: hop run\n",
            encoding="utf-8",
        )
        still = video_dir / "images" / "one.png"
        clip = video_dir / "clips" / "two.mp4"
        run(
            [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "color=c=0x0E3A5A:s=1600x900:r=1",
                "-frames:v",
                "1",
                "-update",
                "1",
                str(still),
            ]
        )
        run(
            [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "color=c=0x1F6F4A:s=1280x720:r=30",
                "-t",
                "1",
                "-c:v",
                "libx264",
                "-pix_fmt",
                "yuv420p",
                str(clip),
            ]
        )
        manifest = textwrap.dedent(
            """
            video:
              id: fixture
              title: Fixture
              width: 1920
              height: 1080
              fps: 30
              transition: fade
              transition_ms: 400
              padding_after_sec: 0.4
              voice_id: luna
              enforce_length: false
            scenes:
              - id: one
                title: One
                visual: images/one.png
                visual_type: still
                shot: A blue card.
                narration:
                  en: This is the first fixture scene for the renderer.
              - id: two
                title: Two
                visual: clips/two.mp4
                visual_type: clip
                shot: A short green clip that must be held.
                narration:
                  en: This second fixture scene is longer than the clip so the last frame has to hold.
            """
        ).lstrip()
        (video_dir / "manifest.yaml").write_text(manifest, encoding="utf-8")
        video = parse_video(video_dir)
        errors = validate_scene_text(video, ["en"]) + validate_files(video)
        if errors:
            die("Self-test manifest failed:\n- " + "\n- ".join(errors))
        rules = load_glossary(root)
        spoken = cmd_speak(video, ["en"], limit=None, force=True, silence=True, rules=rules)
        rows = cmd_align(video, spoken, ["en"], limit=None)
        write_vtt(video, rows, "en")
        # The self-test watermark is local, so card text generation is not required.
        watermark = video_dir / "watermark.png"
        with tempfile.TemporaryDirectory() as mark_tmp:
            text_file = Path(mark_tmp) / "mark.txt"
            write_text(text_file, "Apache Hop · ASF")
            run(
                [
                    "ffmpeg",
                    "-y",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-f",
                    "lavfi",
                    "-i",
                    "color=c=black@0.0:s=420x64:r=1,format=rgba",
                    "-vf",
                    (
                        f"drawtext=fontfile={FONT}:textfile={text_file}:fontsize=28:"
                        "fontcolor=white:x=8:y=(h-text_h)/2"
                    ),
                    "-frames:v",
                    "1",
                    "-update",
                    "1",
                    str(watermark),
                ]
            )
        destination = render_locale(video, rows, "en", watermark=watermark)
        expected = sum(float(row["scene_sec"]) for row in rows)
        actual = probe_duration(destination)
        width, height = probe_size(destination)
        if width != 1920 or height != 1080:
            die(f"Self-test output is {width}x{height}")
        if abs(actual - expected) > 0.35:
            die(f"Self-test duration {actual:.3f}s, expected about {expected:.3f}s")
        print(f"Self-test ok: {destination.name} {actual:.2f}s (timeline {expected:.2f}s)")


def parse_locales(value: str | None, *, publish: bool, video: Video) -> list[str]:
    if publish:
        return publish_locales(video)
    if not value:
        return ["en"]
    return [item.strip() for item in value.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    validate = sub.add_parser("validate", help="Check narration, files, and frame size")
    validate.add_argument("video")
    validate.add_argument("--publish", action="store_true", help="Require every publish locale")

    shotlist = sub.add_parser("shotlist", help="Write the capture checklist")
    shotlist.add_argument("video")

    cards = sub.add_parser("cards", help="Write title-card PNGs for scenes that have card_title")
    cards.add_argument("video")
    cards.add_argument("--force", action="store_true")

    render = sub.add_parser("render", help="Speak, align, and write the MP4")
    render.add_argument("video")
    render.add_argument("--locales", help="Comma-separated locales (default: en)")
    render.add_argument(
        "--publish",
        action="store_true",
        help="Render en, zh, es-MX, pt-BR, fr, de-DE, it-IT, and nl-NL",
    )
    render.add_argument("--limit", type=int)
    render.add_argument("--audio-only", action="store_true")
    render.add_argument("--video-only", action="store_true")
    render.add_argument("--force-audio", action="store_true")
    render.add_argument("--silence", action="store_true", help="Skip TTS and use timed silence")

    captions = sub.add_parser("captions", help="Rewrite VTT cues from the last timings")
    captions.add_argument("video")

    sample = sub.add_parser("voicesample", help="Synthesize luna and orion for the same paragraph")
    sample.add_argument("--text", required=True)

    sub.add_parser("self-test", help="Render a two-scene fixture with silence")
    return parser


def main(argv: list[str]) -> None:
    args = build_parser().parse_args(argv)
    if args.command == "self-test":
        self_test()
        return
    if args.command == "voicesample":
        cmd_voicesample(args.text)
        return
    video = parse_video(resolve_video(args.video))
    if args.command == "validate":
        cmd_validate(video, publish=args.publish)
    elif args.command == "shotlist":
        cmd_shotlist(video)
    elif args.command == "cards":
        cmd_cards(video, force=args.force)
    elif args.command == "captions":
        cmd_captions(video)
    elif args.command == "render":
        locales = parse_locales(args.locales, publish=args.publish, video=video)
        cmd_render(
            video,
            locales,
            limit=args.limit,
            audio_only=args.audio_only,
            video_only=args.video_only,
            force=args.force_audio,
            silence=args.silence,
        )
    else:
        die(f"Unknown command {args.command}")


if __name__ == "__main__":
    main(sys.argv[1:])
