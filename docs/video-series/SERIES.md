<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Apache Hop video series

Short videos for the Apache Hop YouTube channel. Issue [7931](https://github.com/apache/hop/issues/7931).

Each video is a folder with a `manifest.yaml`, PNG stills, and short muted clips. `tools/video-series/render.py` speaks the narration, times every scene to the longest language, and writes a 1080p MP4. Generated files go under `output/` and are not committed.

The voice is not chosen yet. The manifests use `luna` until you listen to `luna` and `orion` and pick one. Set that id on every video before a publish render.

## Series

| Folder | Issue | Topic |
| --- | --- | --- |
| `01-orientation` | 9057 | What Hop is, who it is for, where to start |
| `02-installation` | 9058 | Java 21, download, first start, samples |
| `03-pipelines` | 9060 | Pipelines, the canvas, preview |
| `04-workflows` | 9061 | Workflows, actions, start from an action |
| `05-execution` | 9062 | Run, preview, debug, and hop-run |
| `06-projects` | 9063 | Projects and environments |
| `07-version-control` | 9064 | Git in Hop GUI |
| `08-security` | 9065 | Secrets, variables, resolvers |

Later videos may point at an earlier one in a single sentence. They do not re-teach it.

## How a scene is timed

A scene is one spoken idea, about 20 to 40 seconds, which is 45 to 110 English words. The picture stays up for the longest voice-over among the languages you render, plus `padding_after_sec` (0.4 seconds). The 400 ms fade happens inside that pause. A clip shorter than the voice-over holds its last frame. A clip longer than the voice-over plays to the end, and the sound stays quiet after the narration. Set `trim_sec` on that scene when you do want the recording cut.

English is the source. Further languages are added after the English cut has been heard. A video can list its own `locales`. The orientation episode uses `zh`, `es-MX`, `pt-BR`, `fr`, `de-DE`, `it-IT`, and `nl-NL`. The installation, pipelines, workflows, execution, and projects episodes use `zh-CN`, `es-ES`, `pt-PT`, `fr-FR`, `de-DE`, and `it-IT`. The speech service is called with `zh`, `es-ES`, `pt-PT`, `fr`, `de`, and `it` for those tags. Dutch is outside the published voice list. Product words in `glossary.yaml` stay in English in every language. Variable expressions are listed under `speak_as` so the voice says them as words. Words under `pronounce` are sent to the speech service as a pronunciation map, including IPA in slashes, so the subtitles still show the written word. Subtitles keep the written narration.

## Capture rules

- Capture the Hop window as large as the VM allows. It does not have to be exactly 1920x1080. The renderer scales each picture to fit that frame and pads the rest. A capture close to 1080p keeps the labels sharp. Use one Hop theme for the whole video, and the samples project unless the shot list says otherwise.
- PNG stills for a screen the viewer should read. Muted MP4 clips, 30 fps, for something that moves. A clip is only the action.
- No secrets, personal paths, bookmarks, or notification popups.
- File names are the scene id (`images/what-hop-is.png`), not the sentence.
- Do not paint the watermark. The renderer puts "Apache Hop · ASF" at the bottom right.

`render.py shotlist <video>` writes `output/shotlist.md` with the file, the picture, and the sentence.

Scenes with `card_title` are title cards. `render.py cards <video>` draws them. Replace the PNG if you want a different card.

## Rebuild

From the repository root, with `ffmpeg` on `PATH` and the packages in `tools/video-series/requirements.txt`:

```bash
python3 tools/video-series/render.py self-test
python3 tools/video-series/render.py shotlist 01-orientation
python3 tools/video-series/render.py cards 01-orientation
python3 tools/video-series/render.py validate 01-orientation
```

Voice samples, after `XAI_API_KEY` is exported:

```bash
python3 tools/video-series/render.py voicesample --text "Apache Hop is a data integration and orchestration platform. You design a pipeline once, then run it on your laptop or on Hop Server."
```

English preview, then the five-language render:

```bash
python3 tools/video-series/render.py render 01-orientation --locales en
python3 tools/video-series/render.py render 01-orientation --publish
```

`--audio-only` caches speech and writes timings. `--video-only` rebuilds the MP4 from that cache. `--limit 3` renders the first three scenes. `--force-audio` speaks again even when the text is unchanged.

Output for each video:

- `output/<id>-<locale>.mp4`
- `output/<id>-<locale>.vtt`
- `output/youtube-description.txt`
- `output/timings.yaml`

Each subtitle cue is at most two lines. The time inside a scene is shared out by the length of those lines, so a long sentence does not sit on screen as one block. After a render, `render.py captions <video>` rewrites the VTT files from `output/timings.yaml` without speaking or encoding again.

YouTube upload, thumbnails, and music are manual. Paste the description, attach the VTT, and upload the MP4.

## Add a video

Copy a folder, give it a new `video.id`, and fill `scenes`. Each scene needs an `id`, a `title` for the chapter mark, a `visual`, a `visual_type` of `still` or `clip`, a `shot` line, and narration. Run `shotlist`, capture what is missing, then `validate` and `render`. Keep the claims in line with the user manual page the episode is about.
